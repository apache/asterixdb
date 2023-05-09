/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.metadata.utils.filter;

import static org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.IColumnFilterNormalizedValueAccessorFactory;
import org.apache.asterix.column.filter.normalized.IColumnNormalizedFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.accessor.ColumnFilterNormalizedValueAccessorFactory;
import org.apache.asterix.column.filter.normalized.accessor.ConstantColumnFilterNormalizedValueAccessorFactory;
import org.apache.asterix.column.filter.normalized.compartor.GEColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.compartor.GTColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.compartor.LEColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.compartor.LTColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.evaluator.ANDColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.normalized.evaluator.ORColumnFilterEvaluatorFactory;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.DataProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class NormalizedColumnFilterBuilder {
    public static final Set<FunctionIdentifier> COMPARE_FUNCTIONS = getCompareFunctions();
    public static final Set<FunctionIdentifier> NORMALIZED_PUSHABLE_FUNCTIONS = getNormalizedPushableFunctions();

    private final Map<ILogicalExpression, ARecordType> filterPaths;
    private final ILogicalExpression filterExpression;

    public NormalizedColumnFilterBuilder(DataProjectionFiltrationInfo projectionFiltrationInfo) {
        this.filterPaths = projectionFiltrationInfo.getNormalizedPaths();
        this.filterExpression = projectionFiltrationInfo.getFilterExpression();
    }

    public IColumnNormalizedFilterEvaluatorFactory build() {
        if (filterExpression == null || filterPaths.isEmpty()) {
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }
        return createEvaluator(filterPaths, filterExpression);
    }

    private IColumnNormalizedFilterEvaluatorFactory createEvaluator(Map<ILogicalExpression, ARecordType> filterPaths,
            ILogicalExpression filterExpression) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) filterExpression;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        if (COMPARE_FUNCTIONS.contains(fid)) {
            return createComparator(fid, funcExpr.getArguments(), filterPaths);
        }
        return createEvaluatorsForArgs(funcExpr, filterPaths);
    }

    private IColumnNormalizedFilterEvaluatorFactory createComparator(FunctionIdentifier fid,
            List<Mutable<ILogicalExpression>> arguments, Map<ILogicalExpression, ARecordType> filterPaths) {
        ILogicalExpression arg0 = arguments.get(0).getValue();
        ILogicalExpression arg1 = arguments.get(1).getValue();

        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ARecordType path = filterPaths.get(arg0);
            IAObject constant = getConstant(arg1);
            return createComparator(fid, path, constant, true);
        } else {
            ARecordType path = filterPaths.get(arg1);
            IAObject constant = getConstant(arg0);
            return createComparator(fid, path, constant, false);
        }
    }

    private IColumnNormalizedFilterEvaluatorFactory createEvaluatorsForArgs(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalExpression, ARecordType> filterPaths) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        //AND/OR have at least two arguments
        IColumnNormalizedFilterEvaluatorFactory arg0 = createEvaluator(filterPaths, args.get(0).getValue());
        IColumnNormalizedFilterEvaluatorFactory arg1 = createEvaluator(filterPaths, args.get(1).getValue());

        IColumnNormalizedFilterEvaluatorFactory left = createEvaluator(fid, arg0, arg1);
        for (int i = 2; i < args.size() && left != null; i++) {
            IColumnNormalizedFilterEvaluatorFactory right = createEvaluator(filterPaths, args.get(i).getValue());
            left = createEvaluator(fid, left, right);
        }
        return left;
    }

    private IColumnNormalizedFilterEvaluatorFactory createComparator(FunctionIdentifier fid, ARecordType path,
            IAObject constant, boolean leftToRight) {
        ComparisonKind comparisonKind = AlgebricksBuiltinFunctions.getComparisonType(fid);
        if (path == null) {
            // skipped path
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }

        IColumnFilterNormalizedValueAccessorFactory constValue =
                ConstantColumnFilterNormalizedValueAccessorFactory.createFactory(constant);
        IColumnFilterNormalizedValueAccessorFactory min = new ColumnFilterNormalizedValueAccessorFactory(path, true);
        IColumnFilterNormalizedValueAccessorFactory max = new ColumnFilterNormalizedValueAccessorFactory(path, false);

        if (leftToRight) {
            return createEvaluator(comparisonKind, min, constValue, max);
        }
        return createEvaluator(invert(comparisonKind), min, constValue, max);
    }

    private static IColumnNormalizedFilterEvaluatorFactory createEvaluator(FunctionIdentifier fid,
            IColumnNormalizedFilterEvaluatorFactory left, IColumnNormalizedFilterEvaluatorFactory right) {
        if (right == null) {
            return null;
        }
        if (BuiltinFunctions.AND.equals(fid)) {
            return new ANDColumnFilterEvaluatorFactory(left, right);
        }
        return new ORColumnFilterEvaluatorFactory(left, right);
    }

    private static ComparisonKind invert(ComparisonKind comparisonKind) {
        if (comparisonKind == ComparisonKind.LT) {
            return ComparisonKind.GE;
        } else if (comparisonKind == ComparisonKind.LE) {
            return ComparisonKind.GT;
        } else if (comparisonKind == ComparisonKind.GT) {
            return ComparisonKind.LE;
        }
        //ComparisonKind.GE
        return ComparisonKind.LT;
    }

    private static IColumnNormalizedFilterEvaluatorFactory createEvaluator(ComparisonKind comparisonKind,
            IColumnFilterNormalizedValueAccessorFactory min, IColumnFilterNormalizedValueAccessorFactory constVal,
            IColumnFilterNormalizedValueAccessorFactory max) {
        if (comparisonKind == ComparisonKind.LT) {
            return new GTColumnFilterEvaluatorFactory(constVal, min);
        } else if (comparisonKind == ComparisonKind.LE) {
            return new GEColumnFilterEvaluatorFactory(constVal, min);
        } else if (comparisonKind == ComparisonKind.EQ) {
            IColumnNormalizedFilterEvaluatorFactory minComp = new GEColumnFilterEvaluatorFactory(constVal, min);
            IColumnNormalizedFilterEvaluatorFactory maxComp = new LEColumnFilterEvaluatorFactory(constVal, max);
            return new ANDColumnFilterEvaluatorFactory(minComp, maxComp);
        } else if (comparisonKind == ComparisonKind.GT) {
            return new LTColumnFilterEvaluatorFactory(constVal, max);
        }
        //ComparisonKind.GE
        return new LEColumnFilterEvaluatorFactory(constVal, max);
    }

    private static IAObject getConstant(ILogicalExpression expr) {
        return ((AsterixConstantValue) ((ConstantExpression) expr).getValue()).getObject();
    }

    private static Set<FunctionIdentifier> getCompareFunctions() {
        return Set.of(AlgebricksBuiltinFunctions.LE, AlgebricksBuiltinFunctions.GE, AlgebricksBuiltinFunctions.LT,
                AlgebricksBuiltinFunctions.GT, AlgebricksBuiltinFunctions.EQ);
    }

    private static Set<FunctionIdentifier> getNormalizedPushableFunctions() {
        Set<FunctionIdentifier> pushableFunctions = new HashSet<>(COMPARE_FUNCTIONS);
        pushableFunctions.add(AlgebricksBuiltinFunctions.AND);
        pushableFunctions.add(AlgebricksBuiltinFunctions.OR);
        return pushableFunctions;
    }

}
