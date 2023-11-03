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

import static org.apache.asterix.metadata.utils.PushdownUtil.isCompare;
import static org.apache.asterix.metadata.utils.PushdownUtil.isConstant;
import static org.apache.asterix.metadata.utils.PushdownUtil.isFilterPath;
import static org.apache.asterix.metadata.utils.PushdownUtil.isNot;
import static org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;

import java.util.List;
import java.util.Map;

import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.column.filter.range.accessor.ColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.column.filter.range.accessor.ConstantColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.column.filter.range.compartor.GEColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.compartor.GTColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.compartor.LEColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.compartor.LTColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.evaluator.ANDColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.evaluator.ORColumnFilterEvaluatorFactory;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.projection.ColumnDatasetProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class ColumnRangeFilterBuilder {

    private final Map<ILogicalExpression, ARecordType> filterPaths;
    private final ILogicalExpression filterExpression;

    public ColumnRangeFilterBuilder(ColumnDatasetProjectionFiltrationInfo projectionFiltrationInfo) {
        this.filterPaths = projectionFiltrationInfo.getFilterPaths();
        this.filterExpression = projectionFiltrationInfo.getRangeFilterExpression();
    }

    public IColumnRangeFilterEvaluatorFactory build() {
        if (filterExpression == null || filterPaths.isEmpty()) {
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }
        return createEvaluator(filterPaths, filterExpression);
    }

    private IColumnRangeFilterEvaluatorFactory createEvaluator(Map<ILogicalExpression, ARecordType> filterPaths,
            ILogicalExpression filterExpression) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) filterExpression;

        if (isFilterPath(funcExpr) || isNot(funcExpr)) {
            return createBooleanEvaluator(funcExpr, filterPaths);
        } else if (isCompare(funcExpr)) {
            return createComparator(funcExpr.getFunctionIdentifier(), funcExpr.getArguments(), filterPaths);
        }
        return createEvaluatorsForArgs(funcExpr, filterPaths);
    }

    private IColumnRangeFilterEvaluatorFactory createBooleanEvaluator(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalExpression, ARecordType> filterPaths) {
        boolean not = isNot(funcExpr);
        IAObject constVal = not ? ABoolean.FALSE : ABoolean.TRUE;
        ILogicalExpression pathExpr = not ? funcExpr.getArguments().get(0).getValue() : funcExpr;
        ARecordType path = filterPaths.get(pathExpr);

        return createComparator(BuiltinFunctions.EQ, path, constVal, true);
    }

    private IColumnRangeFilterEvaluatorFactory createComparator(FunctionIdentifier fid,
            List<Mutable<ILogicalExpression>> arguments, Map<ILogicalExpression, ARecordType> filterPaths) {
        ILogicalExpression left = arguments.get(0).getValue();
        ILogicalExpression right = arguments.get(1).getValue();
        boolean rightConstant = isConstant(right);
        ARecordType path;
        IAObject constant;
        if (rightConstant) {
            path = filterPaths.get(left);
            constant = getConstant(right);
        } else {
            path = filterPaths.get(right);
            constant = getConstant(left);
        }

        return createComparator(fid, path, constant, rightConstant);
    }

    private IColumnRangeFilterEvaluatorFactory createEvaluatorsForArgs(AbstractFunctionCallExpression funcExpr,
            Map<ILogicalExpression, ARecordType> filterPaths) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        //AND/OR have at least two arguments
        IColumnRangeFilterEvaluatorFactory arg0 = createEvaluator(filterPaths, args.get(0).getValue());
        IColumnRangeFilterEvaluatorFactory arg1 = createEvaluator(filterPaths, args.get(1).getValue());

        IColumnRangeFilterEvaluatorFactory left = createEvaluator(fid, arg0, arg1);
        for (int i = 2; i < args.size(); i++) {
            IColumnRangeFilterEvaluatorFactory right = createEvaluator(filterPaths, args.get(i).getValue());
            left = createEvaluator(fid, left, right);
        }
        return left;
    }

    private IColumnRangeFilterEvaluatorFactory createComparator(FunctionIdentifier fid, ARecordType path,
            IAObject constant, boolean rightConstant) {
        IColumnRangeFilterValueAccessorFactory constValue =
                ConstantColumnRangeFilterValueAccessorFactory.createFactory(constant);
        if (path == null || constValue == null) {
            // Skipped paths or unsupported constants.
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }

        ComparisonKind comparisonKind = getComparisonKind(fid, constant.getType().getTypeTag());
        if (comparisonKind == ComparisonKind.NEQ) {
            // Ignore NEQ
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }

        IColumnRangeFilterValueAccessorFactory min = new ColumnRangeFilterValueAccessorFactory(path, true);
        IColumnRangeFilterValueAccessorFactory max = new ColumnRangeFilterValueAccessorFactory(path, false);

        if (rightConstant) {
            return createEvaluator(comparisonKind, min, constValue, max);
        }
        return createEvaluator(invert(comparisonKind), min, constValue, max);
    }

    private ComparisonKind getComparisonKind(FunctionIdentifier fid, ATypeTag typeTag) {
        ComparisonKind comparisonKind = AlgebricksBuiltinFunctions.getComparisonType(fid);

        if (!NonTaggedFormatUtil.isFixedSizedCollection(typeTag)) {
            // For variable-length values, we include equal as their filters are not decisive.
            if (comparisonKind == ComparisonKind.LT) {
                return ComparisonKind.LE;
            } else if (comparisonKind == ComparisonKind.GT) {
                return ComparisonKind.GE;
            }
        }

        return comparisonKind;
    }

    private static IColumnRangeFilterEvaluatorFactory createEvaluator(FunctionIdentifier fid,
            IColumnRangeFilterEvaluatorFactory left, IColumnRangeFilterEvaluatorFactory right) {
        if (BuiltinFunctions.AND.equals(fid)) {
            return new ANDColumnFilterEvaluatorFactory(left, right);
        }
        return new ORColumnFilterEvaluatorFactory(left, right);
    }

    private static ComparisonKind invert(ComparisonKind comparisonKind) {
        switch (comparisonKind) {
            case EQ:
                return ComparisonKind.EQ;
            case LE:
                return ComparisonKind.GT;
            case GE:
                return ComparisonKind.LT;
            case LT:
                return ComparisonKind.GE;
            case GT:
                return ComparisonKind.LE;
            default:
                throw new IllegalStateException("Unsupported comparison type: " + comparisonKind);
        }
    }

    private static IColumnRangeFilterEvaluatorFactory createEvaluator(ComparisonKind comparisonKind,
            IColumnRangeFilterValueAccessorFactory min, IColumnRangeFilterValueAccessorFactory constVal,
            IColumnRangeFilterValueAccessorFactory max) {
        if (comparisonKind == ComparisonKind.LT) {
            return new GTColumnFilterEvaluatorFactory(constVal, min);
        } else if (comparisonKind == ComparisonKind.LE) {
            return new GEColumnFilterEvaluatorFactory(constVal, min);
        } else if (comparisonKind == ComparisonKind.EQ) {
            IColumnRangeFilterEvaluatorFactory minComp = new GEColumnFilterEvaluatorFactory(constVal, min);
            IColumnRangeFilterEvaluatorFactory maxComp = new LEColumnFilterEvaluatorFactory(constVal, max);
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

}
