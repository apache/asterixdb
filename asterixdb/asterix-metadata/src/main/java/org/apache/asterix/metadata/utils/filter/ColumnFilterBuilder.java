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

import static org.apache.asterix.metadata.utils.filter.NormalizedColumnFilterBuilder.NORMALIZED_PUSHABLE_FUNCTIONS;

import java.util.List;
import java.util.Map;

import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.accessor.ColumnFilterValueAccessorEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.evaluator.ColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.DataProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ColumnFilterBuilder {
    private final FilterVariableTypeEnvironment typeEnv;
    private final Map<ILogicalExpression, ARecordType> filterPaths;
    private final ILogicalExpression filterExpression;
    private final JobGenContext context;
    private final ArrayPathCheckerVisitor checkerVisitor;

    public ColumnFilterBuilder(DataProjectionFiltrationInfo projectionFiltrationInfo, JobGenContext context) {
        typeEnv = new FilterVariableTypeEnvironment();
        this.filterPaths = projectionFiltrationInfo.getActualPaths();
        this.filterExpression = projectionFiltrationInfo.getFilterExpression();
        this.context = context;
        checkerVisitor = new ArrayPathCheckerVisitor();
    }

    public IColumnIterableFilterEvaluatorFactory build() throws AlgebricksException {
        if (filterExpression == null || filterPaths.isEmpty()
                || checkerVisitor.containsMultipleArrayPaths(filterPaths.values())) {
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }
        IScalarEvaluatorFactory evalFactory = createEvaluator(filterExpression);
        if (evalFactory == null) {
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }
        return new ColumnIterableFilterEvaluatorFactory(evalFactory);
    }

    private IScalarEvaluatorFactory createEvaluator(ILogicalExpression expression) throws AlgebricksException {
        if (filterPaths.containsKey(expression)) {
            return createValueAccessor(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return createConstantAccessor(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return handleFunction(expression);
        }
        return null;
    }

    private IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        return new ColumnFilterValueAccessorEvaluatorFactory(path);
    }

    private IScalarEvaluatorFactory createConstantAccessor(ILogicalExpression expression) throws AlgebricksException {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        ConstantExpression constExpr = (ConstantExpression) expression;
        return metadataProvider.getDataFormat().getConstantEvalFactory(constExpr.getValue());
    }

    private IScalarEvaluatorFactory handleFunction(ILogicalExpression expr) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        IScalarEvaluatorFactory[] args = handleArgs(funcExpr);
        if (args == null) {
            return null;
        }

        IFunctionDescriptor fd = resolveFunction(funcExpr);
        return fd.createEvaluatorFactory(args);
    }

    private IScalarEvaluatorFactory[] handleArgs(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        IScalarEvaluatorFactory[] argsEvalFactories = new IScalarEvaluatorFactory[args.size()];
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression expr = args.get(i).getValue();
            IScalarEvaluatorFactory evalFactory = createEvaluator(expr);
            if (evalFactory == null) {
                return null;
            }
            argsEvalFactories[i] = evalFactory;
        }
        return argsEvalFactories;
    }

    private IFunctionDescriptor resolveFunction(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        IFunctionManager functionManager = metadataProvider.getFunctionManager();
        FunctionIdentifier fnId = funcExpr.getFunctionIdentifier();
        SourceLocation sourceLocation = funcExpr.getSourceLocation();
        IFunctionDescriptor fd = functionManager.lookupFunction(fnId, sourceLocation);
        fd.setSourceLocation(sourceLocation);
        IFunctionTypeInferer fnTypeInfer = functionManager.lookupFunctionTypeInferer(fnId);
        if (fnTypeInfer != null) {
            CompilerProperties compilerProps = ((IApplicationContext) context.getAppContext()).getCompilerProperties();
            fnTypeInfer.infer(funcExpr, fd, typeEnv, compilerProps);
        }
        return fd;
    }

    public static boolean isPushable(FunctionIdentifier fid) {
        return NORMALIZED_PUSHABLE_FUNCTIONS.contains(fid) || !isNestedFunction(fid) && !isTypeFunction(fid);
    }

    private static boolean isTypeFunction(FunctionIdentifier fid) {
        return fid.getName().startsWith("is");
    }

    private static boolean isNestedFunction(FunctionIdentifier fid) {
        return isObjectFunction(fid) || isArrayFunction(fid) || BuiltinFunctions.DEEP_EQUAL.equals(fid);
    }

    private static boolean isObjectFunction(FunctionIdentifier fid) {
        String functionName = fid.getName();
        return functionName.contains("object") || BuiltinFunctions.PAIRS.equals(fid);
    }

    private static boolean isArrayFunction(FunctionIdentifier fid) {
        String functionName = fid.getName();
        return functionName.startsWith("array") || functionName.startsWith("strict")
                || BuiltinFunctions.GET_ITEM.equals(fid);
    }

}
