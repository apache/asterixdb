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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;

abstract class AbstractFilterBuilder {

    protected final Map<ILogicalExpression, ARecordType> filterPaths;
    protected final ILogicalExpression filterExpression;
    protected final JobGenContext context;
    protected final IVariableTypeEnvironment typeEnv;

    AbstractFilterBuilder(Map<ILogicalExpression, ARecordType> filterPaths, ILogicalExpression filterExpression,
            JobGenContext context, IVariableTypeEnvironment typeEnv) {
        this.filterPaths = filterPaths;
        this.filterExpression = filterExpression;
        this.context = context;
        this.typeEnv = typeEnv;
    }

    protected IScalarEvaluatorFactory createEvaluator(ILogicalExpression expression) throws AlgebricksException {
        if (filterPaths.containsKey(expression)) {
            // Path expression, create a value accessor (i.e., a column reader)
            return createValueAccessor(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return createConstantAccessor(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return handleFunction(expression);
        }

        /*
         * A variable expression: This should not happen as the provided filter expression is inlined.
         * If a variable was encountered for some reason, it should only be the record variable. If the record variable
         * was encountered, that means there's a missing value path the compiler didn't provide.
         */
        throw new IllegalStateException(
                "Unsupported expression " + expression + ". the provided paths are: " + filterPaths);
    }

    protected abstract IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression);

    private IScalarEvaluatorFactory createConstantAccessor(ILogicalExpression expression) throws AlgebricksException {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        ConstantExpression constExpr = (ConstantExpression) expression;
        return metadataProvider.getDataFormat().getConstantEvalFactory(constExpr.getValue());
    }

    private IScalarEvaluatorFactory handleFunction(ILogicalExpression expr) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        IScalarEvaluatorFactory[] args = handleArgs(funcExpr);
        IFunctionDescriptor fd = resolveFunction(funcExpr);
        return fd.createEvaluatorFactory(args);
    }

    private IScalarEvaluatorFactory[] handleArgs(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        IScalarEvaluatorFactory[] argsEvalFactories = new IScalarEvaluatorFactory[args.size()];
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression expr = args.get(i).getValue();
            IScalarEvaluatorFactory evalFactory = createEvaluator(expr);
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
}
