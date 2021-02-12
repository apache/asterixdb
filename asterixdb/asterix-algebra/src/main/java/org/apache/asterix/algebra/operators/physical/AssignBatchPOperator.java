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

package org.apache.asterix.algebra.operators.physical;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.library.ExternalFunctionDescriptorProvider;
import org.apache.asterix.external.operators.ExternalAssignBatchRuntimeFactory;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.AbstractAssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class AssignBatchPOperator extends AbstractAssignPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.ASSIGN_BATCH;
    }

    @Override
    protected IPushRuntimeFactory createRuntimeFactory(JobGenContext context, AssignOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, int[] outColumns, int[] projectionList)
            throws AlgebricksException {
        IVariableTypeEnvironment inputTypeEnv = context.getTypeEnvironment(op.getInputs().get(0).getValue());
        List<Mutable<ILogicalExpression>> exprList = op.getExpressions();
        int exprCount = exprList.size();
        IExternalFunctionDescriptor[] fnDescs = new IExternalFunctionDescriptor[exprCount];
        int[][] fnArgColumns = new int[exprCount][];
        for (int i = 0; i < exprCount; i++) {
            Mutable<ILogicalExpression> exprRef = exprList.get(i);
            ILogicalExpression expr = exprRef.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, op.getSourceLocation(),
                        String.valueOf(expr.getExpressionTag()));
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            IFunctionInfo fi = callExpr.getFunctionInfo();
            if (!ExternalFunctionCompilerUtil.supportsBatchInvocation(callExpr.getKind(), fi)) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, op.getSourceLocation(),
                        fi.toString());
            }
            fnDescs[i] = ExternalFunctionDescriptorProvider.resolveExternalFunction(callExpr, inputTypeEnv, context);
            fnArgColumns[i] = getColumns(callExpr.getArguments(), opSchema, op.getSourceLocation());
        }

        return new ExternalAssignBatchRuntimeFactory(outColumns, fnDescs, fnArgColumns, projectionList);
    }

    private int[] getColumns(List<Mutable<ILogicalExpression>> exprList, IOperatorSchema opSchema,
            SourceLocation sourceLoc) throws CompilationException {
        int n = exprList.size();
        int[] columns = new int[n];
        for (int i = 0; i < n; i++) {
            ILogicalExpression expr = exprList.get(i).getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                        String.valueOf(expr.getExpressionTag()));
            }
            VariableReferenceExpression argVarRef = (VariableReferenceExpression) expr;
            LogicalVariable argVar = argVarRef.getVariableReference();
            int argColumn = opSchema.findVariable(argVar);
            if (argColumn < 0) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, String.valueOf(argVar));
            }
            columns[i] = argColumn;
        }
        return columns;
    }
}
