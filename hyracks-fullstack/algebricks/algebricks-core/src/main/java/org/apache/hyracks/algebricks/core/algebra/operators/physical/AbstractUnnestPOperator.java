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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestNonMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.UnnestRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.util.LogRedactionUtil;

public abstract class AbstractUnnestPOperator extends AbstractScanPOperator {
    private final boolean leftOuter;

    public AbstractUnnestPOperator(boolean leftOuter) {
        this.leftOuter = leftOuter;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractUnnestNonMapOperator unnest = (AbstractUnnestNonMapOperator) op;
        int outCol = opSchema.findVariable(unnest.getVariable());
        ILogicalExpression unnestExpr = unnest.getExpressionRef().getValue();
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        boolean exit = false;
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            exit = true;
        } else {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) unnestExpr;
            if (fce.getKind() != FunctionKind.UNNEST) {
                exit = true;
            }
        }
        if (exit) {
            throw new AlgebricksException("Unnest expression " + LogRedactionUtil.userData(unnestExpr.toString())
                    + " is not an unnesting function call.");
        }
        UnnestingFunctionCallExpression agg = (UnnestingFunctionCallExpression) unnestExpr;
        IUnnestingEvaluatorFactory unnestingFactory = expressionRuntimeProvider.createUnnestingFunctionFactory(agg,
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas, context);
        int[] projectionList = JobGenHelper.projectAllVariables(opSchema);
        UnnestRuntimeFactory unnestRuntime = new UnnestRuntimeFactory(outCol, unnestingFactory, projectionList,
                unnest.getPositionWriter(), leftOuter, context.getMissingWriterFactory());
        unnestRuntime.setSourceLocation(unnest.getSourceLocation());
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        builder.contributeMicroOperator(unnest, unnestRuntime, recDesc);
        ILogicalOperator src = unnest.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, unnest, 0);
    }
}
