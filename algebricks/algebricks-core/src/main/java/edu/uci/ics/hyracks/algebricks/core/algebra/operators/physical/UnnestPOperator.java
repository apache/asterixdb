/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.UnnestRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class UnnestPOperator extends AbstractScanPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.UNNEST;
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
        UnnestOperator unnest = (UnnestOperator) op;

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
            throw new AlgebricksException("Unnest expression " + unnestExpr + " is not an unnesting function call.");
        }
        UnnestingFunctionCallExpression agg = (UnnestingFunctionCallExpression) unnestExpr;
        IUnnestingEvaluatorFactory unnestingFactory = expressionRuntimeProvider.createUnnestingFunctionFactory(agg,
                context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas, context);

        // for position offset
        ILogicalExpression posOffsetExpr = unnest.getPositionOffsetExpr();
        IScalarEvaluatorFactory posOffsetExprEvalFactory = null;
        if (posOffsetExpr != null) {
            posOffsetExprEvalFactory = expressionRuntimeProvider.createEvaluatorFactory(posOffsetExpr,
                    context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas, context);
        }

        int[] projectionList = JobGenHelper.projectAllVariables(opSchema);
        UnnestRuntimeFactory unnestRuntime = new UnnestRuntimeFactory(outCol, unnestingFactory, projectionList,
                unnest.getPositionalVariable() != null, posOffsetExprEvalFactory);
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        builder.contributeMicroOperator(unnest, unnestRuntime, recDesc);
        ILogicalOperator src = unnest.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, unnest, 0);
    }
}
