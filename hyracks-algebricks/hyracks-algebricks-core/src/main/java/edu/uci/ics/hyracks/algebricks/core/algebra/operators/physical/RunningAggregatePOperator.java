/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.runtime.base.IRunningAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.RunningAggregateRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class RunningAggregatePOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RUNNING_AGGREGATE;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        IPartitioningProperty pp = null;
        RunningAggregateOperator ragg = (RunningAggregateOperator) op;
        for (Mutable<ILogicalExpression> exprRef : ragg.getExpressions()) {
            StatefulFunctionCallExpression f = (StatefulFunctionCallExpression) exprRef.getValue();
            IPartitioningProperty p = f.getRequiredPartitioningProperty();
            if (p != null) {
                if (pp == null) {
                    pp = p;
                } else {
                    throw new IllegalStateException("Two stateful functions want to set partitioning requirements: "
                            + pp + " and " + p);
                }
            }
        }
        StructuralPropertiesVector[] r = new StructuralPropertiesVector[] { new StructuralPropertiesVector(pp, null) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        RunningAggregateOperator ragg = (RunningAggregateOperator) op;
        List<LogicalVariable> variables = ragg.getVariables();
        List<Mutable<ILogicalExpression>> expressions = ragg.getExpressions();
        int[] outColumns = new int[variables.size()];
        for (int i = 0; i < outColumns.length; i++) {
            outColumns[i] = opSchema.findVariable(variables.get(i));
        }
        IRunningAggregateFunctionFactory[] runningAggFuns = new IRunningAggregateFunctionFactory[expressions.size()];
        ILogicalExpressionJobGen exprJobGen = context.getExpressionJobGen();
        for (int i = 0; i < runningAggFuns.length; i++) {
            StatefulFunctionCallExpression expr = (StatefulFunctionCallExpression) expressions.get(i).getValue();
            runningAggFuns[i] = exprJobGen.createRunningAggregateFunctionFactory(expr,
                    context.getTypeEnvironment(op.getInputs().get(0).getValue()), inputSchemas, context);
        }

        // TODO push projections into the operator
        int[] projectionList = JobGenHelper.projectAllVariables(opSchema);

        RunningAggregateRuntimeFactory runtime = new RunningAggregateRuntimeFactory(outColumns, runningAggFuns,
                projectionList);

        // contribute one Asterix framewriter
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(op, opSchema, context);
        builder.contributeMicroOperator(ragg, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = ragg.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, ragg, 0);

    }

}
