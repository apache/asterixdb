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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.SubplanRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class SubplanPOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SUBPLAN;
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector childsProperties = op2.getPhysicalOperator().getDeliveredProperties();
        List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
        if (childsProperties.getLocalProperties() != null) {
            propsLocal.addAll(childsProperties.getLocalProperties());
        }
        // ... get local properties for newly created variables...
        SubplanOperator subplan = (SubplanOperator) op;
        for (ILogicalPlan plan : subplan.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                AbstractLogicalOperator rOp = (AbstractLogicalOperator) r.getValue();
                propsLocal.addAll(rOp.getPhysicalOperator().getDeliveredProperties().getLocalProperties());
            }
        }

        deliveredProperties = new StructuralPropertiesVector(childsProperties.getPartitioningProperty(), propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        SubplanOperator subplan = (SubplanOperator) op;
        if (subplan.getNestedPlans().size() != 1) {
            throw new NotImplementedException("Subplan currently works only for one nested plan with one root.");
        }
        AlgebricksPipeline[] subplans = compileSubplans(inputSchemas[0], subplan, opSchema, context);
        assert (subplans.length == 1);
        AlgebricksPipeline np = subplans[0];
        RecordDescriptor inputRecordDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op.getInputs().get(0).getValue()),
                inputSchemas[0], context);
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[np.getOutputWidth()];
        for (int i = 0; i < nullWriterFactories.length; i++) {
            nullWriterFactories[i] = context.getNullWriterFactory();
        }
        SubplanRuntimeFactory runtime = new SubplanRuntimeFactory(np, nullWriterFactories, inputRecordDesc, null);

        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        builder.contributeMicroOperator(subplan, runtime, recDesc);

        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

}
