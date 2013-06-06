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

import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import edu.uci.ics.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public abstract class AbstractPhysicalOperator implements IPhysicalOperator {

    protected IPhysicalPropertiesVector deliveredProperties;
    private boolean disableJobGenBelow = false;
    private Object hostQueryContext;

    @Override
    public final IPhysicalPropertiesVector getDeliveredProperties() {
        return deliveredProperties;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString();
    }

    public void setHostQueryContext(Object context) {
        this.hostQueryContext = context;
    }

    public Object getHostQueryContext() {
        return hostQueryContext;
    }

    protected PhysicalRequirements emptyUnaryRequirements() {
        StructuralPropertiesVector[] req = new StructuralPropertiesVector[] { StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR };
        return new PhysicalRequirements(req, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void disableJobGenBelowMe() {
        this.disableJobGenBelow = true;
    }

    @Override
    public boolean isJobGenDisabledBelowMe() {
        return disableJobGenBelow;
    }

    protected void contributeOpDesc(IHyracksJobBuilder builder, AbstractLogicalOperator op, IOperatorDescriptor opDesc) {
        if (op.getExecutionMode() == ExecutionMode.UNPARTITIONED) {
            AlgebricksPartitionConstraint apc = new AlgebricksCountPartitionConstraint(1);
            builder.contributeAlgebricksPartitionConstraint(opDesc, apc);
        }
        builder.contributeHyracksOperator(op, opDesc);
    }

    protected AlgebricksPipeline[] compileSubplans(IOperatorSchema outerPlanSchema,
            AbstractOperatorWithNestedPlans npOp, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        AlgebricksPipeline[] subplans = new AlgebricksPipeline[npOp.getNestedPlans().size()];
        PlanCompiler pc = new PlanCompiler(context);
        int i = 0;
        for (ILogicalPlan p : npOp.getNestedPlans()) {
            subplans[i++] = buildPipelineWithProjection(p, outerPlanSchema, npOp, opSchema, pc);
        }
        return subplans;
    }

    private AlgebricksPipeline buildPipelineWithProjection(ILogicalPlan p, IOperatorSchema outerPlanSchema,
            AbstractOperatorWithNestedPlans npOp, IOperatorSchema opSchema, PlanCompiler pc) throws AlgebricksException {
        if (p.getRoots().size() > 1) {
            throw new NotImplementedException("Nested plans with several roots are not supported.");
        }
        JobSpecification nestedJob = pc.compilePlan(p, outerPlanSchema, null);
        ILogicalOperator topOpInSubplan = p.getRoots().get(0).getValue();
        JobGenContext context = pc.getContext();
        IOperatorSchema topOpInSubplanScm = context.getSchema(topOpInSubplan);
        opSchema.addAllVariables(topOpInSubplanScm);

        Map<OperatorDescriptorId, IOperatorDescriptor> opMap = nestedJob.getOperatorMap();
        if (opMap.size() != 1) {
            throw new AlgebricksException(
                    "Attempting to construct a nested plan with "
                            + opMap.size()
                            + " operator descriptors. Currently, nested plans can only consist in linear pipelines of Asterix micro operators.");
        }

        for (OperatorDescriptorId oid : opMap.keySet()) {
            IOperatorDescriptor opd = opMap.get(oid);
            if (!(opd instanceof AlgebricksMetaOperatorDescriptor)) {
                throw new AlgebricksException(
                        "Can only generate Hyracks jobs for pipelinable Asterix nested plans, not for "
                                + opd.getClass().getName());
            }
            AlgebricksMetaOperatorDescriptor amod = (AlgebricksMetaOperatorDescriptor) opd;

            return amod.getPipeline();
            // we suppose that the top operator in the subplan already does the
            // projection for us
        }

        throw new IllegalStateException();
    }
}
