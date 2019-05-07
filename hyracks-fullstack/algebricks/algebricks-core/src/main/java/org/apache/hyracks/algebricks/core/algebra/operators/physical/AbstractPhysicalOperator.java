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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.PlanCompiler;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.job.JobSpecification;

public abstract class AbstractPhysicalOperator implements IPhysicalOperator {

    protected IPhysicalPropertiesVector deliveredProperties;
    protected LocalMemoryRequirements localMemoryRequirements;
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

    @Override
    public void setHostQueryContext(Object context) {
        this.hostQueryContext = context;
    }

    @Override
    public Object getHostQueryContext() {
        return hostQueryContext;
    }

    protected PhysicalRequirements emptyUnaryRequirements() {
        StructuralPropertiesVector[] req =
                new StructuralPropertiesVector[] { StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR };
        return new PhysicalRequirements(req, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    protected PhysicalRequirements emptyUnaryRequirements(int numberOfChildren) {
        StructuralPropertiesVector[] req = new StructuralPropertiesVector[numberOfChildren];
        for (int i = 0; i < numberOfChildren; i++) {
            req[i] = StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
        }
        return new PhysicalRequirements(req, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public LocalMemoryRequirements getLocalMemoryRequirements() {
        return localMemoryRequirements;
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.fixedMemoryBudget(1);
    }

    @Override
    public void disableJobGenBelowMe() {
        this.disableJobGenBelow = true;
    }

    @Override
    public boolean isJobGenDisabledBelowMe() {
        return disableJobGenBelow;
    }

    /**
     * @return labels (0 or 1) for each input and output indicating the dependency between them.
     * The edges labeled as 1 must wait for the edges with label 0.
     */
    @Override
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op) {
        int[] inputDependencyLabels = new int[op.getInputs().size()]; // filled with 0's
        int[] outputDependencyLabels = new int[] { 0 };
        return new Pair<int[], int[]>(inputDependencyLabels, outputDependencyLabels);
    }

    protected void contributeOpDesc(IHyracksJobBuilder builder, AbstractLogicalOperator op,
            IOperatorDescriptor opDesc) {
        if (op.getExecutionMode() == ExecutionMode.UNPARTITIONED) {
            AlgebricksPartitionConstraint apc = new AlgebricksCountPartitionConstraint(1);
            builder.contributeAlgebricksPartitionConstraint(opDesc, apc);
        }
        builder.contributeHyracksOperator(op, opDesc);
    }

    protected AlgebricksPipeline[] compileSubplans(IOperatorSchema outerPlanSchema,
            AbstractOperatorWithNestedPlans npOp, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        List<List<AlgebricksPipeline>> subplans = compileSubplansImpl(outerPlanSchema, npOp, opSchema, context);
        int n = subplans.size();
        AlgebricksPipeline[] result = new AlgebricksPipeline[n];
        for (int i = 0; i < n; i++) {
            List<AlgebricksPipeline> subplanOps = subplans.get(i);
            if (subplanOps.size() != 1) {
                throw new AlgebricksException("Attempting to construct a nested plan with " + subplanOps.size()
                        + " operator descriptors. Currently, nested plans can only consist in linear pipelines of "
                        + "micro operators.");
            }
            result[i] = subplanOps.get(0);
        }
        return result;
    }

    protected List<List<AlgebricksPipeline>> compileSubplansImpl(IOperatorSchema outerPlanSchema,
            AbstractOperatorWithNestedPlans npOp, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        List<List<AlgebricksPipeline>> subplans = new ArrayList<>(npOp.getNestedPlans().size());
        PlanCompiler pc = new PlanCompiler(context);
        for (ILogicalPlan p : npOp.getNestedPlans()) {
            subplans.add(buildPipelineWithProjection(p, outerPlanSchema, opSchema, pc));
        }
        return subplans;
    }

    private List<AlgebricksPipeline> buildPipelineWithProjection(ILogicalPlan p, IOperatorSchema outerPlanSchema,
            IOperatorSchema opSchema, PlanCompiler pc) throws AlgebricksException {
        if (p.getRoots().size() > 1) {
            throw new NotImplementedException("Nested plans with several roots are not supported.");
        }
        JobSpecification nestedJob = pc.compileNestedPlan(p, outerPlanSchema);
        ILogicalOperator topOpInSubplan = p.getRoots().get(0).getValue();
        JobGenContext context = pc.getContext();
        IOperatorSchema topOpInSubplanScm = context.getSchema(topOpInSubplan);
        opSchema.addAllVariables(topOpInSubplanScm);

        Map<OperatorDescriptorId, IOperatorDescriptor> opMap = nestedJob.getOperatorMap();
        List<? extends IOperatorDescriptor> metaOps = nestedJob.getMetaOps();
        if (opMap.size() != metaOps.size()) {
            for (IOperatorDescriptor opd : opMap.values()) {
                if (!(opd instanceof AlgebricksMetaOperatorDescriptor)) {
                    throw new AlgebricksException(
                            "Can only generate jobs for pipelinable nested plans, not for " + opd.getClass().getName());
                }
            }
            throw new IllegalStateException("Unexpected nested plan");
        }

        List<AlgebricksPipeline> result = new ArrayList<>(metaOps.size());
        for (IOperatorDescriptor opd : metaOps) {
            AlgebricksMetaOperatorDescriptor amod = (AlgebricksMetaOperatorDescriptor) opd;
            result.add(amod.getPipeline());
        }
        return result;
    }
}
