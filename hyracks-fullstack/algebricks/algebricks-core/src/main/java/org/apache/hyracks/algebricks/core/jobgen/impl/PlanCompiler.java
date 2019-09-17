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
package org.apache.hyracks.algebricks.core.jobgen.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;

public class PlanCompiler {
    private JobGenContext context;
    private Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> operatorVisitedToParents = new HashMap<>();

    public PlanCompiler(JobGenContext context) {
        this.context = context;
    }

    public JobGenContext getContext() {
        return context;
    }

    public JobSpecification compilePlan(ILogicalPlan plan, IJobletEventListenerFactory jobEventListenerFactory)
            throws AlgebricksException {
        return compilePlanImpl(plan, false, null, jobEventListenerFactory);
    }

    public JobSpecification compileNestedPlan(ILogicalPlan plan, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        return compilePlanImpl(plan, true, outerPlanSchema, null);
    }

    private JobSpecification compilePlanImpl(ILogicalPlan plan, boolean isNestedPlan, IOperatorSchema outerPlanSchema,
            IJobletEventListenerFactory jobEventListenerFactory) throws AlgebricksException {
        JobSpecification spec = new JobSpecification(context.getFrameSize());
        spec.setMaxWarnings(context.getMaxWarnings());
        if (jobEventListenerFactory != null) {
            spec.setJobletEventListenerFactory(jobEventListenerFactory);
        }
        List<ILogicalOperator> rootOps = new ArrayList<>();
        JobBuilder builder = new JobBuilder(spec, context.getClusterLocations());
        for (Mutable<ILogicalOperator> opRef : plan.getRoots()) {
            compileOpRef(opRef, spec, builder, outerPlanSchema);
            rootOps.add(opRef.getValue());
        }
        reviseEdges(builder);
        operatorVisitedToParents.clear();
        builder.buildSpec(rootOps);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        // Do not do activity cluster planning because it is slow on large clusters
        spec.setUseConnectorPolicyForScheduling(false);
        if (isNestedPlan) {
            spec.setMetaOps(builder.getGeneratedMetaOps());
        }
        return spec;
    }

    private void compileOpRef(Mutable<ILogicalOperator> opRef, IOperatorDescriptorRegistry spec,
            IHyracksJobBuilder builder, IOperatorSchema outerPlanSchema) throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        int n = op.getInputs().size();
        IOperatorSchema[] schemas = new IOperatorSchema[n];
        int i = 0;
        for (Mutable<ILogicalOperator> opChild : op.getInputs()) {
            List<Mutable<ILogicalOperator>> parents = operatorVisitedToParents.get(opChild);
            if (parents == null) {
                parents = new ArrayList<Mutable<ILogicalOperator>>();
                operatorVisitedToParents.put(opChild, parents);
                parents.add(opRef);
                compileOpRef(opChild, spec, builder, outerPlanSchema);
                schemas[i++] = context.getSchema(opChild.getValue());
            } else {
                if (!parents.contains(opRef)) {
                    parents.add(opRef);
                }
                schemas[i++] = context.getSchema(opChild.getValue());
                continue;
            }
        }

        IOperatorSchema opSchema = new OperatorSchemaImpl();
        context.putSchema(op, opSchema);
        op.getVariablePropagationPolicy().propagateVariables(opSchema, schemas);
        op.contributeRuntimeOperator(builder, context, opSchema, schemas, outerPlanSchema);
    }

    private void reviseEdges(IHyracksJobBuilder builder) {
        /*
         * revise the edges for the case of replicate operator
         */
        operatorVisitedToParents.forEach((child, parents) -> {
            if (parents.size() > 1) {
                if (child.getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE
                        || child.getValue().getOperatorTag() == LogicalOperatorTag.SPLIT) {
                    AbstractReplicateOperator rop = (AbstractReplicateOperator) child.getValue();
                    if (rop.isBlocker()) {
                        // make the order of the graph edges consistent with the order of rop's outputs
                        List<Mutable<ILogicalOperator>> outputs = rop.getOutputs();
                        for (Mutable<ILogicalOperator> parent : parents) {
                            builder.contributeGraphEdge(child.getValue(), outputs.indexOf(parent), parent.getValue(),
                                    0);
                        }
                    } else {
                        int i = 0;
                        for (Mutable<ILogicalOperator> parent : parents) {
                            builder.contributeGraphEdge(child.getValue(), i, parent.getValue(), 0);
                            i++;
                        }
                    }
                }
            }
        });
    }
}
