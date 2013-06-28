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
package edu.uci.ics.hyracks.algebricks.core.jobgen.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.api.job.IJobletEventListenerFactory;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class PlanCompiler {
    private JobGenContext context;
    private Map<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> operatorVisitedToParents = new HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>>();

    public PlanCompiler(JobGenContext context) {
        this.context = context;
    }

    public JobGenContext getContext() {
        return context;
    }

    public JobSpecification compilePlan(ILogicalPlan plan, IOperatorSchema outerPlanSchema, IJobletEventListenerFactory jobEventListenerFactory) throws AlgebricksException {
        JobSpecification spec = new JobSpecification();
        if (jobEventListenerFactory != null) {
            spec.setJobletEventListenerFactory(jobEventListenerFactory);
        }
        List<ILogicalOperator> rootOps = new ArrayList<ILogicalOperator>();
        IHyracksJobBuilder builder = new JobBuilder(spec, context.getClusterLocations());
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
        return spec;
    }

    private void compileOpRef(Mutable<ILogicalOperator> opRef, IOperatorDescriptorRegistry spec, IHyracksJobBuilder builder,
            IOperatorSchema outerPlanSchema) throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        int n = op.getInputs().size();
        IOperatorSchema[] schemas = new IOperatorSchema[n];
        int i = 0;
        for (Mutable<ILogicalOperator> opRef2 : op.getInputs()) {
            List<Mutable<ILogicalOperator>> parents = operatorVisitedToParents.get(opRef2);
            if (parents == null) {
                parents = new ArrayList<Mutable<ILogicalOperator>>();
                operatorVisitedToParents.put(opRef2, parents);
                parents.add(opRef);
                compileOpRef(opRef2, spec, builder, outerPlanSchema);
                schemas[i++] = context.getSchema(opRef2.getValue());
            } else {
                if (!parents.contains(opRef))
                    parents.add(opRef);
                schemas[i++] = context.getSchema(opRef2.getValue());
                continue;
            }
        }

        IOperatorSchema opSchema = new OperatorSchemaImpl();
        context.putSchema(op, opSchema);
        op.getVariablePropagationPolicy().propagateVariables(opSchema, schemas);
        op.contributeRuntimeOperator(builder, context, opSchema, schemas, outerPlanSchema);
    }

    private void reviseEdges(IHyracksJobBuilder builder) {
        /**
         * revise the edges for the case of replicate operator
         */
        for (Entry<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> entry : operatorVisitedToParents
                .entrySet()) {
            Mutable<ILogicalOperator> child = entry.getKey();
            List<Mutable<ILogicalOperator>> parents = entry.getValue();
            if (parents.size() > 1) {
                int i = 0;
                for (Mutable<ILogicalOperator> parent : parents) {
                    builder.contributeGraphEdge(child.getValue(), i, parent.getValue(), 0);
                    i++;
                }
            }
        }
    }
}
