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
package org.apache.hyracks.api.client.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.IConstraintAcceptor;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.IActivityClusterGraphGenerator;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobActivityGraph;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.rewriter.ActivityClusterGraphRewriter;

public class JobSpecificationActivityClusterGraphGeneratorFactory implements IActivityClusterGraphGeneratorFactory {
    private static final long serialVersionUID = 1L;

    private final JobSpecification spec;

    public JobSpecificationActivityClusterGraphGeneratorFactory(JobSpecification jobSpec) {
        this.spec = jobSpec;
    }

    @Override
    public JobSpecification getJobSpecification() {
        return spec;
    }

    @Override
    public IActivityClusterGraphGenerator createActivityClusterGraphGenerator(final ICCServiceContext ccServiceCtx,
            Set<JobFlag> jobFlags) throws HyracksException {
        final JobActivityGraphBuilder builder = new JobActivityGraphBuilder(spec, jobFlags);
        PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
            @Override
            public void visit(IConnectorDescriptor conn) throws HyracksException {
                builder.addConnector(conn);
            }
        });
        PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeActivities(builder);
            }
        });
        builder.finish();
        final JobActivityGraph jag = builder.getActivityGraph();
        ActivityClusterGraphBuilder acgb = new ActivityClusterGraphBuilder();

        final ActivityClusterGraph acg = acgb.inferActivityClusters(jag);
        acg.setFrameSize(spec.getFrameSize());
        acg.setMaxReattempts(spec.getMaxReattempts());
        acg.setMaxWarnings(spec.getMaxWarnings());
        acg.setJobletEventListenerFactory(spec.getJobletEventListenerFactory());
        acg.setGlobalJobDataFactory(spec.getGlobalJobDataFactory());
        acg.setConnectorPolicyAssignmentPolicy(spec.getConnectorPolicyAssignmentPolicy());
        acg.setUseConnectorPolicyForScheduling(spec.isUseConnectorPolicyForScheduling());
        final Set<Constraint> constraints = new HashSet<>();
        final IConstraintAcceptor acceptor = new IConstraintAcceptor() {
            @Override
            public void addConstraint(Constraint constraint) {
                constraints.add(constraint);
            }
        };
        PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeSchedulingConstraints(acceptor, ccServiceCtx);
            }
        });
        PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
            @Override
            public void visit(IConnectorDescriptor conn) {
                conn.contributeSchedulingConstraints(acceptor, acg.getConnectorMap().get(conn.getConnectorId()),
                        ccServiceCtx);
            }
        });
        constraints.addAll(spec.getUserConstraints());
        return new IActivityClusterGraphGenerator() {
            @Override
            public ActivityClusterGraph initialize() {
                ActivityClusterGraphRewriter rewriter = new ActivityClusterGraphRewriter();
                rewriter.rewrite(acg);
                return acg;
            }

            @Override
            public Set<Constraint> getConstraints() {
                return constraints;
            }
        };
    }
}
