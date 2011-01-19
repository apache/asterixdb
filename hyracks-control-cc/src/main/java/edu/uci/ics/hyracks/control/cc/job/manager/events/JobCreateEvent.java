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
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.constraints.IConstraintExpressionAcceptor;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.IConnectorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobPlanBuilder;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.jobqueue.SynchronizableRunnable;

public class JobCreateEvent extends SynchronizableRunnable {
    private final ClusterControllerService ccs;
    private final byte[] jobSpec;
    private final EnumSet<JobFlag> jobFlags;
    private final UUID jobId;
    private final String appName;

    public JobCreateEvent(ClusterControllerService ccs, UUID jobId, String appName, byte[] jobSpec,
            EnumSet<JobFlag> jobFlags) {
        this.jobId = jobId;
        this.ccs = ccs;
        this.jobSpec = jobSpec;
        this.jobFlags = jobFlags;
        this.appName = appName;
    }

    @Override
    protected void doRun() throws Exception {
        CCApplicationContext appCtx = ccs.getApplicationMap().get(appName);
        if (appCtx == null) {
            throw new HyracksException("No application with id " + appName + " found");
        }
        JobSpecification spec = appCtx.createJobSpecification(jobId, jobSpec);
        JobRun run = plan(jobId, spec, jobFlags);
        run.setStatus(JobStatus.INITIALIZED);

        ccs.getRunMap().put(jobId, run);
        appCtx.notifyJobCreation(jobId, spec);
    }

    public UUID getJobId() {
        return jobId;
    }

    private JobRun plan(UUID jobId, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        final JobPlanBuilder builder = new JobPlanBuilder();
        builder.init(appName, jobId, jobSpec, jobFlags);
        PlanUtils.visit(jobSpec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeTaskGraph(builder);
            }
        });
        final JobPlan plan = builder.getPlan();

        final Set<ConstraintExpression> contributedConstraints = new HashSet<ConstraintExpression>();
        final IConstraintExpressionAcceptor acceptor = new IConstraintExpressionAcceptor() {
            @Override
            public void addConstraintExpression(ConstraintExpression constraintExpression) {
                contributedConstraints.add(constraintExpression);
            }
        };
        PlanUtils.visit(jobSpec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeSchedulingConstraints(acceptor, plan);
            }
        });
        PlanUtils.visit(jobSpec, new IConnectorDescriptorVisitor() {
            @Override
            public void visit(IConnectorDescriptor conn) {
                conn.contributeSchedulingConstraints(acceptor, plan);
            }
        });
        contributedConstraints.addAll(jobSpec.getUserConstraints());

        JobRun run = new JobRun(plan, contributedConstraints);

        return run;
    }
}