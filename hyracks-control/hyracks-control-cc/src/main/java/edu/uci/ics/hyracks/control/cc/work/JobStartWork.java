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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.IConnectorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobActivityGraphBuilder;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.scheduler.JobScheduler;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class JobStartWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final byte[] jobSpec;
    private final EnumSet<JobFlag> jobFlags;
    private final JobId jobId;
    private final String appName;
    private final IResultCallback<JobId> callback;

    public JobStartWork(ClusterControllerService ccs, String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags,
            JobId jobId, IResultCallback<JobId> callback) {
        this.jobId = jobId;
        this.ccs = ccs;
        this.jobSpec = jobSpec;
        this.jobFlags = jobFlags;
        this.appName = appName;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            final CCApplicationContext appCtx = ccs.getApplicationMap().get(appName);
            if (appCtx == null) {
                throw new HyracksException("No application with id " + appName + " found");
            }
            JobSpecification spec = appCtx.createJobSpecification(jobSpec);

            final JobActivityGraphBuilder builder = new JobActivityGraphBuilder(appName, spec, jobFlags);
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

            JobRun run = new JobRun(jobId, jag);

            run.setStatus(JobStatus.INITIALIZED, null);

            ccs.getActiveRunMap().put(jobId, run);
            final Set<Constraint> contributedConstraints = new HashSet<Constraint>();
            final IConstraintAcceptor acceptor = new IConstraintAcceptor() {
                @Override
                public void addConstraint(Constraint constraint) {
                    contributedConstraints.add(constraint);
                }
            };
            PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
                @Override
                public void visit(IOperatorDescriptor op) {
                    op.contributeSchedulingConstraints(acceptor, jag, appCtx);
                }
            });
            PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
                @Override
                public void visit(IConnectorDescriptor conn) {
                    conn.contributeSchedulingConstraints(acceptor, jag, appCtx);
                }
            });
            contributedConstraints.addAll(spec.getUserConstraints());

            JobScheduler jrs = new JobScheduler(ccs, run, contributedConstraints);
            run.setScheduler(jrs);
            appCtx.notifyJobCreation(jobId, spec);
            run.setStatus(JobStatus.RUNNING, null);
            try {
                run.getScheduler().startJob();
            } catch (Exception e) {
                ccs.getWorkQueue().schedule(new JobCleanupWork(ccs, run.getJobId(), JobStatus.FAILURE, e));
            }
            callback.setValue(jobId);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}