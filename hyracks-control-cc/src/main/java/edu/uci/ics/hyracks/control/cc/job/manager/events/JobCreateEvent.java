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
import java.util.UUID;

import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobActivityGraphBuilder;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.jobqueue.SynchronizableEvent;

public class JobCreateEvent extends SynchronizableEvent {
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

        final JobActivityGraphBuilder builder = new JobActivityGraphBuilder();
        builder.init(appName, spec, jobFlags);
        PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeTaskGraph(builder);
            }
        });
        final JobActivityGraph jag = builder.getActivityGraph();

        JobRun run = new JobRun(jobId, jag);

        run.setStatus(JobStatus.INITIALIZED, null);

        ccs.getRunMap().put(jobId, run);
        ccs.getScheduler().notifyJobCreation(run);
        appCtx.notifyJobCreation(jobId, spec);
    }

    public UUID getJobId() {
        return jobId;
    }
}