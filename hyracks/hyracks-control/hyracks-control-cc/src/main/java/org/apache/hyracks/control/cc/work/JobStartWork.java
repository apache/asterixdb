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
package org.apache.hyracks.control.cc.work;

import java.util.Collections;
import java.util.EnumSet;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.IActivityClusterGraphGenerator;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCApplicationContext;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class JobStartWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final byte[] acggfBytes;
    private final EnumSet<JobFlag> jobFlags;
    private final DeploymentId deploymentId;
    private final JobId jobId;
    private final IResultCallback<JobId> callback;

    public JobStartWork(ClusterControllerService ccs, DeploymentId deploymentId, byte[] acggfBytes,
            EnumSet<JobFlag> jobFlags, JobId jobId, IResultCallback<JobId> callback) {
        this.deploymentId = deploymentId;
        this.jobId = jobId;
        this.ccs = ccs;
        this.acggfBytes = acggfBytes;
        this.jobFlags = jobFlags;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            final CCApplicationContext appCtx = ccs.getApplicationContext();
            IActivityClusterGraphGeneratorFactory acggf = (IActivityClusterGraphGeneratorFactory) DeploymentUtils
                    .deserialize(acggfBytes, deploymentId, appCtx);
            IActivityClusterGraphGenerator acgg = acggf.createActivityClusterGraphGenerator(jobId, appCtx, jobFlags);
            JobRun run = new JobRun(ccs, deploymentId, jobId, acgg, jobFlags);
            run.setStatus(JobStatus.INITIALIZED, null);
            ccs.getActiveRunMap().put(jobId, run);
            appCtx.notifyJobCreation(jobId, acggf);
            run.setStatus(JobStatus.RUNNING, null);
            try {
                run.getScheduler().startJob();
            } catch (Exception e) {
                ccs.getWorkQueue().schedule(
                        new JobCleanupWork(ccs, run.getJobId(), JobStatus.FAILURE, Collections.singletonList(e)));
            }
            callback.setValue(jobId);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}