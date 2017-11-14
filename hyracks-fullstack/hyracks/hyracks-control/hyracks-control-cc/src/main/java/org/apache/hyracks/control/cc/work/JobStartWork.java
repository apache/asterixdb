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

import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.IActivityClusterGraphGenerator;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class JobStartWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final byte[] acggfBytes;
    private final Set<JobFlag> jobFlags;
    private final DeploymentId deploymentId;
    private final IResultCallback<JobId> callback;
    private final JobIdFactory jobIdFactory;
    private final DeployedJobSpecId deployedJobSpecId;
    private final Map<byte[], byte[]> jobParameters;

    public JobStartWork(ClusterControllerService ccs, DeploymentId deploymentId, byte[] acggfBytes,
            Set<JobFlag> jobFlags, JobIdFactory jobIdFactory, Map<byte[], byte[]> jobParameters,
            IResultCallback<JobId> callback, DeployedJobSpecId deployedJobSpecId) {
        this.deploymentId = deploymentId;
        this.ccs = ccs;
        this.acggfBytes = acggfBytes;
        this.jobFlags = jobFlags;
        this.callback = callback;
        this.deployedJobSpecId = deployedJobSpecId;
        this.jobParameters = jobParameters;
        this.jobIdFactory = jobIdFactory;
    }

    @Override
    protected void doRun() throws Exception {
        IJobManager jobManager = ccs.getJobManager();
        try {
            final CCServiceContext ccServiceCtx = ccs.getContext();
            JobId jobId;
            JobRun run;
            jobId = jobIdFactory.create();
            if (deployedJobSpecId == null) {
                //Need to create the ActivityClusterGraph
                IActivityClusterGraphGeneratorFactory acggf = (IActivityClusterGraphGeneratorFactory) DeploymentUtils
                        .deserialize(acggfBytes, deploymentId, ccServiceCtx);
                IActivityClusterGraphGenerator acgg = acggf.createActivityClusterGraphGenerator(ccServiceCtx, jobFlags);
                run = new JobRun(ccs, deploymentId, jobId, acggf, acgg, jobFlags);
            } else {
                //ActivityClusterGraph has already been distributed
                run = new JobRun(ccs, deploymentId, jobId, jobFlags,
                        ccs.getDeployedJobSpecStore().getDeployedJobSpecDescriptor(deployedJobSpecId), jobParameters,
                        deployedJobSpecId);
            }
            jobManager.add(run);
            callback.setValue(jobId);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
