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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobletCleanupNotificationWork extends AbstractHeartbeatWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private JobId jobId;

    public JobletCleanupNotificationWork(ClusterControllerService ccs, JobId jobId, String nodeId) {
        super(ccs, nodeId, null);
        this.jobId = jobId;
    }

    @Override
    public void runWork() {
        IJobManager jobManager = ccs.getJobManager();
        final JobRun run = jobManager.get(jobId);
        if (run == null) {
            LOGGER.log(Level.WARN, () -> "ignoring unknown job " + jobId + " on notification from " + nodeId);
            return;
        }
        Set<String> cleanupPendingNodes = run.getCleanupPendingNodeIds();
        if (!cleanupPendingNodes.remove(nodeId)) {
            LOGGER.log(Level.WARN,
                    () -> nodeId + " not in pending cleanup nodes set: " + cleanupPendingNodes + " for job " + jobId);
            return;
        }
        INodeManager nodeManager = ccs.getNodeManager();
        NodeControllerState ncs = nodeManager.getNodeControllerState(nodeId);
        if (ncs != null) {
            ncs.getActiveJobIds().remove(jobId);
        }
        if (cleanupPendingNodes.isEmpty()) {
            try {
                jobManager.finalComplete(run);
            } catch (HyracksException e) {
                // Fail the job with the caught exception during final completion.
                List<Exception> completionException = new ArrayList<>();
                if (run.getExceptions() != null && !run.getExceptions().isEmpty()) {
                    completionException.addAll(run.getExceptions());
                }
                completionException.add(0, e);
                run.setStatus(JobStatus.FAILURE, completionException);
            }
        }
    }
}
