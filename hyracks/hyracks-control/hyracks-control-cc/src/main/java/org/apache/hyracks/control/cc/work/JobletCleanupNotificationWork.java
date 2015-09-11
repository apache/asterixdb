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

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCApplicationContext;
import org.apache.hyracks.control.cc.job.JobRun;

public class JobletCleanupNotificationWork extends AbstractHeartbeatWork {
    private static final Logger LOGGER = Logger.getLogger(JobletCleanupNotificationWork.class.getName());

    private ClusterControllerService ccs;
    private JobId jobId;
    private String nodeId;

    public JobletCleanupNotificationWork(ClusterControllerService ccs, JobId jobId, String nodeId) {
        super(ccs, nodeId, null);
        this.ccs = ccs;
        this.jobId = jobId;
        this.nodeId = nodeId;
    }

    @Override
    public void runWork() {
        final JobRun run = ccs.getActiveRunMap().get(jobId);
        Set<String> cleanupPendingNodes = run.getCleanupPendingNodeIds();
        if (!cleanupPendingNodes.remove(nodeId)) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning(nodeId + " not in pending cleanup nodes set: " + cleanupPendingNodes + " for Job: "
                        + jobId);
            }
            return;
        }
        NodeControllerState ncs = ccs.getNodeMap().get(nodeId);
        if (ncs != null) {
            ncs.getActiveJobIds().remove(jobId);
        }
        if (cleanupPendingNodes.isEmpty()) {
            CCApplicationContext appCtx = ccs.getApplicationContext();
            if (appCtx != null) {
                try {
                    appCtx.notifyJobFinish(jobId);
                } catch (HyracksException e) {
                    e.printStackTrace();
                }
            }
            run.setStatus(run.getPendingStatus(), run.getPendingExceptions());
            ccs.getActiveRunMap().remove(jobId);
            ccs.getRunMapArchive().put(jobId, run);
            ccs.getRunHistory().put(jobId, run.getExceptions());
        }
    }
}