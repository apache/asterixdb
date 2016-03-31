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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCApplicationContext;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.AbstractWork;

public class JobCleanupWork extends AbstractWork {
    private static final Logger LOGGER = Logger.getLogger(JobCleanupWork.class.getName());

    private ClusterControllerService ccs;
    private JobId jobId;
    private JobStatus status;
    private List<Exception> exceptions;

    public JobCleanupWork(ClusterControllerService ccs, JobId jobId, JobStatus status, List<Exception> exceptions) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.status = status;
        this.exceptions = exceptions;
    }

    @Override
    public void run() {
        LOGGER.info("Cleanup for JobRun with id: " + jobId);
        final JobRun run = ccs.getActiveRunMap().get(jobId);
        if (run == null) {
            LOGGER.warning("Unable to find JobRun with id: " + jobId);
            return;
        }
        if (run.getPendingStatus() != null && run.getCleanupPendingNodeIds().isEmpty()) {
            finishJob(run);
            return;
        }
        if (run.getPendingStatus() != null) {
            LOGGER.warning("Ignoring duplicate cleanup for JobRun with id: " + jobId);
            return;
        }
        Set<String> targetNodes = run.getParticipatingNodeIds();
        run.getCleanupPendingNodeIds().addAll(targetNodes);
        if (run.getPendingStatus() != JobStatus.FAILURE && run.getPendingStatus() != JobStatus.TERMINATED) {
            run.setPendingStatus(status, exceptions);
        }
        if (targetNodes != null && !targetNodes.isEmpty()) {
            Set<String> toDelete = new HashSet<String>();
            for (String n : targetNodes) {
                NodeControllerState ncs = ccs.getNodeMap().get(n);
                try {
                    if (ncs == null) {
                        toDelete.add(n);
                    } else {
                        ncs.getNodeController().cleanUpJoblet(jobId, status);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            targetNodes.removeAll(toDelete);
            run.getCleanupPendingNodeIds().removeAll(toDelete);
            if (run.getCleanupPendingNodeIds().isEmpty()) {
                finishJob(run);
            }
        } else {
            finishJob(run);
        }
    }

    private void finishJob(final JobRun run) {
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

        if (run.getActivityClusterGraph().isReportTaskDetails()) {
            /**
             * log job details when profiling is enabled
             */
            try {
                ccs.getJobLogFile().log(createJobLogObject(run));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private JSONObject createJobLogObject(final JobRun run) {
        JSONObject jobLogObject = new JSONObject();
        try {
            ActivityClusterGraph acg = run.getActivityClusterGraph();
            jobLogObject.put("activity-cluster-graph", acg.toJSON());
            jobLogObject.put("job-run", run.toJSON());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return jobLogObject;
    }

    @Override
    public String toString() {
        return getName() + ": JobId@" + jobId + " Status@" + status
                + (exceptions == null ? "" : " Exceptions@" + exceptions);
    }
}
