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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;

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
        try {
            ccs.getJobLogFile().log(createJobLogObject(run));
        } catch (Exception e) {
            throw new RuntimeException(e);
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
}