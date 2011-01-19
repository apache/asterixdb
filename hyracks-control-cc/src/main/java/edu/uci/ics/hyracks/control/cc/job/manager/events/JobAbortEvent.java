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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.job.JobAttempt;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.manager.JobLifecycleHelper;

public class JobAbortEvent implements Runnable {
    private final ClusterControllerService ccs;

    private final UUID jobId;

    public JobAbortEvent(ClusterControllerService ccs, UUID jobId) {
        this.ccs = ccs;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        Map<UUID, JobRun> runMap = ccs.getRunMap();
        final JobRun run = runMap.get(jobId);
        final Set<String> targetNodes = new HashSet<String>();
        if (run != null) {
            List<JobAttempt> attempts = run.getAttempts();
            JobAttempt attempt = attempts.get(attempts.size() - 1);
            for (String runningNodeId : attempt.getParticipatingNodeIds()) {
                if (nodeMap.containsKey(runningNodeId)) {
                    targetNodes.add(runningNodeId);
                    nodeMap.get(runningNodeId).getActiveJobIds().remove(jobId);
                }
            }
        }

        JobLifecycleHelper.abortJob(ccs, jobId, targetNodes);
    }
}