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
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.AbstractWork;

public class RemoveDeadNodesWork extends AbstractWork {
    private static Logger LOGGER = Logger.getLogger(RemoveDeadNodesWork.class.getName());

    private final ClusterControllerService ccs;

    public RemoveDeadNodesWork(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void run() {
        Set<String> deadNodes = new HashSet<String>();
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        for (Map.Entry<String, NodeControllerState> e : nodeMap.entrySet()) {
            NodeControllerState state = e.getValue();
            if (state.incrementLastHeartbeatDuration() >= ccs.getConfig().maxHeartbeatLapsePeriods) {
                deadNodes.add(e.getKey());
                LOGGER.info(e.getKey() + " considered dead");
            }
        }
        Set<JobId> affectedJobIds = new HashSet<JobId>();
        for (String deadNode : deadNodes) {
            NodeControllerState state = nodeMap.remove(deadNode);

            // Deal with dead tasks.
            affectedJobIds.addAll(state.getActiveJobIds());
        }
        int size = affectedJobIds.size();
        if (size > 0) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Number of affected jobs: " + size);
            }
            for (JobId jobId : affectedJobIds) {
                JobRun run = ccs.getActiveRunMap().get(jobId);
                if (run != null) {
                    run.getScheduler().notifyNodeFailures(deadNodes);
                }
            }
        }
        if (deadNodes != null && deadNodes.size() > 0) {
            ccs.getApplicationContext().notifyNodeFailure(deadNodes);
        }
    }

    @Override
    public Level logLevel() {
        return Level.FINE;
    }
}