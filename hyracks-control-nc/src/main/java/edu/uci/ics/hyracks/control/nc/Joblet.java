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
package edu.uci.ics.hyracks.control.nc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;

public class Joblet {
    private static final long serialVersionUID = 1L;

    private final NodeControllerService nodeController;

    private final UUID jobId;

    private final Map<UUID, Stagelet> stageletMap;

    private final Map<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>> envMap;

    public Joblet(NodeControllerService nodeController, UUID jobId) {
        this.nodeController = nodeController;
        this.jobId = jobId;
        stageletMap = new HashMap<UUID, Stagelet>();
        envMap = new HashMap<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>>();
    }

    public UUID getJobId() {
        return jobId;
    }

    public IOperatorEnvironment getEnvironment(IOperatorDescriptor hod, int partition) {
        if (!envMap.containsKey(hod.getOperatorId())) {
            envMap.put(hod.getOperatorId(), new HashMap<Integer, IOperatorEnvironment>());
        }
        Map<Integer, IOperatorEnvironment> opEnvMap = envMap.get(hod.getOperatorId());
        if (!opEnvMap.containsKey(partition)) {
            opEnvMap.put(partition, new OperatorEnvironmentImpl());
        }
        return opEnvMap.get(partition);
    }

    private static final class OperatorEnvironmentImpl implements IOperatorEnvironment {
        private final Map<String, Object> map;

        public OperatorEnvironmentImpl() {
            map = new HashMap<String, Object>();
        }

        @Override
        public Object get(String name) {
            return map.get(name);
        }

        @Override
        public void set(String name, Object value) {
            map.put(name, value);
        }
    }

    public void setStagelet(UUID stageId, Stagelet stagelet) {
        stageletMap.put(stageId, stagelet);
    }

    public Stagelet getStagelet(UUID stageId) throws Exception {
        return stageletMap.get(stageId);
    }

    public Executor getExecutor() {
        return nodeController.getExecutor();
    }

    public synchronized void notifyStageletComplete(UUID stageId, int attempt, Map<String, Long> stats)
            throws Exception {
        stageletMap.remove(stageId);
        nodeController.notifyStageComplete(jobId, stageId, attempt, stats);
    }

    public void notifyStageletFailed(UUID stageId, int attempt) throws Exception {
        stageletMap.remove(stageId);
        nodeController.notifyStageFailed(jobId, stageId, attempt);
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public void dumpProfile(Map<String, Long> counterDump) {
        Set<UUID> stageIds;
        synchronized (this) {
            stageIds = new HashSet<UUID>(stageletMap.keySet());
        }
        for (UUID stageId : stageIds) {
            Stagelet si;
            synchronized (this) {
                si = stageletMap.get(stageId);
            }
            if (si != null) {
                si.dumpProfile(counterDump);
            }
        }
    }
}