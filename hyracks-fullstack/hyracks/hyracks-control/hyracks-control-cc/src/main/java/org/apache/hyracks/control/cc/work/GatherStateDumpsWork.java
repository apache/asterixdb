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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class GatherStateDumpsWork extends SynchronizableWork {
    private final ClusterControllerService ccs;

    private final StateDumpRun sdr;

    public GatherStateDumpsWork(ClusterControllerService ccs) {
        this.ccs = ccs;
        this.sdr = new StateDumpRun(ccs);
    }

    @Override
    public void doRun() throws Exception {
        ccs.addStateDumpRun(sdr.stateDumpId, sdr);
        INodeManager nodeManager = ccs.getNodeManager();
        Collection<String> nodeIds = new HashSet<>();
        nodeIds.addAll(nodeManager.getAllNodeIds());
        sdr.setNCs(nodeIds);
        for (NodeControllerState ncs : nodeManager.getAllNodeControllerStates()) {
            ncs.getNodeController().dumpState(sdr.stateDumpId);
        }
    }

    public StateDumpRun getStateDumpRun() {
        return sdr;
    }

    public static class StateDumpRun {

        private final ClusterControllerService ccs;

        private final String stateDumpId;

        private final Map<String, String> ncStates;

        private Collection<String> ncIds;

        private boolean complete;

        public StateDumpRun(ClusterControllerService ccs) {
            this.ccs = ccs;
            stateDumpId = UUID.randomUUID().toString();
            ncStates = new HashMap<>();
            complete = false;
        }

        public void setNCs(Collection<String> ncIds) {
            this.ncIds = ncIds;
        }

        public Map<String, String> getStateDump() {
            return ncStates;
        }

        public synchronized void notifyStateDumpReceived(String nodeId, String state) {
            ncIds.remove(nodeId);
            ncStates.put(nodeId, state);
            if (ncIds.size() == 0) {
                complete = true;
                ccs.removeStateDumpRun(stateDumpId);
                notifyAll();
            }
        }

        public synchronized void waitForCompletion() throws InterruptedException {
            while (!complete) {
                wait();
            }
        }

        public String getStateDumpId() {
            return stateDumpId;
        }
    }

}
