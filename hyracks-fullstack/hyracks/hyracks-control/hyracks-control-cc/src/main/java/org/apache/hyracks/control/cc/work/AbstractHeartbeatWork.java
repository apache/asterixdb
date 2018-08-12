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

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public abstract class AbstractHeartbeatWork extends SynchronizableWork {

    protected final ClusterControllerService ccs;
    protected final String nodeId;
    protected final HeartbeatData hbData;

    public AbstractHeartbeatWork(ClusterControllerService ccs, String nodeId, HeartbeatData hbData) {
        this.ccs = ccs;
        this.nodeId = nodeId;
        this.hbData = hbData;
    }

    @Override
    public void doRun() throws Exception {
        INodeManager nodeManager = ccs.getNodeManager();
        NodeControllerState state = nodeManager.getNodeControllerState(nodeId);
        if (state != null) {
            if (hbData != null) {
                state.notifyHeartbeat(hbData);
            } else {
                state.touchHeartbeat();
            }
        }
        runWork();
    }

    public abstract void runWork() throws Exception;

}
