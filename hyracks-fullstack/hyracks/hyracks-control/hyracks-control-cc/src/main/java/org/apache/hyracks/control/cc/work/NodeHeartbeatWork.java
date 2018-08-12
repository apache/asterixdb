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

import java.net.InetSocketAddress;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.heartbeat.HeartbeatData;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.logging.log4j.Level;

public class NodeHeartbeatWork extends AbstractHeartbeatWork {

    private final InetSocketAddress ncAddress;

    public NodeHeartbeatWork(ClusterControllerService ccs, String nodeId, HeartbeatData hbData,
            InetSocketAddress ncAddress) {
        super(ccs, nodeId, hbData);
        this.ncAddress = ncAddress;
    }

    @Override
    public void runWork() throws Exception {
        INodeManager nodeManager = ccs.getNodeManager();
        final NodeControllerState ncState = nodeManager.getNodeControllerState(nodeId);
        if (ncState != null) {
            ncState.getNodeController().heartbeatAck(ccs.getCcId(), null);
        } else {
            // unregistered nc- let him know
            NodeControllerRemoteProxy nc =
                    new NodeControllerRemoteProxy(ccs.getCcId(), ccs.getClusterIPC().getReconnectingHandle(ncAddress));
            nc.heartbeatAck(ccs.getCcId(), HyracksDataException.create(ErrorCode.NO_SUCH_NODE, nodeId));
        }
    }

    @Override
    public Level logLevel() {
        return Level.TRACE;
    }
}
