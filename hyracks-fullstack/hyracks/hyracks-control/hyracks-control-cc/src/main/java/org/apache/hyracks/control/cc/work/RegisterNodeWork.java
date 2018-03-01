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

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegisterNodeWork extends SynchronizableWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;
    private final NodeRegistration reg;
    private final int registrationId;

    public RegisterNodeWork(ClusterControllerService ccs, NodeRegistration reg, int registrationId) {
        this.ccs = ccs;
        this.reg = reg;
        this.registrationId = registrationId;
    }

    @Override
    protected void doRun() throws Exception {
        String id = reg.getNodeId();
        // TODO(mblow): it seems we should close IPC handles when we're done with them (like here)
        IIPCHandle ncIPCHandle = ccs.getClusterIPC().getHandle(reg.getNodeControllerAddress());
        CCNCFunctions.NodeRegistrationResult result;
        Map<IOption, Object> ncConfiguration = new HashMap<>();
        try {
            LOGGER.log(Level.WARN, "Registering INodeController: id = " + id);
            NodeControllerRemoteProxy nc = new NodeControllerRemoteProxy(ccs.getCcId(),
                    ccs.getClusterIPC().getReconnectingHandle(reg.getNodeControllerAddress()));
            NodeControllerState state = new NodeControllerState(nc, reg);
            INodeManager nodeManager = ccs.getNodeManager();
            nodeManager.addNode(id, state);
            IApplicationConfig cfg = state.getNCConfig().getConfigManager().getNodeEffectiveConfig(id);
            for (IOption option : cfg.getOptions()) {
                ncConfiguration.put(option, cfg.get(option));
            }
            LOGGER.log(Level.INFO, "Registered INodeController: id = " + id);
            NodeParameters params = new NodeParameters();
            params.setClusterControllerInfo(ccs.getClusterControllerInfo());
            params.setDistributedState(ccs.getContext().getDistributedState());
            params.setHeartbeatPeriod(ccs.getCCConfig().getHeartbeatPeriodMillis());
            params.setProfileDumpPeriod(ccs.getCCConfig().getProfileDumpPeriod());
            params.setRegistrationId(registrationId);
            result = new CCNCFunctions.NodeRegistrationResult(params, null);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Node registration failed", e);
            result = new CCNCFunctions.NodeRegistrationResult(null, e);
        }
        LOGGER.warn("sending registration response to node");
        ncIPCHandle.send(-1, result, null);
        LOGGER.warn("notifying node join");
        ccs.getContext().notifyNodeJoin(id, ncConfiguration);
    }
}
