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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.base.INodeController;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.ipc.api.IIPCHandle;

public class RegisterNodeWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(RegisterNodeWork.class.getName());

    private final ClusterControllerService ccs;
    private final NodeRegistration reg;

    public RegisterNodeWork(ClusterControllerService ccs, NodeRegistration reg) {
        this.ccs = ccs;
        this.reg = reg;
    }

    @Override
    protected void doRun() throws Exception {
        String id = reg.getNodeId();
        IIPCHandle ncIPCHandle = ccs.getClusterIPC().getHandle(reg.getNodeControllerAddress());
        CCNCFunctions.NodeRegistrationResult result;
        Map<IOption, Object> ncConfiguration = new HashMap<>();
        try {
            INodeController nc = new NodeControllerRemoteProxy(ccs.getClusterIPC(), reg.getNodeControllerAddress());
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
            params.setHeartbeatPeriod(ccs.getCCConfig().getHeartbeatPeriod());
            params.setProfileDumpPeriod(ccs.getCCConfig().getProfileDumpPeriod());
            result = new CCNCFunctions.NodeRegistrationResult(params, null);
            ccs.getJobIdFactory().ensureMinimumId(reg.getMaxJobId() + 1);
        } catch (Exception e) {
            result = new CCNCFunctions.NodeRegistrationResult(null, e);
        }
        ncIPCHandle.send(-1, result, null);
        ccs.getContext().notifyNodeJoin(id, ncConfiguration);
    }
}
