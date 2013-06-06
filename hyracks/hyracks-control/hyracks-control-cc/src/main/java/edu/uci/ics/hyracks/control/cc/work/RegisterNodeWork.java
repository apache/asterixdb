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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.ipc.CCNCFunctions;
import edu.uci.ics.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;

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
        CCNCFunctions.NodeRegistrationResult result = null;
        Map<String, String> ncConfiguration = null;
        try {
            INodeController nodeController = new NodeControllerRemoteProxy(ncIPCHandle);

            NodeControllerState state = new NodeControllerState(nodeController, reg);
            Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
            if (nodeMap.containsKey(id)) {
                throw new Exception("Node with this name already registered.");
            }
            nodeMap.put(id, state);
            Map<String, Set<String>> ipAddressNodeNameMap = ccs.getIpAddressNodeNameMap();
            String ipAddress = state.getNCConfig().dataIPAddress;
            ncConfiguration = new HashMap<String, String>();
            state.getNCConfig().toMap(ncConfiguration);
            Set<String> nodes = ipAddressNodeNameMap.get(ipAddress);
            if (nodes == null) {
                nodes = new HashSet<String>();
                ipAddressNodeNameMap.put(ipAddress, nodes);
            }
            nodes.add(id);
            LOGGER.log(Level.INFO, "Registered INodeController: id = " + id);
            NodeParameters params = new NodeParameters();
            params.setClusterControllerInfo(ccs.getClusterControllerInfo());
            params.setDistributedState(ccs.getApplicationContext().getDistributedState());
            params.setHeartbeatPeriod(ccs.getCCConfig().heartbeatPeriod);
            params.setProfileDumpPeriod(ccs.getCCConfig().profileDumpPeriod);
            result = new CCNCFunctions.NodeRegistrationResult(params, null);
        } catch (Exception e) {
            result = new CCNCFunctions.NodeRegistrationResult(null, e);
        }
        ncIPCHandle.send(-1, result, null);
        ccs.getApplicationContext().notifyNodeJoin(id, ncConfiguration);
    }
}