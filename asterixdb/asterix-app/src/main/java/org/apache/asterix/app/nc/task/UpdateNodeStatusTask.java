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
package org.apache.asterix.app.nc.task;

import java.util.Set;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.storage.IReplicaManager;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateNodeStatusTask implements INCLifecycleTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 2L;
    private final NodeStatus status;
    private final Set<Integer> activePartitions;

    public UpdateNodeStatusTask(NodeStatus status, Set<Integer> activePartitions) {
        this.status = status;
        this.activePartitions = activePartitions;
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        NodeControllerService ncs = (NodeControllerService) cs;
        ncs.setNodeStatus(status);
        if (status != NodeStatus.ACTIVE) {
            updateNodeActivePartitions(cs);
        }
    }

    private void updateNodeActivePartitions(IControllerService cs) {
        INcApplicationContext appCtx = (INcApplicationContext) cs.getApplicationContext();
        IReplicaManager replicaManager = appCtx.getReplicaManager();
        LOGGER.info("updating node active partitions to {}", activePartitions);
        replicaManager.setActivePartitions(activePartitions);
    }

    @Override
    public String toString() {
        return "UpdateNodeStatusTask{" + "status=" + status + ", activePartitions=" + activePartitions + '}';
    }
}
