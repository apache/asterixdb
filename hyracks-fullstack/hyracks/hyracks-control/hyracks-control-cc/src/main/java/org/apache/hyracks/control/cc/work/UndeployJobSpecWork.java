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

import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class UndeployJobSpecWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final DeployedJobSpecId deployedJobSpecId;
    private final IResultCallback<Void> callback;

    public UndeployJobSpecWork(ClusterControllerService ccs, DeployedJobSpecId deployedJobSpecId,
            IResultCallback<Void> callback) {
        this.deployedJobSpecId = deployedJobSpecId;
        this.ccs = ccs;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            ccs.getDeployedJobSpecStore().removeDeployedJobSpecDescriptor(deployedJobSpecId);
            INodeManager nodeManager = ccs.getNodeManager();
            for (NodeControllerState node : nodeManager.getAllNodeControllerStates()) {
                node.getNodeController().undeployJobSpec(deployedJobSpecId);
            }
            callback.setValue(null);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
