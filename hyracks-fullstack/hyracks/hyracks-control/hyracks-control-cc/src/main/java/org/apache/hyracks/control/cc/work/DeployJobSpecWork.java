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

import java.util.EnumSet;

import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.IActivityClusterGraphGenerator;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class DeployJobSpecWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final byte[] acggfBytes;
    private final DeployedJobSpecId deployedJobSpecId;
    private final IResultCallback<DeployedJobSpecId> callback;
    private final boolean upsert;

    public DeployJobSpecWork(ClusterControllerService ccs, byte[] acggfBytes, DeployedJobSpecId deployedJobSpecId,
            boolean upsert, IResultCallback<DeployedJobSpecId> callback) {
        this.deployedJobSpecId = deployedJobSpecId;
        this.ccs = ccs;
        this.acggfBytes = acggfBytes;
        this.callback = callback;
        this.upsert = upsert;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            final CCServiceContext ccServiceCtx = ccs.getContext();
            if (!upsert) {
                ccs.getDeployedJobSpecStore().checkForExistingDeployedJobSpecDescriptor(deployedJobSpecId);
            }
            IActivityClusterGraphGeneratorFactory acggf =
                    (IActivityClusterGraphGeneratorFactory) DeploymentUtils.deserialize(acggfBytes, null, ccServiceCtx);
            IActivityClusterGraphGenerator acgg =
                    acggf.createActivityClusterGraphGenerator(ccServiceCtx, EnumSet.noneOf(JobFlag.class));
            ActivityClusterGraph acg = acgg.initialize();
            ccs.getDeployedJobSpecStore().addDeployedJobSpecDescriptor(deployedJobSpecId, acg,
                    acggf.getJobSpecification(), acgg.getConstraints());

            byte[] acgBytes = JavaSerializationUtils.serialize(acg);

            INodeManager nodeManager = ccs.getNodeManager();
            for (NodeControllerState node : nodeManager.getAllNodeControllerStates()) {
                node.getNodeController().deployJobSpec(deployedJobSpecId, acgBytes, upsert);
            }
            callback.setValue(deployedJobSpecId);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}
