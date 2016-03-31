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

import java.util.logging.Logger;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.deployment.DeploymentRun;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.shutdown.ShutdownRun;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class NotifyShutdownWork extends SynchronizableWork {

    private final ClusterControllerService ccs;
    private final String nodeId;
    private static Logger LOGGER = Logger.getLogger(NotifyShutdownWork.class.getName());

    public NotifyShutdownWork(ClusterControllerService ccs, String nodeId) {
        this.ccs = ccs;
        this.nodeId = nodeId;

    }

    @Override
    public void doRun() {
        /** triggered remotely by a NC to notify that the NC is shutting down */
        ShutdownRun sRun = ccs.getShutdownRun();
        LOGGER.info("Recieved shutdown acknowledgement from NC ID:" + nodeId);
        sRun.notifyShutdown(nodeId);
    }

}
