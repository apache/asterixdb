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
import org.apache.hyracks.control.common.shutdown.ShutdownRun;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NotifyShutdownWork extends SynchronizableWork {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ClusterControllerService ccs;
    private final String nodeId;

    public NotifyShutdownWork(ClusterControllerService ccs, String nodeId) {
        this.ccs = ccs;
        this.nodeId = nodeId;

    }

    @Override
    public void doRun() {
        // Triggered remotely by a NC to notify that the NC is shutting down.
        ShutdownRun sRun = ccs.getShutdownRun();
        if (sRun != null) {
            LOGGER.info("Received shutdown acknowledgement from node " + nodeId);
            sRun.notifyShutdown(nodeId);
        } else {
            LOGGER.info("Received unsolicted shutdown notification from node " + nodeId);
        }
    }

}
