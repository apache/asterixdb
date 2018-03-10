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

import static org.apache.hyracks.util.ExitUtil.EC_ABNORMAL_TERMINATION;
import static org.apache.hyracks.util.ExitUtil.EC_NORMAL_TERMINATION;

import java.util.Collection;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.shutdown.ShutdownRun;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterShutdownWork extends SynchronizableWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;
    private final boolean terminateNCService;
    private final IResultCallback<Boolean> callback;

    public ClusterShutdownWork(ClusterControllerService ncs, boolean terminateNCService,
            IResultCallback<Boolean> callback) {
        this.ccs = ncs;
        this.terminateNCService = terminateNCService;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        try {
            if (ccs.getShutdownRun() != null) {
                throw new IPCException("Shutdown already in progress");
            }
            INodeManager nodeManager = ccs.getNodeManager();
            Collection<String> nodeIds = nodeManager.getAllNodeIds();
            /*
             * set up our listener for the node ACKs
             */
            final ShutdownRun shutdownStatus = new ShutdownRun(nodeIds);
            // set up the CC to listen for it
            ccs.setShutdownRun(shutdownStatus);
            /*
             * Shutdown all the nodes...
             */
            nodeManager.apply(this::shutdownNode);

            ccs.getExecutor().execute(() -> {
                try {
                    /*
                     * wait for all our acks
                     */
                    LOGGER.info("Waiting for NCs to shutdown...");
                    boolean cleanShutdown = shutdownStatus.waitForCompletion();
                    if (!cleanShutdown) {
                        /*
                         * best effort - just exit, user will have to kill misbehaving NCs
                         */
                        LOGGER.error("Clean shutdown of NCs timed out- giving up; unresponsive nodes: "
                                + shutdownStatus.getRemainingNodes());
                    }
                    callback.setValue(cleanShutdown);
                    ccs.stop(terminateNCService);
                    ExitUtil.exit(cleanShutdown ? EC_NORMAL_TERMINATION : EC_ABNORMAL_TERMINATION);
                } catch (Exception e) {
                    callback.setException(e);
                }
            });
        } catch (Exception e) {
            callback.setException(e);
        }
    }

    protected void shutdownNode(String nodeId, NodeControllerState ncState) {
        try {
            LOGGER.info("Notifying NC " + nodeId + " to shutdown...");
            ncState.getNodeController().shutdown(terminateNCService);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "Exception shutting down NC " + nodeId + " (possibly dead?), continuing shutdown...",
                    e);
        }
    }
}
