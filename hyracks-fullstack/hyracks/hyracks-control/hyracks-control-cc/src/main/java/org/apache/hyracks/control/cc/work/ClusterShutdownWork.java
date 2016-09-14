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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.common.shutdown.ShutdownRun;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.ipc.exceptions.IPCException;

public class ClusterShutdownWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(ClusterShutdownWork.class.getName());

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
                throw new IPCException("Shutdown in Progress");
            }
            Map<String, NodeControllerState> nodeControllerStateMap = ccs.getNodeMap();
            Set<String> nodeIds = new TreeSet<String>();
            nodeIds.addAll(nodeControllerStateMap.keySet());
            /**
             * set up our listener for the node ACKs
             */
            final ShutdownRun shutdownStatus = new ShutdownRun(nodeIds);
            // set up the CC to listen for it
            ccs.setShutdownRun(shutdownStatus);
            /**
             * Shutdown all the nodes...
             */
            nodeControllerStateMap.forEach(this::shutdownNode);

            ccs.getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        /**
                         * wait for all our acks
                         */
                        boolean cleanShutdown = shutdownStatus.waitForCompletion();
                        if (cleanShutdown) {
                            callback.setValue(true);
                            ccs.stop();
                            LOGGER.info("JVM Exiting.. Bye!");
                            Runtime rt = Runtime.getRuntime();
                            rt.exit(0);
                        }
                        /**
                         * best effort - just exit, user will have to kill misbehaving NCs
                         */
                        else {
                            LOGGER.severe("Clean shutdown of NCs timed out- CC bailing out!");
                            StringBuilder unresponsive = new StringBuilder();
                            for (String s : shutdownStatus.getRemainingNodes()) {
                                unresponsive.append(s).append(' ');
                            }
                            LOGGER.severe("Unresponsive Nodes: " + unresponsive);
                            callback.setValue(false);
                            ccs.stop();
                            LOGGER.info("JVM Exiting.. Bye!");
                            Runtime rt = Runtime.getRuntime();
                            rt.exit(1);
                        }
                    } catch (Exception e) {
                        callback.setException(e);
                    }
                }
            });
        } catch (Exception e) {
            callback.setException(e);
        }
    }

    protected void shutdownNode(String key, NodeControllerState ncState) {
        try {
            ncState.getNodeController().shutdown(terminateNCService);
        } catch (Exception e) {
            LOGGER.log(
                    Level.INFO, "Exception shutting down NC " + key + " (possibly dead?), continuing shutdown...", e);
        }
    }
}
