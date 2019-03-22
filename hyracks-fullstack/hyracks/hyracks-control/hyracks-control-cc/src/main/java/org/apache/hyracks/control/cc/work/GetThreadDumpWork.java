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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.util.ThreadDumpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GetThreadDumpWork extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final int TIMEOUT_SECS = 60;

    private final ClusterControllerService ccs;
    private final String nodeId;
    private final IResultCallback<String> callback;
    private final ThreadDumpRun run;

    public GetThreadDumpWork(ClusterControllerService ccs, String nodeId, IResultCallback<String> callback) {
        this.ccs = ccs;
        this.nodeId = nodeId;
        this.callback = callback;
        this.run = new ThreadDumpRun(UUID.randomUUID().toString());
    }

    @Override
    public void run() {
        if (nodeId == null) {
            // null nodeId means the request is for the cluster controller
            try {
                callback.setValue(ThreadDumpUtil.takeDumpJSONString());
            } catch (Exception e) {
                LOGGER.log(Level.WARN, "Exception taking CC thread dump", e);
                callback.setException(e);
            }
        } else {
            INodeManager nodeManager = ccs.getNodeManager();
            final NodeControllerState ncState = nodeManager.getNodeControllerState(nodeId);
            if (ncState == null) {
                // bad node id, reply with null immediately
                callback.setValue(null);
            } else {
                ccs.addThreadDumpRun(run.getRequestId(), run);
                try {
                    ncState.getNodeController().takeThreadDump(run.getRequestId());
                } catch (Exception e) {
                    ccs.removeThreadDumpRun(run.getRequestId());
                    callback.setException(e);
                }
                final long requestTime = System.currentTimeMillis();
                ccs.getExecutor().execute(() -> {
                    try {
                        final long queueTime = System.currentTimeMillis() - requestTime;
                        final long sleepTime = TimeUnit.SECONDS.toMillis(TIMEOUT_SECS) - queueTime;
                        if (sleepTime > 0) {
                            Thread.sleep(sleepTime);
                        }
                        if (ccs.removeThreadDumpRun(run.getRequestId()) != null) {
                            LOGGER.log(Level.WARN,
                                    "Timed out thread dump request " + run.getRequestId() + " for node " + nodeId);
                            callback.setException(new TimeoutException("Thread dump request for node " + nodeId
                                    + " timed out after " + TIMEOUT_SECS + " seconds."));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
    }

    public class ThreadDumpRun {
        private final String requestId;

        public ThreadDumpRun(String requestId) {
            this.requestId = requestId;
        }

        public String getRequestId() {
            return requestId;
        }

        public synchronized void notifyThreadDumpReceived(String threadDumpJSON) {
            callback.setValue(threadDumpJSON);
        }
    }
}
