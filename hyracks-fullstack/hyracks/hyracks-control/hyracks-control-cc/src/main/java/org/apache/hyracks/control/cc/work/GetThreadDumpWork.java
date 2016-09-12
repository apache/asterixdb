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

import java.lang.management.ManagementFactory;
import java.util.UUID;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.ThreadDumpWork;

public class GetThreadDumpWork extends ThreadDumpWork {
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
    protected void doRun() throws Exception {
        if (nodeId == null) {
            // null nodeId means the request is for the cluster controller
            callback.setValue(takeDump(ManagementFactory.getThreadMXBean()));
        } else {
            final NodeControllerState ncState = ccs.getNodeMap().get(nodeId);
            if (ncState == null) {
                // bad node id, reply with null immediately
                callback.setValue(null);
            } else {
                ccs.addThreadDumpRun(run.getRequestId(), run);
                ncState.getNodeController().takeThreadDump(run.getRequestId());
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
