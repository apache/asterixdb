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
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NotifyThreadDumpResponse extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;

    private final String requestId;
    private final String threadDumpJSON;

    public NotifyThreadDumpResponse(ClusterControllerService ccs, String requestId, String threadDumpJSON) {
        this.ccs = ccs;
        this.requestId = requestId;
        this.threadDumpJSON = threadDumpJSON;
    }

    @Override
    public void run() {
        LOGGER.debug("Delivering thread dump response: " + requestId);
        final GetThreadDumpWork.ThreadDumpRun threadDumpRun = ccs.removeThreadDumpRun(requestId);
        if (threadDumpRun == null) {
            LOGGER.warn("Thread dump run " + requestId + " not found; discarding reply: " + threadDumpJSON);
        } else {
            threadDumpRun.notifyThreadDumpReceived(threadDumpJSON);
        }
    }
}
