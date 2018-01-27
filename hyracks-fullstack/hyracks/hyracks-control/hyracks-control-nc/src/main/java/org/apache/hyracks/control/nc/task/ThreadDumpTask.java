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
package org.apache.hyracks.control.nc.task;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.util.ThreadDumpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadDumpTask implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger();
    private final NodeControllerService ncs;
    private final String requestId;
    private final CcId ccId;

    public ThreadDumpTask(NodeControllerService ncs, String requestId, CcId ccId) {
        this.ncs = ncs;
        this.requestId = requestId;
        this.ccId = ccId;
    }

    @Override
    public void run() {
        String result;
        try {
            result = ThreadDumpUtil.takeDumpJSONString();
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Exception taking thread dump", e);
            result = null;
        }
        try {
            ncs.getClusterController(ccId).notifyThreadDump(ncs.getContext().getNodeId(), requestId, result);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Exception sending thread dump to CC", e);
        }
    }
}
