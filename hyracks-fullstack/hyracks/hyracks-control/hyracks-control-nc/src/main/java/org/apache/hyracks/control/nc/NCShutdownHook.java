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
package org.apache.hyracks.control.nc;

import org.apache.hyracks.util.ThreadDumpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shutdown hook that invokes {@link NodeControllerService#stop() stop} method.
 * This shutdown hook must have a failsafe mechanism to halt the process in case the shutdown
 * operation is hanging for any reason
 */
public class NCShutdownHook extends Thread {

    private static final Logger LOGGER = LogManager.getLogger();
    private final NodeControllerService nodeControllerService;

    NCShutdownHook(NodeControllerService nodeControllerService) {
        super("ShutdownHook-" + nodeControllerService.getId());
        this.nodeControllerService = nodeControllerService;
    }

    @Override
    public void run() {
        try {
            try {
                LOGGER.info("Shutdown hook called");
            } catch (Throwable th) {//NOSONAR
            }
            LOGGER.log(Level.DEBUG, () -> "Thread dump at shutdown: " + ThreadDumpUtil.takeDumpString());
            nodeControllerService.stop();
        } catch (Throwable th) { // NOSONAR... This is fine since this is shutdown hook
            LOGGER.log(Level.WARN, "Exception in executing shutdown hook", th);
        }
    }
}