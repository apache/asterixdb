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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shutdown hook that invokes {@link NodeControllerService#stop() stop} method.
 */
public class NCShutdownHook extends Thread {
    private static final Logger LOGGER = Logger.getLogger(NCShutdownHook.class.getName());
    private final NodeControllerService nodeControllerService;
    public NCShutdownHook(NodeControllerService nodeControllerService) {
        this.nodeControllerService = nodeControllerService;
    }

    @Override
    public void run() {
        LOGGER.info("Shutdown hook in progress");
        try {
            nodeControllerService.stop();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception in executing shutdown hook", e);
        }
    }
}