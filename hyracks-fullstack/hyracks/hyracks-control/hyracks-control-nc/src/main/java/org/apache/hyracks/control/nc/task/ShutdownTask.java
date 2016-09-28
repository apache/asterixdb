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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.nc.NodeControllerService;

public class ShutdownTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ShutdownTask.class.getName());
    private final NodeControllerService ncs;
    private final boolean terminateNCService;

    public ShutdownTask(NodeControllerService ncs, boolean terminateNCService) {
        this.ncs = ncs;
        this.terminateNCService = terminateNCService;
    }

    @Override
    public void run() {
        IClusterController ccs = ncs.getClusterController();
        try {
            ccs.notifyShutdown(ncs.getId());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception notifying CC of shutdown acknowledgment", e);
            // proceed with shutdown
        }

        LOGGER.info("JVM Exiting.. Bye!");
        //run the shutdown in a new thread, so we don't block this last work task
        Thread t = new Thread("NC " + ncs.getId() + " Shutdown") {
            @Override
            public void run() {
                try {
                    ncs.stop();
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Exception stopping node controller service", e);
                } finally {
                    Runtime rt = Runtime.getRuntime();
                    rt.exit(terminateNCService ? 99 : 0);
                }
            }
        };
        t.start();
    }

}
