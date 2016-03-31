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

package org.apache.hyracks.control.nc.work;

import java.net.URL;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.deployment.DeploymentStatus;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;

public class ShutdownWork extends AbstractWork {

    private final NodeControllerService ncs;
    private static Logger LOGGER = Logger.getLogger(ShutdownWork.class.getName());

    public ShutdownWork(NodeControllerService ncs) {
        this.ncs = ncs;
    }

    @Override
    public void run() {
        try {
            IClusterController ccs = ncs.getClusterController();
            ccs.notifyShutdown(ncs.getId());
            LOGGER.info("JVM Exiting.. Bye!");
            //run the shutdown in a new thread, so we don't block this last work task
            Thread t = new Thread() {
                public void run() {
                    try {
                        ncs.stop();
                    } catch (Exception e) {
                        LOGGER.severe(e.getMessage());
                    } finally {
                        Runtime rt = Runtime.getRuntime();
                        rt.exit(0);
                    }
                }
            };
            t.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
