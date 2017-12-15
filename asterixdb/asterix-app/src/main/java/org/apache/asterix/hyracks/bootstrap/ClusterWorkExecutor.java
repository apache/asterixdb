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
package org.apache.asterix.hyracks.bootstrap;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.metadata.cluster.AddNodeWork;
import org.apache.asterix.metadata.cluster.RemoveNodeWork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterWorkExecutor implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LinkedBlockingQueue<Set<IClusterManagementWork>> inbox;

    public ClusterWorkExecutor(LinkedBlockingQueue<Set<IClusterManagementWork>> inbox) {
        this.inbox = inbox;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Set<IClusterManagementWork> workSet = inbox.take();
                int nodesToAdd = 0;
                Set<String> nodesToRemove = new HashSet<>();
                Set<IClusterManagementWork> nodeAdditionRequests = new HashSet<>();
                Set<IClusterManagementWork> nodeRemovalRequests = new HashSet<>();
                for (IClusterManagementWork w : workSet) {
                    switch (w.getClusterManagementWorkType()) {
                        case ADD_NODE:
                            if (nodesToAdd < ((AddNodeWork) w).getNumberOfNodesRequested()) {
                                nodesToAdd = ((AddNodeWork) w).getNumberOfNodesRequested();
                            }
                            nodeAdditionRequests.add(w);
                            break;
                        case REMOVE_NODE:
                            nodesToRemove.addAll(((RemoveNodeWork) w).getNodesToBeRemoved());
                            nodeRemovalRequests.add(w);
                            break;
                    }
                }

            } catch (InterruptedException e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("interruped" + e.getMessage());
                }
                throw new IllegalStateException(e);
            } catch (Exception e) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Unexpected exception in handling cluster event" + e.getMessage());
                }
            }

        }
    }

}
