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

package org.apache.hyracks.control.common.shutdown;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class ShutdownRun implements IShutdownStatusConditionVariable {

    private final Set<String> shutdownNodeIds = new TreeSet<>();
    private boolean shutdownSuccess = false;
    private static final long SHUTDOWN_TIMER_MS = TimeUnit.SECONDS.toMillis(30);

    public ShutdownRun(Collection<String> nodeIds) {
        shutdownNodeIds.addAll(nodeIds);
    }

    /**
     * Notify that a node is shutting down.
     *
     * @param nodeId the node acknowledging the shutdown
     */
    public synchronized void notifyShutdown(String nodeId) {
        shutdownNodeIds.remove(nodeId);
        if (shutdownNodeIds.isEmpty()) {
            shutdownSuccess = true;
            notifyAll();
        }
    }

    @Override
    public synchronized boolean waitForCompletion() throws Exception {
        if (shutdownNodeIds.isEmpty()) {
            shutdownSuccess = true;
        } else {
            /*
             * Either be woken up when we're done, or default to fail.
             */
            wait(SHUTDOWN_TIMER_MS);
        }
        return shutdownSuccess;
    }

    public synchronized Set<String> getRemainingNodes() {
        return shutdownNodeIds;
    }

}
