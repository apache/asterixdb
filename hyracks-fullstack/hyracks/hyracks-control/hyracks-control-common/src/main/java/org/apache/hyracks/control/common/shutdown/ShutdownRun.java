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

import org.apache.hyracks.util.Span;

public class ShutdownRun implements IShutdownStatusConditionVariable {

    private final Set<String> shutdownNodeIds = new TreeSet<>();
    private boolean ccStopComplete = false;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;

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
            notifyAll();
        }
    }

    @Override
    public synchronized boolean waitForCompletion() throws Exception {
        Span span = Span.start(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        while (!span.elapsed()) {
            if (shutdownNodeIds.isEmpty()) {
                return true;
            }
            /*
             * Either be woken up when we're done, or after (remaining) timeout has elapsed
             */
            span.wait(this);
        }
        return false;
    }

    public synchronized Set<String> getRemainingNodes() {
        return shutdownNodeIds;
    }

    public synchronized void notifyCcStopComplete() {
        ccStopComplete = true;
        notifyAll();
    }

    public synchronized boolean waitForCcStopCompletion() throws Exception {
        while (!ccStopComplete) {
            wait();
        }
        return true;
    }
}
