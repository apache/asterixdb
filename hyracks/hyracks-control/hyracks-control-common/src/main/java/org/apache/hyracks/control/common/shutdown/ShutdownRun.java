/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.control.common.shutdown;

import java.util.Set;
import java.util.TreeSet;

public class ShutdownRun implements IShutdownStatusConditionVariable{

    private final Set<String> shutdownNodeIds = new TreeSet<String>();
    private boolean shutdownSuccess = false;
    private static final int SHUTDOWN_TIMER_MS = 10000; //10 seconds

    public ShutdownRun(Set<String> nodeIds) {
        shutdownNodeIds.addAll(nodeIds);
    }

    /**
     * Notify that a node is shutting down.
     *
     * @param nodeId
     * @param status
     */
    public synchronized void notifyShutdown(String nodeId) {
        shutdownNodeIds.remove(nodeId);
        if (shutdownNodeIds.size() == 0) {
            shutdownSuccess = true;
            notifyAll();
        }
    }

    @Override
    public synchronized boolean waitForCompletion() throws Exception {
        /*
         * Either be woken up when we're done, or default to fail.
         */
        wait(SHUTDOWN_TIMER_MS);
        return shutdownSuccess;
    }
    
    public synchronized Set<String> getRemainingNodes(){
        return shutdownNodeIds;
    }

}
