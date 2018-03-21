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

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings({ "squid:S1181", "squid:S1166" })
public class EnsureAllCcTasksCompleted implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(2);
    private final NodeControllerService ncs;
    private final CcId ccId;
    private final Deque<Task> abortedTasks;
    private final Span span;

    public EnsureAllCcTasksCompleted(NodeControllerService ncs, CcId ccId, Deque<Task> abortedTasks) {
        this.ncs = ncs;
        this.ccId = ccId;
        this.abortedTasks = abortedTasks;
        span = Span.start(2, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        try {
            LOGGER.log(Level.INFO, "Ensuring all tasks of {} have completed", ccId);
            while (!span.elapsed()) {
                removeAborted();
                if (abortedTasks.isEmpty()) {
                    break;
                }
                LOGGER.log(Level.INFO, "{} tasks are still running", abortedTasks.size());
                Thread.sleep(TimeUnit.SECONDS.toMillis(1)); // Check once a second
            }
            if (abortedTasks.isEmpty()) {
                LOGGER.log(Level.INFO, "All tasks of {} have completed, Completing registration", ccId);
                // all tasks has completed
                ncs.getApplication().onRegisterNode(ccId);
            } else {
                LOGGER.log(Level.ERROR,
                        "Failed to abort all previous tasks associated with CC {} after {}ms. Giving up", ccId,
                        TIMEOUT);
                LOGGER.log(Level.ERROR, "{} tasks failed to complete within timeout", abortedTasks.size());
                abortedTasks.forEach(task -> {
                    List<Thread> pendingThreads = task.getPendingThreads();
                    LOGGER.log(Level.ERROR, "task {} was stuck. Stuck thread count = {}", task.getTaskAttemptId(),
                            pendingThreads.size());
                    pendingThreads.forEach(thread -> {
                        LOGGER.log(Level.ERROR, "Stuck thread trace: {}", Arrays.toString(thread.getStackTrace()));
                    });
                });
                ExitUtil.halt(ExitUtil.NC_FAILED_TO_ABORT_ALL_PREVIOUS_TASKS);
            }
        } catch (Throwable th) {
            try {
                LOGGER.log(Level.ERROR, "Failed to abort all previous tasks associated with CC {}", ccId, th);
            } catch (Throwable ignore) {
                // Ignore logging errors
            }
            ExitUtil.halt(ExitUtil.NC_FAILED_TO_ABORT_ALL_PREVIOUS_TASKS);
        }
    }

    private void removeAborted() {
        int numTasks = abortedTasks.size();
        for (int i = 0; i < numTasks; i++) {
            Task task = abortedTasks.poll();
            if (!task.isCompleted()) {
                abortedTasks.add(task);
            }
        }
    }
}
