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

import java.util.Deque;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("squid:S1181")
public class EnsureAllCcTasksCompleted implements Runnable {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(2);
    private final NodeControllerService ncs;
    private final CcId ccId;
    private final Deque<Task> runningTasks;

    EnsureAllCcTasksCompleted(NodeControllerService ncs, CcId ccId, Deque<Task> runningTasks) {
        this.ncs = ncs;
        this.ccId = ccId;
        this.runningTasks = runningTasks;
    }

    @Override
    public void run() {
        LOGGER.info("Ensuring all tasks of CC {} have completed", ccId);
        try {
            waitForTaskCompletion();
        } catch (InterruptedException e) {
            LOGGER.info("interrupted waiting for CC tasks to complete; giving up");
            Thread.currentThread().interrupt();
        }
    }

    private void waitForTaskCompletion() throws InterruptedException {
        final Span maxWaitTime = Span.start(TIMEOUT, TimeUnit.MILLISECONDS);
        while (!maxWaitTime.elapsed()) {
            removeCompleted();
            if (runningTasks.isEmpty()) {
                break;
            }
            LOGGER.info("{} tasks are still running", runningTasks.size());
            TimeUnit.SECONDS.sleep(1); // Check once a second
        }
        removeCompleted();
        if (runningTasks.isEmpty()) {
            LOGGER.info("all tasks of CC {} have completed", ccId);
            try {
                ncs.notifyTasksCompleted(ccId);
            } catch (InterruptedException e) {
                LOGGER.info("interrupted during notifyTasksCompleted");
                throw e;
            } catch (Exception e) {
                LOGGER.error("unexpected error during notifyTasksCompleted", e);
                ExitUtil.halt(ExitUtil.EC_NC_FAILED_TO_NOTIFY_TASKS_COMPLETED);
            }
        } else {
            LOGGER.error("{} tasks associated with CC {} failed to complete after {}ms. Giving up", runningTasks.size(),
                    ccId, TIMEOUT);
            logPendingTasks();
            ExitUtil.halt(ExitUtil.EC_NC_FAILED_TO_ABORT_ALL_PREVIOUS_TASKS);
        }
    }

    private void removeCompleted() {
        runningTasks.removeIf(Task::isCompleted);
    }

    private void logPendingTasks() {
        for (Task task : runningTasks) {
            final List<Thread> pendingThreads = task.getPendingThreads();
            LOGGER.error("task {} was stuck. Stuck thread count = {}", task.getTaskAttemptId(), pendingThreads.size());
            for (Thread thread : pendingThreads) {
                LOGGER.error("stuck thread trace", ExceptionUtils.fromThreadStack(thread));
            }
        }
    }
}
