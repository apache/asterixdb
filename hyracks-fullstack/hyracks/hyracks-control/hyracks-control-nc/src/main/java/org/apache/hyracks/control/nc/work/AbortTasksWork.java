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

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AbortTasksWork extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final NodeControllerService ncs;

    private final JobId jobId;

    private final List<TaskAttemptId> tasks;

    public AbortTasksWork(NodeControllerService ncs, JobId jobId, List<TaskAttemptId> tasks) {
        this.ncs = ncs;
        this.jobId = jobId;
        this.tasks = tasks;
    }

    @Override
    public void run() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Aborting Tasks: " + jobId + ":" + tasks);
        }
        IResultPartitionManager resultPartitionManager = ncs.getResultPartitionManager();
        if (resultPartitionManager != null) {
            ncs.getResultPartitionManager().abortReader(jobId);
        }
        Joblet ji = ncs.getJobletMap().get(jobId);
        if (ji != null) {
            Map<TaskAttemptId, Task> taskMap = ji.getTaskMap();
            for (TaskAttemptId taId : tasks) {
                Task task = taskMap.get(taId);
                if (task != null) {
                    task.abort();
                }
            }
        } else {
            LOGGER.log(Level.WARN,
                    "Joblet couldn't be found. Tasks of job " + jobId + " have all either completed or failed");
        }
    }
}
