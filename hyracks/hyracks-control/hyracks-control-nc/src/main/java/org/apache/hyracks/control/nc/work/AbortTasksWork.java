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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;

public class AbortTasksWork extends AbstractWork {
    private static final Logger LOGGER = Logger.getLogger(AbortTasksWork.class.getName());

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
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Aborting Tasks: " + jobId + ":" + tasks);
        }
        IDatasetPartitionManager dpm = ncs.getDatasetPartitionManager();
        if (dpm != null) {
            ncs.getDatasetPartitionManager().abortReader(jobId);
        }

        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet ji = jobletMap.get(jobId);
        if (ji != null) {
            Map<TaskAttemptId, Task> taskMap = ji.getTaskMap();
            for (TaskAttemptId taId : tasks) {
                Task task = taskMap.get(taId);
                if (task != null) {
                    task.abort();
                }
            }
        }
    }
}