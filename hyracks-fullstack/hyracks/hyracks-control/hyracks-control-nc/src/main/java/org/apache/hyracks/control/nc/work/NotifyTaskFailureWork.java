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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.Task;

public class NotifyTaskFailureWork extends AbstractWork {
    private static final Logger LOGGER = Logger.getLogger(NotifyTaskFailureWork.class.getName());
    private final NodeControllerService ncs;
    private final Task task;
    private final JobId jobId;
    private final TaskAttemptId taskId;
    private final List<Exception> exceptions;

    public NotifyTaskFailureWork(NodeControllerService ncs, Task task, List<Exception> exceptions, JobId jobId,
            TaskAttemptId taskId) {
        this.ncs = ncs;
        this.task = task;
        this.exceptions = exceptions;
        this.jobId = jobId;
        this.taskId = taskId;
    }

    @Override
    public void run() {
        LOGGER.log(Level.WARNING, ncs.getId() + " is sending a notification to cc that task " + taskId + " has failed",
                exceptions.get(0));
        try {
            IDatasetPartitionManager dpm = ncs.getDatasetPartitionManager();
            if (dpm != null) {
                dpm.abortReader(jobId);
            }
            ncs.getClusterController().notifyTaskFailure(jobId, taskId, ncs.getId(), exceptions);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure reporting task failure to cluster controller", e);
        }
        if (task != null) {
            task.getJoblet().removeTask(task);
        }
    }
}
