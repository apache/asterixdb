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
package edu.uci.ics.hyracks.control.nc.work;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.Task;

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