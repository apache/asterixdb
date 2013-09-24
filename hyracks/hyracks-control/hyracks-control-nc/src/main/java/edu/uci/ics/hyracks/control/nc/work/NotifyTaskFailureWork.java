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

import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.Task;

public class NotifyTaskFailureWork extends AbstractWork {
    private final NodeControllerService ncs;
    private final Task task;

    private final List<Exception> exceptions;

    public NotifyTaskFailureWork(NodeControllerService ncs, Task task, List<Exception> exceptions) {
        this.ncs = ncs;
        this.task = task;
        this.exceptions = exceptions;
    }

    @Override
    public void run() {
        try {
            JobId jobId = task.getJobletContext().getJobId();
            IDatasetPartitionManager dpm = ncs.getDatasetPartitionManager();
            if (dpm != null) {
                dpm.abortReader(jobId);
            }
            ncs.getClusterController().notifyTaskFailure(jobId, task.getTaskAttemptId(), ncs.getId(), exceptions);
            //exceptions.get(0).printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        task.getJoblet().removeTask(task);
    }
}