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
package edu.uci.ics.hyracks.control.cc.work;

import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;

public class TaskFailureWork extends AbstractTaskLifecycleWork {
    private final List<Exception> exceptions;

    public TaskFailureWork(ClusterControllerService ccs, JobId jobId, TaskAttemptId taId, String nodeId,
            List<Exception> exceptions) {
        super(ccs, jobId, taId, nodeId);
        this.exceptions = exceptions;
    }

    @Override
    protected void performEvent(TaskAttempt ta) {
        JobRun run = ccs.getActiveRunMap().get(jobId);
        ccs.getDatasetDirectoryService().reportJobFailure(jobId, exceptions);
        ActivityCluster ac = ta.getTask().getTaskCluster().getActivityCluster();
        run.getScheduler().notifyTaskFailure(ta, ac, exceptions);
    }

    @Override
    public String toString() {
        return "TaskFailureEvent[" + jobId + ":" + taId + ":" + nodeId + "]";
    }
}