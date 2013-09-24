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
import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.ActivityPlan;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.Task;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterAttempt;

public abstract class AbstractTaskLifecycleWork extends AbstractHeartbeatWork {
    protected final ClusterControllerService ccs;
    protected final JobId jobId;
    protected final TaskAttemptId taId;
    protected final String nodeId;

    public AbstractTaskLifecycleWork(ClusterControllerService ccs, JobId jobId, TaskAttemptId taId, String nodeId) {
        super(ccs, nodeId, null);
        this.ccs = ccs;
        this.jobId = jobId;
        this.taId = taId;
        this.nodeId = nodeId;
    }

    @Override
    public final void runWork() {
        JobRun run = ccs.getActiveRunMap().get(jobId);
        if (run != null) {
            TaskId tid = taId.getTaskId();
            Map<ActivityId, ActivityCluster> activityClusterMap = run.getActivityClusterGraph().getActivityMap();
            ActivityCluster ac = activityClusterMap.get(tid.getActivityId());
            if (ac != null) {
                Map<ActivityId, ActivityPlan> taskStateMap = run.getActivityClusterPlanMap().get(ac.getId())
                        .getActivityPlanMap();
                Task[] taskStates = taskStateMap.get(tid.getActivityId()).getTasks();
                if (taskStates != null && taskStates.length > tid.getPartition()) {
                    Task ts = taskStates[tid.getPartition()];
                    TaskCluster tc = ts.getTaskCluster();
                    List<TaskClusterAttempt> taskClusterAttempts = tc.getAttempts();
                    if (taskClusterAttempts != null && taskClusterAttempts.size() > taId.getAttempt()) {
                        TaskClusterAttempt tca = taskClusterAttempts.get(taId.getAttempt());
                        TaskAttempt ta = tca.getTaskAttempts().get(tid);
                        if (ta != null) {
                            performEvent(ta);
                        }
                    }
                }
            }
        }
    }

    protected abstract void performEvent(TaskAttempt ta);
}