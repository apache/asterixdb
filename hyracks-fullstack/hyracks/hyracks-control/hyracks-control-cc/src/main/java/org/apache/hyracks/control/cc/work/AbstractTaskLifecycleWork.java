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
package org.apache.hyracks.control.cc.work;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.ActivityPlan;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.job.Task;
import org.apache.hyracks.control.cc.job.TaskAttempt;
import org.apache.hyracks.control.cc.job.TaskCluster;
import org.apache.hyracks.control.cc.job.TaskClusterAttempt;

public abstract class AbstractTaskLifecycleWork extends AbstractHeartbeatWork {
    protected final JobId jobId;
    protected final TaskAttemptId taId;

    public AbstractTaskLifecycleWork(ClusterControllerService ccs, JobId jobId, TaskAttemptId taId, String nodeId) {
        super(ccs, nodeId, null);
        this.jobId = jobId;
        this.taId = taId;
    }

    @Override
    public final void runWork() {
        IJobManager jobManager = ccs.getJobManager();
        JobRun run = jobManager.get(jobId);
        if (run != null) {
            TaskId tid = taId.getTaskId();
            Map<ActivityId, ActivityCluster> activityClusterMap = run.getActivityClusterGraph().getActivityMap();
            ActivityCluster ac = activityClusterMap.get(tid.getActivityId());
            if (ac != null) {
                Map<ActivityId, ActivityPlan> taskStateMap =
                        run.getActivityClusterPlanMap().get(ac.getId()).getActivityPlanMap();
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
