/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskState;

public class TaskFailureEvent implements Runnable {
    private final ClusterControllerService ccs;
    private final UUID jobId;
    private final TaskAttemptId taId;
    private final String nodeId;
    private final Exception exception;

    public TaskFailureEvent(ClusterControllerService ccs, UUID jobId, TaskAttemptId taId, String nodeId, Exception exception) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.taId = taId;
        this.nodeId = nodeId;
        this.exception = exception;
    }

    @Override
    public void run() {
        JobRun run = ccs.getRunMap().get(jobId);
        if (run != null) {
            TaskId tid = taId.getTaskId();
            Map<ActivityNodeId, ActivityCluster> activityClusterMap = run.getActivityClusterMap();
            ActivityCluster ac = activityClusterMap.get(tid.getActivityId());
            if (ac != null) {
                Map<ActivityNodeId, TaskState[]> taskStateMap = ac.getTaskStateMap();
                TaskState[] taskStates = taskStateMap.get(tid.getActivityId());
                if (taskStates != null && taskStates.length > tid.getPartition()) {
                    TaskState ts = taskStates[tid.getPartition()];
                    TaskCluster tc = ts.getTaskCluster();
                    List<TaskClusterAttempt> taskClusterAttempts = tc.getAttempts();
                    if (taskClusterAttempts != null && taskClusterAttempts.size() > taId.getAttempt()) {
                        TaskClusterAttempt tca = taskClusterAttempts.get(taId.getAttempt());
                        TaskAttempt ta = tca.getTaskAttempts()[tid.getPartition()];
                        try {
                            ta.notifyTaskFailure(exception);
                        } catch (HyracksException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "TaskCompleteEvent[" + jobId + ":" + taId + ":" + nodeId + "]";
    }
}