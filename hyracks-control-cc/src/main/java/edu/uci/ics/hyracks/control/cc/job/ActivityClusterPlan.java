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
package edu.uci.ics.hyracks.control.cc.job;

import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class ActivityClusterPlan {
    private final Map<ActivityId, Task[]> taskStateMap;

    private final Map<PartitionId, TaskCluster> partitionProducingTaskClusterMap;

    private final TaskCluster[] taskClusters;

    public ActivityClusterPlan(TaskCluster[] taskClusters, Map<ActivityId, Task[]> taskStateMap,
            Map<PartitionId, TaskCluster> partitionProducingTaskClusterMap) {
        this.taskStateMap = taskStateMap;
        this.partitionProducingTaskClusterMap = partitionProducingTaskClusterMap;
        this.taskClusters = taskClusters;
    }

    public Map<ActivityId, Task[]> getTaskMap() {
        return taskStateMap;
    }

    public TaskCluster[] getTaskClusters() {
        return taskClusters;
    }

    public Map<PartitionId, TaskCluster> getPartitionProducingTaskClusterMap() {
        return partitionProducingTaskClusterMap;
    }
}