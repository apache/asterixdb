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
package edu.uci.ics.hyracks.control.cc.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedReceiveSideMaterializedBlockingConnectorPolicy;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.util.Pair;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.ActivityClusterPlan;
import edu.uci.ics.hyracks.control.cc.job.ActivityPlan;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.Task;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterId;

public class ActivityClusterPlanner {
    private static final Logger LOGGER = Logger.getLogger(ActivityClusterPlanner.class.getName());

    private final JobScheduler scheduler;

    private final Map<PartitionId, TaskCluster> partitionProducingTaskClusterMap;

    public ActivityClusterPlanner(JobScheduler newJobScheduler) {
        this.scheduler = newJobScheduler;
        partitionProducingTaskClusterMap = new HashMap<PartitionId, TaskCluster>();
    }

    public void planActivityCluster(ActivityCluster ac) throws HyracksException {
        JobRun jobRun = scheduler.getJobRun();
        Map<ActivityId, ActivityPartitionDetails> pcMap = computePartitionCounts(ac);

        Map<ActivityId, ActivityPlan> activityPlanMap = new HashMap<ActivityId, ActivityPlan>();
        Set<ActivityId> activities = ac.getActivities();

        Map<TaskId, Set<TaskId>> taskClusterMap = new HashMap<TaskId, Set<TaskId>>();

        Set<ActivityId> depAnIds = new HashSet<ActivityId>();
        for (ActivityId anId : activities) {
            depAnIds.clear();
            getDependencyActivityIds(depAnIds, anId);
            ActivityPartitionDetails apd = pcMap.get(anId);
            Task[] tasks = new Task[apd.getPartitionCount()];
            ActivityPlan activityPlan = new ActivityPlan(apd);
            for (int i = 0; i < tasks.length; ++i) {
                TaskId tid = new TaskId(anId, i);
                tasks[i] = new Task(tid, activityPlan);
                for (ActivityId danId : depAnIds) {
                    ActivityCluster dAC = jobRun.getActivityClusterMap().get(danId);
                    ActivityClusterPlan dACP = dAC.getPlan();
                    assert dACP != null : "IllegalStateEncountered: Dependent AC is being planned without a plan for dependency AC: Encountered no plan for ActivityID "
                            + danId;
                    Task[] dATasks = dACP.getActivityPlanMap().get(danId).getTasks();
                    assert dATasks != null : "IllegalStateEncountered: Dependent AC is being planned without a plan for dependency AC: Encountered no plan for ActivityID "
                            + danId;
                    assert dATasks.length == tasks.length : "Dependency activity partitioned differently from dependent: "
                            + dATasks.length + " != " + tasks.length;
                    Task dTask = dATasks[i];
                    TaskId dTaskId = dTask.getTaskId();
                    tasks[i].getDependencies().add(dTaskId);
                    dTask.getDependents().add(tid);
                }
                Set<TaskId> cluster = new HashSet<TaskId>();
                cluster.add(tid);
                taskClusterMap.put(tid, cluster);
            }
            activityPlan.setTasks(tasks);
            activityPlanMap.put(anId, activityPlan);
        }

        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = assignConnectorPolicy(ac, activityPlanMap);
        scheduler.getJobRun().getConnectorPolicyMap().putAll(connectorPolicies);

        Map<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> taskConnectivity = new HashMap<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>>();
        JobActivityGraph jag = jobRun.getJobActivityGraph();
        BitSet targetBitmap = new BitSet();
        for (ActivityId ac1 : activities) {
            Task[] ac1TaskStates = activityPlanMap.get(ac1).getTasks();
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = jag.getActivityOutputConnectorDescriptors(ac1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    ActivityId ac2 = jag.getConsumerActivity(cdId);
                    Task[] ac2TaskStates = activityPlanMap.get(ac2).getTasks();
                    int nConsumers = ac2TaskStates.length;
                    for (int i = 0; i < nProducers; ++i) {
                        c.indicateTargetPartitions(nProducers, nConsumers, i, targetBitmap);
                        List<Pair<TaskId, ConnectorDescriptorId>> cInfoList = taskConnectivity.get(ac1TaskStates[i]
                                .getTaskId());
                        if (cInfoList == null) {
                            cInfoList = new ArrayList<Pair<TaskId, ConnectorDescriptorId>>();
                            taskConnectivity.put(ac1TaskStates[i].getTaskId(), cInfoList);
                        }
                        Set<TaskId> cluster = taskClusterMap.get(ac1TaskStates[i].getTaskId());
                        for (int j = targetBitmap.nextSetBit(0); j >= 0; j = targetBitmap.nextSetBit(j + 1)) {
                            cInfoList.add(new Pair<TaskId, ConnectorDescriptorId>(ac2TaskStates[j].getTaskId(), cdId));
                            IConnectorPolicy cPolicy = connectorPolicies.get(cdId);
                            if (cPolicy.requiresProducerConsumerCoscheduling()) {
                                cluster.add(ac2TaskStates[j].getTaskId());
                            }
                        }
                    }
                }
            }
        }

        boolean done = false;
        while (!done) {
            done = true;
            Set<TaskId> set = new HashSet<TaskId>();
            Set<TaskId> oldSet = null;
            for (Map.Entry<TaskId, Set<TaskId>> e : taskClusterMap.entrySet()) {
                set.clear();
                oldSet = e.getValue();
                set.addAll(e.getValue());
                for (TaskId tid : e.getValue()) {
                    set.addAll(taskClusterMap.get(tid));
                }
                for (TaskId tid : set) {
                    Set<TaskId> targetSet = taskClusterMap.get(tid);
                    if (!targetSet.equals(set)) {
                        done = false;
                        break;
                    }
                }
                if (!done) {
                    break;
                }
            }
            for (TaskId tid : oldSet) {
                taskClusterMap.put(tid, set);
            }
        }

        Set<Set<TaskId>> clusters = new HashSet<Set<TaskId>>(taskClusterMap.values());
        Set<TaskCluster> tcSet = new HashSet<TaskCluster>();
        int counter = 0;
        for (Set<TaskId> cluster : clusters) {
            Set<Task> taskStates = new HashSet<Task>();
            for (TaskId tid : cluster) {
                taskStates.add(activityPlanMap.get(tid.getActivityId()).getTasks()[tid.getPartition()]);
            }
            TaskCluster tc = new TaskCluster(new TaskClusterId(ac.getActivityClusterId(), counter++), ac,
                    taskStates.toArray(new Task[taskStates.size()]));
            tcSet.add(tc);
            for (TaskId tid : cluster) {
                activityPlanMap.get(tid.getActivityId()).getTasks()[tid.getPartition()].setTaskCluster(tc);
            }
        }
        TaskCluster[] taskClusters = tcSet.toArray(new TaskCluster[tcSet.size()]);

        for (TaskCluster tc : taskClusters) {
            Set<TaskCluster> tcDependencyTaskClusters = tc.getDependencyTaskClusters();
            for (Task ts : tc.getTasks()) {
                TaskId tid = ts.getTaskId();
                List<Pair<TaskId, ConnectorDescriptorId>> cInfoList = taskConnectivity.get(tid);
                if (cInfoList != null) {
                    for (Pair<TaskId, ConnectorDescriptorId> p : cInfoList) {
                        Task targetTS = activityPlanMap.get(p.first.getActivityId()).getTasks()[p.first.getPartition()];
                        TaskCluster targetTC = targetTS.getTaskCluster();
                        if (targetTC != tc) {
                            ConnectorDescriptorId cdId = p.second;
                            PartitionId pid = new PartitionId(jobRun.getJobId(), cdId, tid.getPartition(),
                                    p.first.getPartition());
                            tc.getProducedPartitions().add(pid);
                            targetTC.getRequiredPartitions().add(pid);
                            partitionProducingTaskClusterMap.put(pid, tc);
                        }
                    }
                }

                for (TaskId dTid : ts.getDependencies()) {
                    TaskCluster dTC = getTaskCluster(dTid);
                    dTC.getDependentTaskClusters().add(tc);
                    tcDependencyTaskClusters.add(dTC);
                }
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Plan for " + ac);
            LOGGER.info("Built " + tcSet.size() + " Task Clusters");
            for (TaskCluster tc : tcSet) {
                LOGGER.info("Tasks: " + Arrays.toString(tc.getTasks()));
            }
        }

        ac.setPlan(new ActivityClusterPlan(taskClusters, activityPlanMap));
    }

    private TaskCluster getTaskCluster(TaskId tid) {
        ActivityCluster ac = scheduler.getJobRun().getActivityClusterMap().get(tid.getActivityId());
        ActivityClusterPlan acp = ac.getPlan();
        Task[] tasks = acp.getActivityPlanMap().get(tid.getActivityId()).getTasks();
        Task task = tasks[tid.getPartition()];
        assert task.getTaskId().equals(tid);
        return task.getTaskCluster();
    }

    private void getDependencyActivityIds(Set<ActivityId> depAnIds, ActivityId anId) {
        JobActivityGraph jag = scheduler.getJobRun().getJobActivityGraph();
        Set<ActivityId> blockers = jag.getBlocked2BlockerMap().get(anId);
        if (blockers != null) {
            depAnIds.addAll(blockers);
        }
    }

    private Map<ConnectorDescriptorId, IConnectorPolicy> assignConnectorPolicy(ActivityCluster ac,
            Map<ActivityId, ActivityPlan> taskMap) {
        JobActivityGraph jag = scheduler.getJobRun().getJobActivityGraph();
        Map<ConnectorDescriptorId, IConnectorPolicy> cPolicyMap = new HashMap<ConnectorDescriptorId, IConnectorPolicy>();
        Set<ActivityId> activities = ac.getActivities();
        BitSet targetBitmap = new BitSet();
        for (ActivityId a1 : activities) {
            Task[] ac1TaskStates = taskMap.get(a1).getTasks();
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = jag.getActivityOutputConnectorDescriptors(a1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    ActivityId a2 = jag.getConsumerActivity(cdId);
                    Task[] ac2TaskStates = taskMap.get(a2).getTasks();
                    int nConsumers = ac2TaskStates.length;

                    int[] fanouts = new int[nProducers];
                    for (int i = 0; i < nProducers; ++i) {
                        c.indicateTargetPartitions(nProducers, nConsumers, i, targetBitmap);
                        fanouts[i] = targetBitmap.cardinality();
                    }
                    IConnectorPolicy cp = assignConnectorPolicy(c, nProducers, nConsumers, fanouts);
                    cPolicyMap.put(cdId, cp);
                }
            }
        }
        return cPolicyMap;
    }

    private IConnectorPolicy assignConnectorPolicy(IConnectorDescriptor c, int nProducers, int nConsumers, int[] fanouts) {
        IConnectorPolicyAssignmentPolicy cpap = scheduler.getJobRun().getJobActivityGraph().getJobSpecification()
                .getConnectorPolicyAssignmentPolicy();
        if (cpap != null) {
            return cpap.getConnectorPolicyAssignment(c, nProducers, nConsumers, fanouts);
        }
        return new SendSideMaterializedReceiveSideMaterializedBlockingConnectorPolicy();
    }

    private Map<ActivityId, ActivityPartitionDetails> computePartitionCounts(ActivityCluster ac)
            throws HyracksException {
        PartitionConstraintSolver solver = scheduler.getSolver();
        JobRun jobRun = scheduler.getJobRun();
        Set<LValueConstraintExpression> lValues = new HashSet<LValueConstraintExpression>();
        for (ActivityId anId : ac.getActivities()) {
            lValues.add(new PartitionCountExpression(anId.getOperatorDescriptorId()));
        }
        solver.solve(lValues);
        Map<OperatorDescriptorId, Integer> nPartMap = new HashMap<OperatorDescriptorId, Integer>();
        for (LValueConstraintExpression lv : lValues) {
            Object value = solver.getValue(lv);
            if (value == null) {
                throw new HyracksException("No value found for " + lv);
            }
            if (!(value instanceof Number)) {
                throw new HyracksException("Unexpected type of value bound to " + lv + ": " + value.getClass() + "("
                        + value + ")");
            }
            int nParts = ((Number) value).intValue();
            if (nParts <= 0) {
                throw new HyracksException("Unsatisfiable number of partitions for " + lv + ": " + nParts);
            }
            nPartMap.put(((PartitionCountExpression) lv).getOperatorDescriptorId(), Integer.valueOf(nParts));
        }
        Map<ActivityId, ActivityPartitionDetails> activityPartsMap = new HashMap<ActivityId, ActivityPartitionDetails>();
        for (ActivityId anId : ac.getActivities()) {
            int nParts = nPartMap.get(anId.getOperatorDescriptorId());
            int[] nInputPartitions = null;
            List<IConnectorDescriptor> inputs = jobRun.getJobActivityGraph().getActivityInputConnectorDescriptors(anId);
            if (inputs != null) {
                nInputPartitions = new int[inputs.size()];
                for (int i = 0; i < nInputPartitions.length; ++i) {
                    nInputPartitions[i] = nPartMap.get(jobRun.getJobActivityGraph()
                            .getProducerActivity(inputs.get(i).getConnectorId()).getOperatorDescriptorId());
                }
            }
            int[] nOutputPartitions = null;
            List<IConnectorDescriptor> outputs = jobRun.getJobActivityGraph().getActivityOutputConnectorDescriptors(
                    anId);
            if (outputs != null) {
                nOutputPartitions = new int[outputs.size()];
                for (int i = 0; i < nOutputPartitions.length; ++i) {
                    nOutputPartitions[i] = nPartMap.get(jobRun.getJobActivityGraph()
                            .getConsumerActivity(outputs.get(i).getConnectorId()).getOperatorDescriptorId());
                }
            }
            ActivityPartitionDetails apd = new ActivityPartitionDetails(nParts, nInputPartitions, nOutputPartitions);
            activityPartsMap.put(anId, apd);
        }
        return activityPartsMap;
    }

    public Map<? extends PartitionId, ? extends TaskCluster> getPartitionProducingTaskClusterMap() {
        return partitionProducingTaskClusterMap;
    }
}