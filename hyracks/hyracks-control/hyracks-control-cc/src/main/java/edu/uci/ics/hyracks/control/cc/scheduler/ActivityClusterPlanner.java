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

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
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

    public ActivityClusterPlan planActivityCluster(ActivityCluster ac) throws HyracksException {
        JobRun jobRun = scheduler.getJobRun();
        Map<ActivityId, ActivityPartitionDetails> pcMap = computePartitionCounts(ac);

        Map<ActivityId, ActivityPlan> activityPlanMap = buildActivityPlanMap(ac, jobRun, pcMap);

        assignConnectorPolicy(ac, activityPlanMap);

        TaskCluster[] taskClusters = computeTaskClusters(ac, jobRun, activityPlanMap);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Plan for " + ac);
            LOGGER.info("Built " + taskClusters.length + " Task Clusters");
            for (TaskCluster tc : taskClusters) {
                LOGGER.info("Tasks: " + Arrays.toString(tc.getTasks()));
            }
        }

        return new ActivityClusterPlan(taskClusters, activityPlanMap);
    }

    private Map<ActivityId, ActivityPlan> buildActivityPlanMap(ActivityCluster ac, JobRun jobRun,
            Map<ActivityId, ActivityPartitionDetails> pcMap) {
        Map<ActivityId, ActivityPlan> activityPlanMap = new HashMap<ActivityId, ActivityPlan>();
        Set<ActivityId> depAnIds = new HashSet<ActivityId>();
        for (ActivityId anId : ac.getActivityMap().keySet()) {
            depAnIds.clear();
            getDependencyActivityIds(depAnIds, anId, ac);
            ActivityPartitionDetails apd = pcMap.get(anId);
            Task[] tasks = new Task[apd.getPartitionCount()];
            ActivityPlan activityPlan = new ActivityPlan(apd);
            for (int i = 0; i < tasks.length; ++i) {
                TaskId tid = new TaskId(anId, i);
                tasks[i] = new Task(tid, activityPlan);
                for (ActivityId danId : depAnIds) {
                    ActivityCluster dAC = ac.getActivityClusterGraph().getActivityMap().get(danId);
                    ActivityClusterPlan dACP = jobRun.getActivityClusterPlanMap().get(dAC.getId());
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
            }
            activityPlan.setTasks(tasks);
            activityPlanMap.put(anId, activityPlan);
        }
        return activityPlanMap;
    }

    private TaskCluster[] computeTaskClusters(ActivityCluster ac, JobRun jobRun,
            Map<ActivityId, ActivityPlan> activityPlanMap) {
        Set<ActivityId> activities = ac.getActivityMap().keySet();
        Map<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> taskConnectivity = computeTaskConnectivity(jobRun,
                activityPlanMap, activities);

        TaskCluster[] taskClusters = ac.getActivityClusterGraph().isUseConnectorPolicyForScheduling() ? buildConnectorPolicyAwareTaskClusters(
                ac, activityPlanMap, taskConnectivity) : buildConnectorPolicyUnawareTaskClusters(ac, activityPlanMap);

        for (TaskCluster tc : taskClusters) {
            Set<TaskCluster> tcDependencyTaskClusters = tc.getDependencyTaskClusters();
            for (Task ts : tc.getTasks()) {
                TaskId tid = ts.getTaskId();
                List<Pair<TaskId, ConnectorDescriptorId>> cInfoList = taskConnectivity.get(tid);
                if (cInfoList != null) {
                    for (Pair<TaskId, ConnectorDescriptorId> p : cInfoList) {
                        Task targetTS = activityPlanMap.get(p.getLeft().getActivityId()).getTasks()[p.getLeft()
                                .getPartition()];
                        TaskCluster targetTC = targetTS.getTaskCluster();
                        if (targetTC != tc) {
                            ConnectorDescriptorId cdId = p.getRight();
                            PartitionId pid = new PartitionId(jobRun.getJobId(), cdId, tid.getPartition(), p.getLeft()
                                    .getPartition());
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
        return taskClusters;
    }

    private TaskCluster[] buildConnectorPolicyUnawareTaskClusters(ActivityCluster ac,
            Map<ActivityId, ActivityPlan> activityPlanMap) {
        List<Task> taskStates = new ArrayList<Task>();
        for (ActivityId anId : ac.getActivityMap().keySet()) {
            ActivityPlan ap = activityPlanMap.get(anId);
            Task[] tasks = ap.getTasks();
            for (Task t : tasks) {
                taskStates.add(t);
            }
        }
        TaskCluster tc = new TaskCluster(new TaskClusterId(ac.getId(), 0), ac, taskStates.toArray(new Task[taskStates
                .size()]));
        for (Task t : tc.getTasks()) {
            t.setTaskCluster(tc);
        }
        return new TaskCluster[] { tc };
    }

    private Map<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> computeTaskConnectivity(JobRun jobRun,
            Map<ActivityId, ActivityPlan> activityPlanMap, Set<ActivityId> activities) {
        Map<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> taskConnectivity = new HashMap<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>>();
        ActivityClusterGraph acg = jobRun.getActivityClusterGraph();
        BitSet targetBitmap = new BitSet();
        for (ActivityId ac1 : activities) {
            ActivityCluster ac = acg.getActivityMap().get(ac1);
            Task[] ac1TaskStates = activityPlanMap.get(ac1).getTasks();
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = ac.getActivityOutputMap().get(ac1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    ActivityId ac2 = ac.getConsumerActivity(cdId);
                    Task[] ac2TaskStates = activityPlanMap.get(ac2).getTasks();
                    int nConsumers = ac2TaskStates.length;
                    if (c.allProducersToAllConsumers()) {
                        List<Pair<TaskId, ConnectorDescriptorId>> cInfoList = new ArrayList<Pair<TaskId, ConnectorDescriptorId>>();
                        for (int j = 0; j < nConsumers; j++) {
                            TaskId targetTID = ac2TaskStates[j].getTaskId();
                            cInfoList.add(Pair.<TaskId, ConnectorDescriptorId> of(targetTID, cdId));
                        }
                        for (int i = 0; i < nProducers; ++i) {
                            taskConnectivity.put(ac1TaskStates[i].getTaskId(), cInfoList);
                        }
                    } else {
                        for (int i = 0; i < nProducers; ++i) {
                            c.indicateTargetPartitions(nProducers, nConsumers, i, targetBitmap);
                            List<Pair<TaskId, ConnectorDescriptorId>> cInfoList = taskConnectivity.get(ac1TaskStates[i]
                                    .getTaskId());
                            if (cInfoList == null) {
                                cInfoList = new ArrayList<Pair<TaskId, ConnectorDescriptorId>>();
                                taskConnectivity.put(ac1TaskStates[i].getTaskId(), cInfoList);
                            }
                            for (int j = targetBitmap.nextSetBit(0); j >= 0; j = targetBitmap.nextSetBit(j + 1)) {
                                TaskId targetTID = ac2TaskStates[j].getTaskId();
                                cInfoList.add(Pair.<TaskId, ConnectorDescriptorId> of(targetTID, cdId));
                            }
                        }
                    }
                }
            }
        }
        return taskConnectivity;
    }

    private TaskCluster[] buildConnectorPolicyAwareTaskClusters(ActivityCluster ac,
            Map<ActivityId, ActivityPlan> activityPlanMap,
            Map<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> taskConnectivity) {
        Map<TaskId, Set<TaskId>> taskClusterMap = new HashMap<TaskId, Set<TaskId>>();
        for (ActivityId anId : ac.getActivityMap().keySet()) {
            ActivityPlan ap = activityPlanMap.get(anId);
            Task[] tasks = ap.getTasks();
            for (Task t : tasks) {
                Set<TaskId> cluster = new HashSet<TaskId>();
                TaskId tid = t.getTaskId();
                cluster.add(tid);
                taskClusterMap.put(tid, cluster);
            }
        }

        JobRun jobRun = scheduler.getJobRun();
        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = jobRun.getConnectorPolicyMap();
        for (Map.Entry<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> e : taskConnectivity.entrySet()) {
            Set<TaskId> cluster = taskClusterMap.get(e.getKey());
            for (Pair<TaskId, ConnectorDescriptorId> p : e.getValue()) {
                IConnectorPolicy cPolicy = connectorPolicies.get(p.getRight());
                if (cPolicy.requiresProducerConsumerCoscheduling()) {
                    cluster.add(p.getLeft());
                }
            }
        }

        /*
         * taskClusterMap contains for every TID x, x -> { coscheduled consumer TIDs U x }
         * We compute the transitive closure of this relation to find the largest set of
         * tasks that need to be co-scheduled
         */
        int counter = 0;
        TaskId[] ordinalList = new TaskId[taskClusterMap.size()];
        Map<TaskId, Integer> ordinalMap = new HashMap<TaskId, Integer>();
        for (TaskId tid : taskClusterMap.keySet()) {
            ordinalList[counter] = tid;
            ordinalMap.put(tid, counter);
            ++counter;
        }

        int n = ordinalList.length;
        BitSet[] paths = new BitSet[n];
        for (Map.Entry<TaskId, Set<TaskId>> e : taskClusterMap.entrySet()) {
            int i = ordinalMap.get(e.getKey());
            BitSet bsi = paths[i];
            if (bsi == null) {
                bsi = new BitSet(n);
                paths[i] = bsi;
            }
            for (TaskId ttid : e.getValue()) {
                int j = ordinalMap.get(ttid);
                paths[i].set(j);
                BitSet bsj = paths[j];
                if (bsj == null) {
                    bsj = new BitSet(n);
                    paths[j] = bsj;
                }
                bsj.set(i);
            }
        }
        for (int k = 0; k < n; ++k) {
            for (int i = paths[k].nextSetBit(0); i >= 0; i = paths[k].nextSetBit(i + 1)) {
                for (int j = paths[i].nextClearBit(0); j < n && j >= 0; j = paths[i].nextClearBit(j + 1)) {
                    paths[i].set(j, paths[k].get(j));
                    paths[j].set(i, paths[i].get(j));
                }
            }
        }
        BitSet pending = new BitSet(n);
        pending.set(0, n);
        List<List<TaskId>> clusters = new ArrayList<List<TaskId>>();
        for (int i = pending.nextSetBit(0); i >= 0; i = pending.nextSetBit(i)) {
            List<TaskId> cluster = new ArrayList<TaskId>();
            for (int j = paths[i].nextSetBit(0); j >= 0; j = paths[i].nextSetBit(j + 1)) {
                cluster.add(ordinalList[j]);
                pending.clear(j);
            }
            clusters.add(cluster);
        }

        List<TaskCluster> tcSet = new ArrayList<TaskCluster>();
        counter = 0;
        for (List<TaskId> cluster : clusters) {
            List<Task> taskStates = new ArrayList<Task>();
            for (TaskId tid : cluster) {
                taskStates.add(activityPlanMap.get(tid.getActivityId()).getTasks()[tid.getPartition()]);
            }
            TaskCluster tc = new TaskCluster(new TaskClusterId(ac.getId(), counter++), ac,
                    taskStates.toArray(new Task[taskStates.size()]));
            tcSet.add(tc);
            for (TaskId tid : cluster) {
                activityPlanMap.get(tid.getActivityId()).getTasks()[tid.getPartition()].setTaskCluster(tc);
            }
        }
        TaskCluster[] taskClusters = tcSet.toArray(new TaskCluster[tcSet.size()]);
        return taskClusters;
    }

    private TaskCluster getTaskCluster(TaskId tid) {
        JobRun run = scheduler.getJobRun();
        ActivityCluster ac = run.getActivityClusterGraph().getActivityMap().get(tid.getActivityId());
        ActivityClusterPlan acp = run.getActivityClusterPlanMap().get(ac.getId());
        Task[] tasks = acp.getActivityPlanMap().get(tid.getActivityId()).getTasks();
        Task task = tasks[tid.getPartition()];
        assert task.getTaskId().equals(tid);
        return task.getTaskCluster();
    }

    private void getDependencyActivityIds(Set<ActivityId> depAnIds, ActivityId anId, ActivityCluster ac) {
        Set<ActivityId> blockers = ac.getBlocked2BlockerMap().get(anId);
        if (blockers != null) {
            depAnIds.addAll(blockers);
        }
    }

    private void assignConnectorPolicy(ActivityCluster ac, Map<ActivityId, ActivityPlan> taskMap) {
        Map<ConnectorDescriptorId, IConnectorPolicy> cPolicyMap = new HashMap<ConnectorDescriptorId, IConnectorPolicy>();
        Set<ActivityId> activities = ac.getActivityMap().keySet();
        BitSet targetBitmap = new BitSet();
        for (ActivityId a1 : activities) {
            Task[] ac1TaskStates = taskMap.get(a1).getTasks();
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = ac.getActivityOutputMap().get(a1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    ActivityId a2 = ac.getConsumerActivity(cdId);
                    Task[] ac2TaskStates = taskMap.get(a2).getTasks();
                    int nConsumers = ac2TaskStates.length;

                    int[] fanouts = new int[nProducers];
                    if (c.allProducersToAllConsumers()) {
                        for (int i = 0; i < nProducers; ++i) {
                            fanouts[i] = nConsumers;
                        }
                    } else {
                        for (int i = 0; i < nProducers; ++i) {
                            c.indicateTargetPartitions(nProducers, nConsumers, i, targetBitmap);
                            fanouts[i] = targetBitmap.cardinality();
                        }
                    }
                    IConnectorPolicy cp = assignConnectorPolicy(ac, c, nProducers, nConsumers, fanouts);
                    cPolicyMap.put(cdId, cp);
                }
            }
        }
        scheduler.getJobRun().getConnectorPolicyMap().putAll(cPolicyMap);
    }

    private IConnectorPolicy assignConnectorPolicy(ActivityCluster ac, IConnectorDescriptor c, int nProducers,
            int nConsumers, int[] fanouts) {
        IConnectorPolicyAssignmentPolicy cpap = ac.getConnectorPolicyAssignmentPolicy();
        if (cpap != null) {
            return cpap.getConnectorPolicyAssignment(c, nProducers, nConsumers, fanouts);
        }
        cpap = ac.getActivityClusterGraph().getConnectorPolicyAssignmentPolicy();
        if (cpap != null) {
            return cpap.getConnectorPolicyAssignment(c, nProducers, nConsumers, fanouts);
        }
        return new PipeliningConnectorPolicy();
    }

    private Map<ActivityId, ActivityPartitionDetails> computePartitionCounts(ActivityCluster ac)
            throws HyracksException {
        PartitionConstraintSolver solver = scheduler.getSolver();
        Set<LValueConstraintExpression> lValues = new HashSet<LValueConstraintExpression>();
        for (ActivityId anId : ac.getActivityMap().keySet()) {
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
        for (ActivityId anId : ac.getActivityMap().keySet()) {
            int nParts = nPartMap.get(anId.getOperatorDescriptorId());
            int[] nInputPartitions = null;
            List<IConnectorDescriptor> inputs = ac.getActivityInputMap().get(anId);
            if (inputs != null) {
                nInputPartitions = new int[inputs.size()];
                for (int i = 0; i < nInputPartitions.length; ++i) {
                    ConnectorDescriptorId cdId = inputs.get(i).getConnectorId();
                    ActivityId aid = ac.getProducerActivity(cdId);
                    Integer nPartInt = nPartMap.get(aid.getOperatorDescriptorId());
                    nInputPartitions[i] = nPartInt;
                }
            }
            int[] nOutputPartitions = null;
            List<IConnectorDescriptor> outputs = ac.getActivityOutputMap().get(anId);
            if (outputs != null) {
                nOutputPartitions = new int[outputs.size()];
                for (int i = 0; i < nOutputPartitions.length; ++i) {
                    ConnectorDescriptorId cdId = outputs.get(i).getConnectorId();
                    ActivityId aid = ac.getConsumerActivity(cdId);
                    Integer nPartInt = nPartMap.get(aid.getOperatorDescriptorId());
                    nOutputPartitions[i] = nPartInt;
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