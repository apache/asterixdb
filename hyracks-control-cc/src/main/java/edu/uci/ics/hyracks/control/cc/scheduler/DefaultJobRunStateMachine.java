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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.util.Pair;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.IConnectorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskState;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobCleanupEvent;
import edu.uci.ics.hyracks.control.common.job.dataflow.IConnectorPolicy;

public class DefaultJobRunStateMachine implements IJobRunStateMachine {
    private static final Logger LOGGER = Logger.getLogger(DefaultJobRunStateMachine.class.getName());

    private final ClusterControllerService ccs;

    private final JobRun jobRun;

    private final Set<ActivityCluster> completedClusters;

    private final Set<ActivityCluster> inProgressClusters;

    private PartitionConstraintSolver solver;

    private ActivityCluster rootActivityCluster;

    public DefaultJobRunStateMachine(ClusterControllerService ccs, JobRun jobRun) {
        this.ccs = ccs;
        this.jobRun = jobRun;
        completedClusters = new HashSet<ActivityCluster>();
        inProgressClusters = new HashSet<ActivityCluster>();
    }

    public PartitionConstraintSolver getSolver() {
        return solver;
    }

    private static Pair<ActivityNodeId, ActivityNodeId> findMergePair(JobActivityGraph jag, JobSpecification spec,
            Set<ActivityCluster> eqSets) {
        Map<ActivityNodeId, IActivityNode> activityNodeMap = jag.getActivityNodeMap();
        for (ActivityCluster eqSet : eqSets) {
            for (ActivityNodeId t : eqSet.getActivities()) {
                IOperatorDescriptor owner = activityNodeMap.get(t).getOwner();
                List<Integer> inputList = jag.getActivityInputMap().get(t);
                if (inputList != null) {
                    for (Integer idx : inputList) {
                        IConnectorDescriptor conn = spec.getInputConnectorDescriptor(owner, idx);
                        OperatorDescriptorId producerId = spec.getProducer(conn).getOperatorId();
                        int producerOutputIndex = spec.getProducerOutputIndex(conn);
                        ActivityNodeId inTask = jag.getOperatorOutputMap().get(producerId).get(producerOutputIndex);
                        if (!eqSet.getActivities().contains(inTask)) {
                            return new Pair<ActivityNodeId, ActivityNodeId>(t, inTask);
                        }
                    }
                }
                List<Integer> outputList = jag.getActivityOutputMap().get(t);
                if (outputList != null) {
                    for (Integer idx : outputList) {
                        IConnectorDescriptor conn = spec.getOutputConnectorDescriptor(owner, idx);
                        OperatorDescriptorId consumerId = spec.getConsumer(conn).getOperatorId();
                        int consumerInputIndex = spec.getConsumerInputIndex(conn);
                        ActivityNodeId outTask = jag.getOperatorInputMap().get(consumerId).get(consumerInputIndex);
                        if (!eqSet.getActivities().contains(outTask)) {
                            return new Pair<ActivityNodeId, ActivityNodeId>(t, outTask);
                        }
                    }
                }
            }
        }
        return null;
    }

    private ActivityCluster inferStages(JobActivityGraph jag) {
        JobSpecification spec = jag.getJobSpecification();

        /*
         * Build initial equivalence sets map. We create a map such that for each IOperatorTask, t -> { t }
         */
        Map<ActivityNodeId, ActivityCluster> stageMap = new HashMap<ActivityNodeId, ActivityCluster>();
        Set<ActivityCluster> stages = new HashSet<ActivityCluster>();
        for (Set<ActivityNodeId> taskIds : jag.getOperatorActivityMap().values()) {
            for (ActivityNodeId taskId : taskIds) {
                Set<ActivityNodeId> eqSet = new HashSet<ActivityNodeId>();
                eqSet.add(taskId);
                ActivityCluster stage = new ActivityCluster(jobRun, eqSet);
                stageMap.put(taskId, stage);
                stages.add(stage);
            }
        }

        boolean changed = true;
        while (changed) {
            changed = false;
            Pair<ActivityNodeId, ActivityNodeId> pair = findMergePair(jag, spec, stages);
            if (pair != null) {
                merge(stageMap, stages, pair.first, pair.second);
                changed = true;
            }
        }

        ActivityCluster endStage = new ActivityCluster(jobRun, new HashSet<ActivityNodeId>());
        Map<ActivityNodeId, Set<ActivityNodeId>> blocker2BlockedMap = jag.getBlocker2BlockedMap();
        for (ActivityCluster s : stages) {
            endStage.addDependency(s);
            s.addDependent(endStage);
            Set<ActivityCluster> blockedStages = new HashSet<ActivityCluster>();
            for (ActivityNodeId t : s.getActivities()) {
                Set<ActivityNodeId> blockedTasks = blocker2BlockedMap.get(t);
                if (blockedTasks != null) {
                    for (ActivityNodeId bt : blockedTasks) {
                        blockedStages.add(stageMap.get(bt));
                    }
                }
            }
            for (ActivityCluster bs : blockedStages) {
                bs.addDependency(s);
                s.addDependent(bs);
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inferred " + (stages.size() + 1) + " stages");
            for (ActivityCluster s : stages) {
                LOGGER.info(s.toString());
            }
            LOGGER.info("SID: ENDSTAGE");
        }
        return endStage;
    }

    private void merge(Map<ActivityNodeId, ActivityCluster> eqSetMap, Set<ActivityCluster> eqSets, ActivityNodeId t1,
            ActivityNodeId t2) {
        ActivityCluster stage1 = eqSetMap.get(t1);
        Set<ActivityNodeId> s1 = stage1.getActivities();
        ActivityCluster stage2 = eqSetMap.get(t2);
        Set<ActivityNodeId> s2 = stage2.getActivities();

        Set<ActivityNodeId> mergedSet = new HashSet<ActivityNodeId>();
        mergedSet.addAll(s1);
        mergedSet.addAll(s2);

        eqSets.remove(stage1);
        eqSets.remove(stage2);
        ActivityCluster mergedStage = new ActivityCluster(jobRun, mergedSet);
        eqSets.add(mergedStage);

        for (ActivityNodeId t : mergedSet) {
            eqSetMap.put(t, mergedStage);
        }
    }

    private void findRunnableClusters(Set<ActivityCluster> frontier, ActivityCluster candidate) {
        if (completedClusters.contains(candidate) || frontier.contains(candidate)) {
            return;
        }
        boolean runnable = true;
        for (ActivityCluster s : candidate.getDependencies()) {
            if (!completedClusters.contains(s)) {
                runnable = false;
                findRunnableClusters(frontier, s);
            }
        }
        if (runnable && candidate != rootActivityCluster) {
            frontier.add(candidate);
        }
    }

    private void findRunnableClusters(Set<ActivityCluster> frontier) {
        findRunnableClusters(frontier, rootActivityCluster);
    }

    @Override
    public void schedule() throws HyracksException {
        try {
            solver = new PartitionConstraintSolver();
            final JobActivityGraph jag = jobRun.getJobActivityGraph();
            JobSpecification spec = jag.getJobSpecification();
            final Set<Constraint> contributedConstraints = new HashSet<Constraint>();
            final IConstraintAcceptor acceptor = new IConstraintAcceptor() {
                @Override
                public void addConstraint(Constraint constraint) {
                    contributedConstraints.add(constraint);
                }
            };
            PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
                @Override
                public void visit(IOperatorDescriptor op) {
                    op.contributeSchedulingConstraints(acceptor, jag);
                }
            });
            PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
                @Override
                public void visit(IConnectorDescriptor conn) {
                    conn.contributeSchedulingConstraints(acceptor, jag);
                }
            });
            contributedConstraints.addAll(spec.getUserConstraints());
            solver.addConstraints(contributedConstraints);

            rootActivityCluster = inferStages(jag);
            Set<ActivityCluster> runnableClusters = new HashSet<ActivityCluster>();
            findRunnableClusters(runnableClusters);
            if (runnableClusters.isEmpty() && inProgressClusters.isEmpty()) {
                ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobRun.getJobId(), JobStatus.TERMINATED, null));
                return;
            }
            for (ActivityCluster ac : runnableClusters) {
                inProgressClusters.add(ac);
                buildTaskClusters(ac);
                IActivityClusterStateMachine acsm = new DefaultActivityClusterStateMachine(ccs, this, ac);
                ac.setStateMachine(acsm);
                acsm.schedule();
            }
        } catch (Exception e) {
            ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobRun.getJobId(), JobStatus.FAILURE, e));
            throw new HyracksException(e);
        }
    }

    private Map<ActivityNodeId, Integer> computePartitionCounts(ActivityCluster ac) throws HyracksException {
        Set<LValueConstraintExpression> lValues = new HashSet<LValueConstraintExpression>();
        for (ActivityNodeId anId : ac.getActivities()) {
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
        Map<ActivityNodeId, Integer> activityPartsMap = new HashMap<ActivityNodeId, Integer>();
        for (ActivityNodeId anId : ac.getActivities()) {
            activityPartsMap.put(anId, nPartMap.get(anId.getOperatorDescriptorId()));
        }
        return activityPartsMap;
    }

    private void buildTaskClusters(ActivityCluster ac) throws HyracksException {
        Map<ActivityNodeId, Integer> pcMap = computePartitionCounts(ac);
        Map<ActivityNodeId, TaskState[]> taskStateMap = ac.getTaskStateMap();

        for (ActivityNodeId anId : ac.getActivities()) {
            int nParts = pcMap.get(anId);
            TaskState[] taskStates = new TaskState[nParts];
            for (int i = 0; i < nParts; ++i) {
                taskStates[i] = new TaskState(new TaskId(anId, i));
            }
            taskStateMap.put(anId, taskStates);
        }

        Set<ActivityNodeId> activities = ac.getActivities();

        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = new HashMap<ConnectorDescriptorId, IConnectorPolicy>();

        Map<TaskId, Set<TaskId>> taskClusterMap = new HashMap<TaskId, Set<TaskId>>();
        for (ActivityNodeId anId : activities) {
            TaskState[] taskStates = taskStateMap.get(anId);
            for (TaskState ts : taskStates) {
                Set<TaskId> cluster = new HashSet<TaskId>();
                cluster.add(ts.getTaskId());
                taskClusterMap.put(ts.getTaskId(), cluster);
            }
        }

        Map<TaskId, List<Pair<TaskId, IConnectorPolicy>>> connectionInfo = new HashMap<TaskId, List<Pair<TaskId, IConnectorPolicy>>>();
        JobActivityGraph jag = jobRun.getJobActivityGraph();
        BitSet targetBitmap = new BitSet();
        for (ActivityNodeId ac1 : activities) {
            TaskState[] ac1TaskStates = taskStateMap.get(ac1);
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = jag.getActivityOutputConnectorDescriptors(ac1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    IConnectorPolicy cPolicy = connectorPolicies.get(cdId);
                    ActivityNodeId ac2 = jag.getConsumerActivity(cdId);
                    TaskState[] ac2TaskStates = taskStateMap.get(ac2);
                    int nConsumers = ac2TaskStates.length;
                    for (int i = 0; i < nProducers; ++i) {
                        c.indicateTargetPartitions(nProducers, nConsumers, i, targetBitmap);
                        List<Pair<TaskId, IConnectorPolicy>> cInfoList = connectionInfo.get(ac1TaskStates[i]
                                .getTaskId());
                        if (cInfoList == null) {
                            cInfoList = new ArrayList<Pair<TaskId, IConnectorPolicy>>();
                            connectionInfo.put(ac1TaskStates[i].getTaskId(), cInfoList);
                        }
                        Set<TaskId> cluster = taskClusterMap.get(ac1TaskStates[i].getTaskId());
                        for (int j = targetBitmap.nextSetBit(0); j >= 0; j = targetBitmap.nextSetBit(j)) {
                            cInfoList.add(new Pair<TaskId, IConnectorPolicy>(ac2TaskStates[j].getTaskId(), cPolicy));
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
            for (Map.Entry<TaskId, Set<TaskId>> e : taskClusterMap.entrySet()) {
                set.clear();
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
            for (TaskId tid : set) {
                taskClusterMap.put(tid, set);
            }
        }

        Set<Set<TaskId>> clusters = new HashSet<Set<TaskId>>(taskClusterMap.values());
        Set<TaskCluster> tcSet = new HashSet<TaskCluster>();
        for (Set<TaskId> cluster : clusters) {
            Set<TaskState> taskStates = new HashSet<TaskState>();
            for (TaskId tid : cluster) {
                taskStates.add(taskStateMap.get(tid.getActivityId())[tid.getPartition()]);
            }
            TaskCluster tc = new TaskCluster(ac, taskStates.toArray(new TaskState[taskStates.size()]));
            tcSet.add(tc);
            for (TaskId tid : cluster) {
                taskStateMap.get(tid.getActivityId())[tid.getPartition()].setTaskCluster(tc);
            }
        }
        ac.setTaskClusters(tcSet.toArray(new TaskCluster[tcSet.size()]));

        for (TaskCluster tc : tcSet) {
            for (TaskState ts : tc.getTasks()) {
                TaskId tid = ts.getTaskId();
                List<Pair<TaskId, IConnectorPolicy>> cInfoList = connectionInfo.get(tid);
                if (cInfoList != null) {
                    for (Pair<TaskId, IConnectorPolicy> p : cInfoList) {
                        TaskState targetTS = taskStateMap.get(p.first.getActivityId())[p.first.getPartition()];
                        TaskCluster targetTC = targetTS.getTaskCluster();
                        if (targetTC != tc) {
                            targetTC.getDependencies().add(tc);
                            if (p.second.consumerWaitsForProducerToFinish()) {
                                targetTC.getBlockers().add(tc);
                            }
                        }
                    }
                }
            }
        }

        computeBlockerClosure(tcSet);
        computeDependencyClosure(tcSet);
    }

    private void computeDependencyClosure(Set<TaskCluster> tcSet) {
        boolean done = false;
        while (!done) {
            done = true;
            for (TaskCluster tc : tcSet) {
                Set<TaskCluster> deps = tc.getDependencies();
                if (!deps.isEmpty()) {
                    Set<TaskCluster> copy = new HashSet<TaskCluster>(deps);
                    for (TaskCluster tc2 : copy) {
                        for (TaskCluster tc3 : tc2.getDependencies()) {
                            if (!deps.contains(tc3)) {
                                deps.add(tc3);
                                done = false;
                            }
                        }
                    }
                }
            }
        }
    }

    private void computeBlockerClosure(Set<TaskCluster> tcSet) {
        boolean done = false;
        while (!done) {
            done = true;
            for (TaskCluster tc : tcSet) {
                Set<TaskCluster> blockers = tc.getBlockers();
                if (!blockers.isEmpty()) {
                    Set<TaskCluster> copy = new HashSet<TaskCluster>(blockers);
                    for (TaskCluster tc2 : copy) {
                        for (TaskCluster tc3 : tc2.getBlockers()) {
                            if (!blockers.contains(tc3)) {
                                blockers.add(tc3);
                                done = false;
                            }
                        }
                    }
                }
            }
        }
    }
}