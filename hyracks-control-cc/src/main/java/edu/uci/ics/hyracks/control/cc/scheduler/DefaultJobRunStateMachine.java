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

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.PipelinedConnectorPolicy;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.util.Pair;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.IConnectorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.job.Task;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobCleanupEvent;

public class DefaultJobRunStateMachine implements IJobRunStateMachine {
    private static final Logger LOGGER = Logger.getLogger(DefaultJobRunStateMachine.class.getName());

    private final ClusterControllerService ccs;

    private final JobRun jobRun;

    private final Map<OperatorDescriptorId, String> operatorLocationAssignmentMap;

    private final Set<ActivityCluster> completedClusters;

    private final Set<ActivityCluster> inProgressClusters;

    private PartitionConstraintSolver solver;

    private ActivityCluster rootActivityCluster;

    public DefaultJobRunStateMachine(ClusterControllerService ccs, JobRun jobRun) {
        this.ccs = ccs;
        this.jobRun = jobRun;
        this.operatorLocationAssignmentMap = new HashMap<OperatorDescriptorId, String>();
        completedClusters = new HashSet<ActivityCluster>();
        inProgressClusters = new HashSet<ActivityCluster>();
    }

    public Map<OperatorDescriptorId, String> getOperatorLocationAssignmentMap() {
        return operatorLocationAssignmentMap;
    }

    public PartitionConstraintSolver getSolver() {
        return solver;
    }

    private static Pair<ActivityId, ActivityId> findMergePair(JobActivityGraph jag, JobSpecification spec,
            Set<ActivityCluster> eqSets) {
        Map<ActivityId, IActivity> activityNodeMap = jag.getActivityNodeMap();
        for (ActivityCluster eqSet : eqSets) {
            for (ActivityId t : eqSet.getActivities()) {
                IActivity activity = activityNodeMap.get(t);
                List<Integer> inputList = jag.getActivityInputMap().get(t);
                if (inputList != null) {
                    for (Integer idx : inputList) {
                        IConnectorDescriptor conn = spec.getInputConnectorDescriptor(activity.getActivityId()
                                .getOperatorDescriptorId(), idx);
                        OperatorDescriptorId producerId = spec.getProducer(conn).getOperatorId();
                        int producerOutputIndex = spec.getProducerOutputIndex(conn);
                        ActivityId inTask = jag.getOperatorOutputMap().get(producerId).get(producerOutputIndex);
                        if (!eqSet.getActivities().contains(inTask)) {
                            return new Pair<ActivityId, ActivityId>(t, inTask);
                        }
                    }
                }
                List<Integer> outputList = jag.getActivityOutputMap().get(t);
                if (outputList != null) {
                    for (Integer idx : outputList) {
                        IConnectorDescriptor conn = spec.getOutputConnectorDescriptor(activity.getActivityId()
                                .getOperatorDescriptorId(), idx);
                        OperatorDescriptorId consumerId = spec.getConsumer(conn).getOperatorId();
                        int consumerInputIndex = spec.getConsumerInputIndex(conn);
                        ActivityId outTask = jag.getOperatorInputMap().get(consumerId).get(consumerInputIndex);
                        if (!eqSet.getActivities().contains(outTask)) {
                            return new Pair<ActivityId, ActivityId>(t, outTask);
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
        Map<ActivityId, ActivityCluster> stageMap = new HashMap<ActivityId, ActivityCluster>();
        Set<ActivityCluster> stages = new HashSet<ActivityCluster>();
        for (Set<ActivityId> taskIds : jag.getOperatorActivityMap().values()) {
            for (ActivityId taskId : taskIds) {
                Set<ActivityId> eqSet = new HashSet<ActivityId>();
                eqSet.add(taskId);
                ActivityCluster stage = new ActivityCluster(jobRun, eqSet);
                stageMap.put(taskId, stage);
                stages.add(stage);
            }
        }

        boolean changed = true;
        while (changed) {
            changed = false;
            Pair<ActivityId, ActivityId> pair = findMergePair(jag, spec, stages);
            if (pair != null) {
                merge(stageMap, stages, pair.first, pair.second);
                changed = true;
            }
        }

        ActivityCluster endStage = new ActivityCluster(jobRun, new HashSet<ActivityId>());
        Map<ActivityId, Set<ActivityId>> blocker2BlockedMap = jag.getBlocker2BlockedMap();
        for (ActivityCluster s : stages) {
            endStage.addDependency(s);
            s.addDependent(endStage);
            Set<ActivityCluster> blockedStages = new HashSet<ActivityCluster>();
            for (ActivityId t : s.getActivities()) {
                Set<ActivityId> blockedTasks = blocker2BlockedMap.get(t);
                if (blockedTasks != null) {
                    for (ActivityId bt : blockedTasks) {
                        blockedStages.add(stageMap.get(bt));
                    }
                }
            }
            for (ActivityCluster bs : blockedStages) {
                bs.addDependency(s);
                s.addDependent(bs);
            }
        }
        jobRun.getActivityClusterMap().putAll(stageMap);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inferred " + (stages.size() + 1) + " stages");
            for (ActivityCluster s : stages) {
                LOGGER.info(s.toString());
            }
            LOGGER.info("SID: ENDSTAGE");
        }
        return endStage;
    }

    private void merge(Map<ActivityId, ActivityCluster> eqSetMap, Set<ActivityCluster> eqSets, ActivityId t1,
            ActivityId t2) {
        ActivityCluster stage1 = eqSetMap.get(t1);
        Set<ActivityId> s1 = stage1.getActivities();
        ActivityCluster stage2 = eqSetMap.get(t2);
        Set<ActivityId> s2 = stage2.getActivities();

        Set<ActivityId> mergedSet = new HashSet<ActivityId>();
        mergedSet.addAll(s1);
        mergedSet.addAll(s2);

        eqSets.remove(stage1);
        eqSets.remove(stage2);
        ActivityCluster mergedStage = new ActivityCluster(jobRun, mergedSet);
        eqSets.add(mergedStage);

        for (ActivityId t : mergedSet) {
            eqSetMap.put(t, mergedStage);
        }
    }

    private void findRunnableActivityClusters(Set<ActivityCluster> frontier, ActivityCluster candidate) {
        if (completedClusters.contains(candidate) || frontier.contains(candidate)
                || inProgressClusters.contains(candidate)) {
            return;
        }
        boolean runnable = true;
        for (ActivityCluster s : candidate.getDependencies()) {
            if (!completedClusters.contains(s)) {
                runnable = false;
                findRunnableActivityClusters(frontier, s);
            }
        }
        if (runnable && candidate != rootActivityCluster) {
            frontier.add(candidate);
        }
    }

    private void findRunnableActivityClusters(Set<ActivityCluster> frontier) {
        findRunnableActivityClusters(frontier, rootActivityCluster);
    }

    @Override
    public void schedule() throws HyracksException {
        try {
            solver = new PartitionConstraintSolver();
            final JobActivityGraph jag = jobRun.getJobActivityGraph();
            final ICCApplicationContext appCtx = ccs.getApplicationMap().get(jag.getApplicationName());
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
                    op.contributeSchedulingConstraints(acceptor, jag, appCtx);
                }
            });
            PlanUtils.visit(spec, new IConnectorDescriptorVisitor() {
                @Override
                public void visit(IConnectorDescriptor conn) {
                    conn.contributeSchedulingConstraints(acceptor, jag, appCtx);
                }
            });
            contributedConstraints.addAll(spec.getUserConstraints());
            solver.addConstraints(contributedConstraints);

            rootActivityCluster = inferStages(jag);
            startRunnableActivityClusters();
        } catch (Exception e) {
            e.printStackTrace();
            ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobRun.getJobId(), JobStatus.FAILURE, e));
            throw new HyracksException(e);
        }
    }

    private void startRunnableActivityClusters() throws HyracksException {
        Set<ActivityCluster> runnableClusters = new HashSet<ActivityCluster>();
        findRunnableActivityClusters(runnableClusters);
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
    }

    private Map<ActivityId, ActivityPartitionDetails> computePartitionCounts(ActivityCluster ac)
            throws HyracksException {
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

    private void buildTaskClusters(ActivityCluster ac) throws HyracksException {
        Map<ActivityId, ActivityPartitionDetails> pcMap = computePartitionCounts(ac);

        Map<ActivityId, Task[]> taskStateMap = ac.getTaskMap();
        Set<ActivityId> activities = ac.getActivities();

        Map<TaskId, Set<TaskId>> taskClusterMap = new HashMap<TaskId, Set<TaskId>>();

        for (ActivityId anId : activities) {
            ActivityPartitionDetails apd = pcMap.get(anId);
            Task[] taskStates = new Task[apd.getPartitionCount()];
            for (int i = 0; i < taskStates.length; ++i) {
                TaskId tid = new TaskId(anId, i);
                taskStates[i] = new Task(tid, apd);
                Set<TaskId> cluster = new HashSet<TaskId>();
                cluster.add(tid);
                taskClusterMap.put(tid, cluster);
            }
            taskStateMap.put(anId, taskStates);
        }

        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = assignConnectorPolicy(ac, pcMap);
        ac.setConnectorPolicyMap(connectorPolicies);

        Map<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>> taskConnectivity = new HashMap<TaskId, List<Pair<TaskId, ConnectorDescriptorId>>>();
        JobActivityGraph jag = jobRun.getJobActivityGraph();
        BitSet targetBitmap = new BitSet();
        for (ActivityId ac1 : activities) {
            Task[] ac1TaskStates = taskStateMap.get(ac1);
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = jag.getActivityOutputConnectorDescriptors(ac1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    ActivityId ac2 = jag.getConsumerActivity(cdId);
                    Task[] ac2TaskStates = taskStateMap.get(ac2);
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
        for (Set<TaskId> cluster : clusters) {
            Set<Task> taskStates = new HashSet<Task>();
            for (TaskId tid : cluster) {
                taskStates.add(taskStateMap.get(tid.getActivityId())[tid.getPartition()]);
            }
            TaskCluster tc = new TaskCluster(ac, taskStates.toArray(new Task[taskStates.size()]));
            tcSet.add(tc);
            for (TaskId tid : cluster) {
                taskStateMap.get(tid.getActivityId())[tid.getPartition()].setTaskCluster(tc);
            }
        }
        ac.setTaskClusters(tcSet.toArray(new TaskCluster[tcSet.size()]));

        Map<PartitionId, TaskCluster> partitionProducingTaskClusterMap = ac.getPartitionProducingTaskClusterMap();
        for (TaskCluster tc : ac.getTaskClusters()) {
            for (Task ts : tc.getTasks()) {
                TaskId tid = ts.getTaskId();
                List<Pair<TaskId, ConnectorDescriptorId>> cInfoList = taskConnectivity.get(tid);
                if (cInfoList != null) {
                    for (Pair<TaskId, ConnectorDescriptorId> p : cInfoList) {
                        Task targetTS = taskStateMap.get(p.first.getActivityId())[p.first.getPartition()];
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
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Plan for " + ac);
            LOGGER.info("Built " + tcSet.size() + " Task Clusters");
            for (TaskCluster tc : tcSet) {
                LOGGER.info("Tasks: " + Arrays.toString(tc.getTasks()));
            }
        }
    }

    private Map<ConnectorDescriptorId, IConnectorPolicy> assignConnectorPolicy(ActivityCluster ac,
            Map<ActivityId, ActivityPartitionDetails> pcMap) {
        JobActivityGraph jag = jobRun.getJobActivityGraph();
        Map<ConnectorDescriptorId, IConnectorPolicy> cPolicyMap = new HashMap<ConnectorDescriptorId, IConnectorPolicy>();
        Set<ActivityId> activities = ac.getActivities();
        Map<ActivityId, Task[]> taskStateMap = ac.getTaskMap();
        BitSet targetBitmap = new BitSet();
        for (ActivityId ac1 : activities) {
            Task[] ac1TaskStates = taskStateMap.get(ac1);
            int nProducers = ac1TaskStates.length;
            List<IConnectorDescriptor> outputConns = jag.getActivityOutputConnectorDescriptors(ac1);
            if (outputConns != null) {
                for (IConnectorDescriptor c : outputConns) {
                    ConnectorDescriptorId cdId = c.getConnectorId();
                    ActivityId ac2 = jag.getConsumerActivity(cdId);
                    Task[] ac2TaskStates = taskStateMap.get(ac2);
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
        IConnectorPolicyAssignmentPolicy cpap = jobRun.getJobActivityGraph().getJobSpecification()
                .getConnectorPolicyAssignmentPolicy();
        if (cpap != null) {
            return cpap.getConnectorPolicyAssignment(c, nProducers, nConsumers, fanouts);
        }
        return new PipelinedConnectorPolicy();
    }

    @Override
    public void notifyActivityClusterFailure(ActivityCluster ac, Exception exception) throws HyracksException {
        for (ActivityCluster ac2 : inProgressClusters) {
            abortActivityCluster(ac2);
        }
        jobRun.setStatus(JobStatus.FAILURE, exception);
    }

    private void abortActivityCluster(ActivityCluster ac) throws HyracksException {
        ac.getStateMachine().abort();
    }

    @Override
    public void notifyActivityClusterComplete(ActivityCluster ac) throws HyracksException {
        completedClusters.add(ac);
        inProgressClusters.remove(ac);
        startRunnableActivityClusters();
    }

    @Override
    public void notifyNodeFailures(Set<String> deadNodes) throws HyracksException {
        jobRun.getPartitionMatchMaker().notifyNodeFailures(deadNodes);
        if (!inProgressClusters.isEmpty()) {
            for (ActivityCluster ac : inProgressClusters) {
                ac.notifyNodeFailures(deadNodes);
            }
        }
    }
}