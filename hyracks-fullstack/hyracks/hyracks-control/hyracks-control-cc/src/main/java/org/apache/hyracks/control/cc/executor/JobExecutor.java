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
package org.apache.hyracks.control.cc.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.job.ActivityClusterPlan;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.job.Task;
import org.apache.hyracks.control.cc.job.TaskAttempt;
import org.apache.hyracks.control.cc.job.TaskCluster;
import org.apache.hyracks.control.cc.job.TaskClusterAttempt;
import org.apache.hyracks.control.cc.partitions.PartitionMatchMaker;
import org.apache.hyracks.control.cc.work.JobCleanupWork;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.common.job.TaskAttemptDescriptor;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.NoOpCallback;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobExecutor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;

    private final JobRun jobRun;

    private final PartitionConstraintSolver solver;

    private final DeployedJobSpecId deployedJobSpecId;

    private final Map<PartitionId, TaskCluster> partitionProducingTaskClusterMap;

    private final Set<TaskCluster> inProgressTaskClusters;

    private final Random random;

    private boolean cancelled = false;

    public JobExecutor(ClusterControllerService ccs, JobRun jobRun, Collection<Constraint> constraints,
            DeployedJobSpecId deployedJobSpecId) {
        this.ccs = ccs;
        this.jobRun = jobRun;
        this.deployedJobSpecId = deployedJobSpecId;
        solver = new PartitionConstraintSolver();
        partitionProducingTaskClusterMap = new HashMap<>();
        inProgressTaskClusters = new HashSet<>();
        solver.addConstraints(constraints);
        random = new Random();
    }

    public boolean isDeployed() {
        return deployedJobSpecId != null;
    }

    public JobRun getJobRun() {
        return jobRun;
    }

    public PartitionConstraintSolver getSolver() {
        return solver;
    }

    public void startJob() throws HyracksException {
        startRunnableActivityClusters();
        ccs.getContext().notifyJobStart(jobRun.getJobId());
    }

    public void cancelJob(IResultCallback<Void> callback) throws HyracksException {
        // If the job is already terminated or failed, do nothing here.
        if (jobRun.getPendingStatus() != null) {
            callback.setValue(null);
            return;
        }
        // Sets the cancelled flag.
        cancelled = true;
        // Aborts on-ongoing task clusters.
        abortOngoingTaskClusters(ta -> false, ta -> null);
        // Aborts the whole job.
        abortJob(Collections.singletonList(HyracksException.create(ErrorCode.JOB_CANCELED, jobRun.getJobId())),
                callback);
    }

    private void findRunnableTaskClusterRoots(Set<TaskCluster> frontier, Collection<ActivityCluster> roots)
            throws HyracksException {
        for (ActivityCluster root : roots) {
            findRunnableTaskClusterRoots(frontier, root);
        }
    }

    private void findRunnableTaskClusterRoots(Set<TaskCluster> frontier, ActivityCluster candidate)
            throws HyracksException {
        boolean depsComplete = true;
        for (ActivityCluster depAC : candidate.getDependencies()) {
            if (!isPlanned(depAC)) {
                depsComplete = false;
                findRunnableTaskClusterRoots(frontier, depAC);
            } else {
                boolean tcRootsComplete = true;
                for (TaskCluster tc : getActivityClusterPlan(depAC).getTaskClusters()) {
                    if (!tc.getProducedPartitions().isEmpty()) {
                        continue;
                    }
                    TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
                    if (tca == null || tca.getStatus() != TaskClusterAttempt.TaskClusterStatus.COMPLETED) {
                        tcRootsComplete = false;
                        break;
                    }
                }
                if (!tcRootsComplete) {
                    depsComplete = false;
                    findRunnableTaskClusterRoots(frontier, depAC);
                }
            }
        }
        if (!depsComplete) {
            return;
        }
        if (!isPlanned(candidate)) {
            ActivityClusterPlanner acp = new ActivityClusterPlanner(this);
            ActivityClusterPlan acPlan = acp.planActivityCluster(candidate);
            jobRun.getActivityClusterPlanMap().put(candidate.getId(), acPlan);
            partitionProducingTaskClusterMap.putAll(acp.getPartitionProducingTaskClusterMap());
        }
        for (TaskCluster tc : getActivityClusterPlan(candidate).getTaskClusters()) {
            if (!tc.getProducedPartitions().isEmpty()) {
                continue;
            }
            TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
            if (tca == null || tca.getStatus() != TaskClusterAttempt.TaskClusterStatus.COMPLETED) {
                frontier.add(tc);
            }
        }
    }

    private ActivityClusterPlan getActivityClusterPlan(ActivityCluster ac) {
        return jobRun.getActivityClusterPlanMap().get(ac.getId());
    }

    private boolean isPlanned(ActivityCluster ac) {
        return jobRun.getActivityClusterPlanMap().get(ac.getId()) != null;
    }

    private void startRunnableActivityClusters() throws HyracksException {
        Set<TaskCluster> taskClusterRoots = new HashSet<>();
        findRunnableTaskClusterRoots(taskClusterRoots,
                jobRun.getActivityClusterGraph().getActivityClusterMap().values());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.log(Level.TRACE,
                    "Runnable TC roots: " + taskClusterRoots + ", inProgressTaskClusters: " + inProgressTaskClusters);
        }
        if (taskClusterRoots.isEmpty() && inProgressTaskClusters.isEmpty()) {
            ccs.getWorkQueue().schedule(new JobCleanupWork(ccs.getJobManager(), jobRun.getJobId(), JobStatus.TERMINATED,
                    null, NoOpCallback.INSTANCE));
            return;
        }
        startRunnableTaskClusters(taskClusterRoots);
    }

    private void startRunnableTaskClusters(Set<TaskCluster> tcRoots) throws HyracksException {
        Map<TaskCluster, Runnability> runnabilityMap = new HashMap<>();
        for (TaskCluster tc : tcRoots) {
            assignRunnabilityRank(tc, runnabilityMap);
        }

        PriorityQueue<RankedRunnableTaskCluster> queue = new PriorityQueue<>();
        for (Map.Entry<TaskCluster, Runnability> e : runnabilityMap.entrySet()) {
            TaskCluster tc = e.getKey();
            Runnability runnability = e.getValue();
            if (runnability.getTag() != Runnability.Tag.RUNNABLE) {
                continue;
            }
            int priority = runnability.getPriority();
            if (priority >= 0 && priority < Integer.MAX_VALUE) {
                queue.add(new RankedRunnableTaskCluster(priority, tc));
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Ranked TCs: " + queue);
        }

        Map<String, List<TaskAttemptDescriptor>> taskAttemptMap = new HashMap<>();
        for (RankedRunnableTaskCluster rrtc : queue) {
            TaskCluster tc = rrtc.getTaskCluster();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Found runnable TC: " + tc);
                List<TaskClusterAttempt> attempts = tc.getAttempts();
                LOGGER.trace("Attempts so far:" + attempts.size());
                for (TaskClusterAttempt tcAttempt : attempts) {
                    LOGGER.trace("Status: " + tcAttempt.getStatus());
                }
            }
            assignTaskLocations(tc, taskAttemptMap);
        }

        if (taskAttemptMap.isEmpty()) {
            return;
        }

        startTasks(taskAttemptMap);
    }

    /*
     * Runnability rank has the following semantics
     * Runnability(Runnable TaskCluster depending on completed TaskClusters) = {RUNNABLE, 0}
     * Runnability(Runnable TaskCluster) = max(Rank(Dependent TaskClusters)) + 1
     * Runnability(Non-schedulable TaskCluster) = {NOT_RUNNABLE, _}
     */
    private Runnability assignRunnabilityRank(TaskCluster goal, Map<TaskCluster, Runnability> runnabilityMap) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Computing runnability: " + goal);
        }
        if (runnabilityMap.containsKey(goal)) {
            return runnabilityMap.get(goal);
        }
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(goal);
        if (lastAttempt != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Last Attempt Status: " + lastAttempt.getStatus());
            }
            if (lastAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.COMPLETED) {
                Runnability runnability = new Runnability(Runnability.Tag.COMPLETED, Integer.MIN_VALUE);
                runnabilityMap.put(goal, runnability);
                return runnability;
            }
            if (lastAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING) {
                Runnability runnability = new Runnability(Runnability.Tag.RUNNING, Integer.MIN_VALUE);
                runnabilityMap.put(goal, runnability);
                return runnability;
            }
        }
        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap = jobRun.getConnectorPolicyMap();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        Runnability aggregateRunnability = new Runnability(Runnability.Tag.RUNNABLE, 0);
        for (PartitionId pid : goal.getRequiredPartitions()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Inspecting required partition: " + pid);
            }
            Runnability runnability;
            ConnectorDescriptorId cdId = pid.getConnectorDescriptorId();
            IConnectorPolicy cPolicy = connectorPolicyMap.get(cdId);
            PartitionState maxState = pmm.getMaximumAvailableState(pid);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Policy: " + cPolicy + " maxState: " + maxState);
            }
            if (PartitionState.COMMITTED.equals(maxState)) {
                runnability = new Runnability(Runnability.Tag.RUNNABLE, 0);
            } else if (PartitionState.STARTED.equals(maxState) && !cPolicy.consumerWaitsForProducerToFinish()) {
                runnability = new Runnability(Runnability.Tag.RUNNABLE, 1);
            } else {
                runnability = assignRunnabilityRank(partitionProducingTaskClusterMap.get(pid), runnabilityMap);
                switch (runnability.getTag()) {
                    case RUNNABLE:
                        if (cPolicy.consumerWaitsForProducerToFinish()) {
                            runnability = new Runnability(Runnability.Tag.NOT_RUNNABLE, Integer.MAX_VALUE);
                        } else {
                            runnability = new Runnability(Runnability.Tag.RUNNABLE, runnability.getPriority() + 1);
                        }
                        break;

                    case NOT_RUNNABLE:
                        break;

                    case RUNNING:
                        if (cPolicy.consumerWaitsForProducerToFinish()) {
                            runnability = new Runnability(Runnability.Tag.NOT_RUNNABLE, Integer.MAX_VALUE);
                        } else {
                            runnability = new Runnability(Runnability.Tag.RUNNABLE, 1);
                        }
                        break;
                    default:
                        break;
                }
            }
            aggregateRunnability = Runnability.getWorstCase(aggregateRunnability, runnability);
            if (aggregateRunnability.getTag() == Runnability.Tag.NOT_RUNNABLE) {
                // already not runnable -- cannot get better. bail.
                break;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("aggregateRunnability: " + aggregateRunnability);
            }
        }
        runnabilityMap.put(goal, aggregateRunnability);
        return aggregateRunnability;
    }

    private void assignTaskLocations(TaskCluster tc, Map<String, List<TaskAttemptDescriptor>> taskAttemptMap)
            throws HyracksException {
        ActivityClusterGraph acg = jobRun.getActivityClusterGraph();
        Task[] tasks = tc.getTasks();
        List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
        int attempts = tcAttempts.size();
        TaskClusterAttempt tcAttempt = new TaskClusterAttempt(tc, attempts);
        Map<TaskId, TaskAttempt> taskAttempts = new HashMap<>();
        Map<TaskId, LValueConstraintExpression> locationMap = new HashMap<>();
        for (int i = 0; i < tasks.length; ++i) {
            Task ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = new TaskAttempt(tcAttempt,
                    new TaskAttemptId(new TaskId(tid.getActivityId(), tid.getPartition()), attempts), ts);
            taskAttempt.setStatus(TaskAttempt.TaskStatus.INITIALIZED, null);
            locationMap.put(tid,
                    new PartitionLocationExpression(tid.getActivityId().getOperatorDescriptorId(), tid.getPartition()));
            taskAttempts.put(tid, taskAttempt);
        }
        tcAttempt.setTaskAttempts(taskAttempts);
        solver.solve(locationMap.values());
        for (int i = 0; i < tasks.length; ++i) {
            Task ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = taskAttempts.get(tid);
            String nodeId = assignLocation(acg, locationMap, tid, taskAttempt);
            taskAttempt.setNodeId(nodeId);
            taskAttempt.setStatus(TaskAttempt.TaskStatus.RUNNING, null);
            taskAttempt.setStartTime(System.currentTimeMillis());
            List<TaskAttemptDescriptor> tads = taskAttemptMap.get(nodeId);
            if (tads == null) {
                tads = new ArrayList<>();
                taskAttemptMap.put(nodeId, tads);
            }
            OperatorDescriptorId opId = tid.getActivityId().getOperatorDescriptorId();
            jobRun.registerOperatorLocation(opId, tid.getPartition(), nodeId);
            ActivityPartitionDetails apd = ts.getActivityPlan().getActivityPartitionDetails();
            TaskAttemptDescriptor tad = new TaskAttemptDescriptor(taskAttempt.getTaskAttemptId(),
                    apd.getPartitionCount(), apd.getInputPartitionCounts(), apd.getOutputPartitionCounts());
            tads.add(tad);
        }
        tcAttempt.initializePendingTaskCounter();
        tcAttempts.add(tcAttempt);

        /*
         * Improvement for reducing master/slave message communications, for each TaskAttemptDescriptor,
         * we set the NetworkAddress[][] partitionLocations, in which each row is for an incoming connector descriptor
         * and each column is for an input channel of the connector.
         */
        INodeManager nodeManager = ccs.getNodeManager();
        taskAttemptMap.forEach((key, tads) -> {
            for (TaskAttemptDescriptor tad : tads) {
                TaskAttemptId taid = tad.getTaskAttemptId();
                int attempt = taid.getAttempt();
                TaskId tid = taid.getTaskId();
                ActivityId aid = tid.getActivityId();
                List<IConnectorDescriptor> inConnectors = acg.getActivityInputs(aid);
                int[] inPartitionCounts = tad.getInputPartitionCounts();
                if (inPartitionCounts == null) {
                    continue;
                }
                NetworkAddress[][] partitionLocations = new NetworkAddress[inPartitionCounts.length][];
                for (int i = 0; i < inPartitionCounts.length; ++i) {
                    ConnectorDescriptorId cdId = inConnectors.get(i).getConnectorId();
                    IConnectorPolicy policy = jobRun.getConnectorPolicyMap().get(cdId);
                    /*
                     * carry sender location information into a task
                     * when it is not the case that it is an re-attempt and the send-side
                     * is materialized blocking.
                     */
                    if (attempt > 0 && policy.materializeOnSendSide() && policy.consumerWaitsForProducerToFinish()) {
                        continue;
                    }
                    ActivityId producerAid = acg.getProducerActivity(cdId);
                    partitionLocations[i] = new NetworkAddress[inPartitionCounts[i]];
                    for (int j = 0; j < inPartitionCounts[i]; ++j) {
                        TaskId producerTaskId = new TaskId(producerAid, j);
                        String nodeId = findTaskLocation(producerTaskId);
                        partitionLocations[i][j] = nodeManager.getNodeControllerState(nodeId).getDataPort();
                    }
                }
                tad.setInputPartitionLocations(partitionLocations);
            }
        });

        tcAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.RUNNING);
        tcAttempt.setStartTime(System.currentTimeMillis());
        inProgressTaskClusters.add(tc);
    }

    private String assignLocation(ActivityClusterGraph acg, Map<TaskId, LValueConstraintExpression> locationMap,
            TaskId tid, TaskAttempt taskAttempt) throws HyracksException {
        ActivityId aid = tid.getActivityId();
        ActivityCluster ac = acg.getActivityMap().get(aid);
        Set<ActivityId> blockers = ac.getBlocked2BlockerMap().get(aid);
        String nodeId = null;
        if (blockers != null) {
            for (ActivityId blocker : blockers) {
                nodeId = findTaskLocation(new TaskId(blocker, tid.getPartition()));
                if (nodeId != null) {
                    break;
                }
            }
        }
        INodeManager nodeManager = ccs.getNodeManager();
        Collection<String> liveNodes = nodeManager.getAllNodeIds();
        if (nodeId == null) {
            LValueConstraintExpression pLocationExpr = locationMap.get(tid);
            Object location = solver.getValue(pLocationExpr);
            if (location == null) {
                // pick any
                nodeId = liveNodes.toArray(new String[liveNodes.size()])[random.nextInt(liveNodes.size())];
            } else if (location instanceof String) {
                nodeId = (String) location;
            } else if (location instanceof String[]) {
                for (String choice : (String[]) location) {
                    if (liveNodes.contains(choice)) {
                        nodeId = choice;
                        break;
                    }
                }
                if (nodeId == null) {
                    throw new HyracksException("No satisfiable location found for " + taskAttempt.getTaskAttemptId());
                }
            } else {
                throw new HyracksException("Unknown type of value for " + pLocationExpr + ": " + location + "("
                        + location.getClass() + ")");
            }
        }
        if (nodeId == null) {
            throw new HyracksException("No satisfiable location found for " + taskAttempt.getTaskAttemptId());
        }
        if (!liveNodes.contains(nodeId)) {
            throw HyracksException.create(ErrorCode.NO_SUCH_NODE, nodeId);
        }
        return nodeId;
    }

    private String findTaskLocation(TaskId tid) {
        ActivityId aid = tid.getActivityId();
        ActivityCluster ac = jobRun.getActivityClusterGraph().getActivityMap().get(aid);
        Task[] tasks = getActivityClusterPlan(ac).getActivityPlanMap().get(aid).getTasks();
        List<TaskClusterAttempt> tcAttempts = tasks[tid.getPartition()].getTaskCluster().getAttempts();
        if (tcAttempts == null || tcAttempts.isEmpty()) {
            return null;
        }
        TaskClusterAttempt lastTCA = tcAttempts.get(tcAttempts.size() - 1);
        TaskAttempt ta = lastTCA.getTaskAttempts().get(tid);
        return ta == null ? null : ta.getNodeId();
    }

    private static TaskClusterAttempt findLastTaskClusterAttempt(TaskCluster tc) {
        List<TaskClusterAttempt> attempts = tc.getAttempts();
        if (!attempts.isEmpty()) {
            return attempts.get(attempts.size() - 1);
        }
        return null;
    }

    private void startTasks(Map<String, List<TaskAttemptDescriptor>> taskAttemptMap) throws HyracksException {
        final DeploymentId deploymentId = jobRun.getDeploymentId();
        final JobId jobId = jobRun.getJobId();
        final ActivityClusterGraph acg = jobRun.getActivityClusterGraph();
        final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies =
                new HashMap<>(jobRun.getConnectorPolicyMap());
        INodeManager nodeManager = ccs.getNodeManager();
        try {
            byte[] acgBytes = isDeployed() ? null : JavaSerializationUtils.serialize(acg);
            for (Map.Entry<String, List<TaskAttemptDescriptor>> entry : taskAttemptMap.entrySet()) {
                String nodeId = entry.getKey();
                final List<TaskAttemptDescriptor> taskDescriptors = entry.getValue();
                final NodeControllerState node = nodeManager.getNodeControllerState(nodeId);
                if (node != null) {
                    node.getActiveJobIds().add(jobRun.getJobId());
                    boolean changed = jobRun.getParticipatingNodeIds().add(nodeId);
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Starting: " + taskDescriptors + " at " + entry.getKey());
                    }
                    byte[] jagBytes = changed ? acgBytes : null;
                    node.getNodeController().startTasks(deploymentId, jobId, jagBytes, taskDescriptors,
                            connectorPolicies, jobRun.getFlags(),
                            ccs.createOrGetJobParameterByteStore(jobId).getParameterMap(), deployedJobSpecId,
                            jobRun.getStartTime());
                }
            }
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    public void abortJob(List<Exception> exceptions, IResultCallback<Void> callback) {
        Set<TaskCluster> inProgressTaskClustersCopy = new HashSet<>(inProgressTaskClusters);
        for (TaskCluster tc : inProgressTaskClustersCopy) {
            abortTaskCluster(findLastTaskClusterAttempt(tc), TaskClusterAttempt.TaskClusterStatus.ABORTED);
        }
        assert inProgressTaskClusters.isEmpty();
        ccs.getWorkQueue().schedule(
                new JobCleanupWork(ccs.getJobManager(), jobRun.getJobId(), JobStatus.FAILURE, exceptions, callback));
    }

    private void abortTaskCluster(TaskClusterAttempt tcAttempt,
            TaskClusterAttempt.TaskClusterStatus failedOrAbortedStatus) {
        LOGGER.trace(() -> "Aborting task cluster: " + tcAttempt.getAttempt());
        Set<TaskAttemptId> abortTaskIds = new HashSet<>();
        Map<String, List<TaskAttemptId>> abortTaskAttemptMap = new HashMap<>();
        for (TaskAttempt ta : tcAttempt.getTaskAttempts().values()) {
            TaskAttemptId taId = ta.getTaskAttemptId();
            TaskAttempt.TaskStatus status = ta.getStatus();
            abortTaskIds.add(taId);
            LOGGER.trace(() -> "Checking " + taId + ": " + ta.getStatus());
            if (status == TaskAttempt.TaskStatus.RUNNING || status == TaskAttempt.TaskStatus.COMPLETED) {
                ta.setStatus(TaskAttempt.TaskStatus.ABORTED, null);
                ta.setEndTime(System.currentTimeMillis());
                List<TaskAttemptId> abortTaskAttempts = abortTaskAttemptMap.get(ta.getNodeId());
                if (status == TaskAttempt.TaskStatus.RUNNING && abortTaskAttempts == null) {
                    abortTaskAttempts = new ArrayList<>();
                    abortTaskAttemptMap.put(ta.getNodeId(), abortTaskAttempts);
                }
                if (status == TaskAttempt.TaskStatus.RUNNING) {
                    abortTaskAttempts.add(taId);
                }
            }
        }
        final JobId jobId = jobRun.getJobId();
        LOGGER.trace(() -> "Abort map for job: " + jobId + ": " + abortTaskAttemptMap);
        INodeManager nodeManager = ccs.getNodeManager();
        abortTaskAttemptMap.forEach((key, abortTaskAttempts) -> {
            final NodeControllerState node = nodeManager.getNodeControllerState(key);
            if (node != null) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Aborting: " + abortTaskAttempts + " at " + key);
                }
                try {
                    node.getNodeController().abortTasks(jobId, abortTaskAttempts);
                } catch (Exception e) {
                    LOGGER.log(Level.ERROR, e.getMessage(), e);
                }
            }
        });
        inProgressTaskClusters.remove(tcAttempt.getTaskCluster());
        TaskCluster tc = tcAttempt.getTaskCluster();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        pmm.removeUncommittedPartitions(tc.getProducedPartitions(), abortTaskIds);
        pmm.removePartitionRequests(tc.getRequiredPartitions(), abortTaskIds);

        tcAttempt.setStatus(failedOrAbortedStatus);
        tcAttempt.setEndTime(System.currentTimeMillis());
    }

    private void abortDoomedTaskClusters() throws HyracksException {
        LOGGER.trace("aborting doomed task clusters");
        Set<TaskCluster> doomedTaskClusters = new HashSet<>();
        for (TaskCluster tc : inProgressTaskClusters) {
            // Start search at TCs that produce no outputs (sinks)
            if (tc.getProducedPartitions().isEmpty()) {
                findDoomedTaskClusters(tc, doomedTaskClusters);
            }
        }

        LOGGER.trace(() -> "number of doomed task clusters found = " + doomedTaskClusters.size());
        for (TaskCluster tc : doomedTaskClusters) {
            TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
            if (tca != null) {
                abortTaskCluster(tca, TaskClusterAttempt.TaskClusterStatus.ABORTED);
            }
        }
    }

    private boolean findDoomedTaskClusters(TaskCluster tc, Set<TaskCluster> doomedTaskClusters) {
        if (doomedTaskClusters.contains(tc)) {
            return true;
        }
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
        if (lastAttempt != null) {
            switch (lastAttempt.getStatus()) {
                case ABORTED:
                case FAILED:
                case COMPLETED:
                    return false;
                default:
                    break;
            }
        }
        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap = jobRun.getConnectorPolicyMap();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        boolean doomed = false;
        for (TaskCluster depTC : tc.getDependencyTaskClusters()) {
            if (findDoomedTaskClusters(depTC, doomedTaskClusters)) {
                doomed = true;
            }
        }
        for (PartitionId pid : tc.getRequiredPartitions()) {
            ConnectorDescriptorId cdId = pid.getConnectorDescriptorId();
            IConnectorPolicy cPolicy = connectorPolicyMap.get(cdId);
            PartitionState maxState = pmm.getMaximumAvailableState(pid);
            if ((maxState == null
                    || (cPolicy.consumerWaitsForProducerToFinish() && maxState != PartitionState.COMMITTED))
                    && findDoomedTaskClusters(partitionProducingTaskClusterMap.get(pid), doomedTaskClusters)) {
                doomed = true;
            }
        }
        if (doomed) {
            doomedTaskClusters.add(tc);
        }
        return doomed;
    }

    public void notifyTaskComplete(TaskAttempt ta) {
        try {
            TaskAttemptId taId = ta.getTaskAttemptId();
            TaskCluster tc = ta.getTask().getTaskCluster();
            TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
            if (lastAttempt == null || taId.getAttempt() != lastAttempt.getAttempt()) {
                LOGGER.warn(() -> "Ignoring task complete notification: " + taId + " -- Current last attempt = "
                        + lastAttempt);
                return;
            }
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            if (taStatus != TaskAttempt.TaskStatus.RUNNING) {
                LOGGER.warn(() -> "Spurious task complete notification: " + taId + " Current state = " + taStatus);
                return;
            }
            ta.setStatus(TaskAttempt.TaskStatus.COMPLETED, null);
            ta.setEndTime(System.currentTimeMillis());
            if (lastAttempt.decrementPendingTasksCounter() == 0) {
                lastAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.COMPLETED);
                lastAttempt.setEndTime(System.currentTimeMillis());
                inProgressTaskClusters.remove(tc);
                startRunnableActivityClusters();
            }
        } catch (Exception e) {
            LOGGER.error(() -> "Unexpected failure. Aborting job " + jobRun.getJobId(), e);
            abortJob(Collections.singletonList(e), NoOpCallback.INSTANCE);
        }
    }

    /**
     * Indicates that a single task attempt has encountered a failure.
     *
     * @param ta
     *            Failed Task Attempt
     * @param exceptions
     *            exeptions thrown during the failure
     */
    public void notifyTaskFailure(TaskAttempt ta, List<Exception> exceptions) {
        try {
            LOGGER.debug("Received failure notification for TaskAttempt " + ta.getTaskAttemptId());
            TaskAttemptId taId = ta.getTaskAttemptId();
            TaskCluster tc = ta.getTask().getTaskCluster();
            TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
            if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
                LOGGER.trace(() -> "Marking TaskAttempt " + ta.getTaskAttemptId() + " as failed");
                ta.setStatus(TaskAttempt.TaskStatus.FAILED, exceptions);
                abortTaskCluster(lastAttempt, TaskClusterAttempt.TaskClusterStatus.FAILED);
                abortDoomedTaskClusters();
                int maxReattempts = jobRun.getActivityClusterGraph().getMaxReattempts();
                LOGGER.trace(() -> "Marking TaskAttempt " + ta.getTaskAttemptId()
                        + " as failed and the number of max re-attempts = " + maxReattempts);
                if (lastAttempt.getAttempt() >= maxReattempts || isCancelled()) {
                    LOGGER.debug(() -> "Aborting the job of " + ta.getTaskAttemptId());
                    abortJob(exceptions, NoOpCallback.INSTANCE);
                    return;
                }
                LOGGER.debug(() -> "We will try to start runnable activity clusters of " + ta.getTaskAttemptId());
                startRunnableActivityClusters();
            } else {
                LOGGER.warn(() -> "Ignoring task failure notification: " + taId + " -- Current last attempt = "
                        + lastAttempt);
            }
        } catch (Exception e) {
            abortJob(Collections.singletonList(e), NoOpCallback.INSTANCE);
        }
    }

    /**
     * Indicates that the provided set of nodes have left the cluster.
     *
     * @param deadNodes
     *            - Set of failed nodes
     */
    public void notifyNodeFailures(Collection<String> deadNodes) {
        try {
            jobRun.getPartitionMatchMaker().notifyNodeFailures(deadNodes);
            jobRun.getParticipatingNodeIds().removeAll(deadNodes);
            jobRun.getCleanupPendingNodeIds().removeAll(deadNodes);
            if (jobRun.getPendingStatus() != null && jobRun.getCleanupPendingNodeIds().isEmpty()) {
                IJobManager jobManager = ccs.getJobManager();
                jobManager.finalComplete(jobRun);
                return;
            }
            abortOngoingTaskClusters(ta -> deadNodes.contains(ta.getNodeId()),
                    ta -> HyracksException.create(ErrorCode.NODE_FAILED, ta.getNodeId()));
            startRunnableActivityClusters();
        } catch (Exception e) {
            LOGGER.error(() -> "Unexpected failure. Aborting job " + jobRun.getJobId(), e);
            abortJob(Collections.singletonList(e), NoOpCallback.INSTANCE);
        }
    }

    private interface ITaskFilter {
        boolean directlyMarkAsFailed(TaskAttempt ta);
    }

    private interface IExceptionGenerator {
        HyracksException getException(TaskAttempt ta);
    }

    /**
     * Aborts ongoing task clusters.
     *
     * @param taskFilter,
     *            selects tasks that should be directly marked as failed without doing the aborting RPC.
     * @param exceptionGenerator,
     *            generates an exception for tasks that are directly marked as failed.
     */
    private void abortOngoingTaskClusters(ITaskFilter taskFilter, IExceptionGenerator exceptionGenerator)
            throws HyracksException {
        for (ActivityCluster ac : jobRun.getActivityClusterGraph().getActivityClusterMap().values()) {
            if (!isPlanned(ac)) {
                continue;
            }
            TaskCluster[] taskClusters = getActivityClusterPlan(ac).getTaskClusters();
            if (taskClusters == null) {
                continue;
            }
            for (TaskCluster tc : taskClusters) {
                TaskClusterAttempt lastTaskClusterAttempt = findLastTaskClusterAttempt(tc);
                if (lastTaskClusterAttempt == null || !(lastTaskClusterAttempt
                        .getStatus() == TaskClusterAttempt.TaskClusterStatus.COMPLETED
                        || lastTaskClusterAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING)) {
                    continue;
                }
                boolean abort = false;
                for (TaskAttempt ta : lastTaskClusterAttempt.getTaskAttempts().values()) {
                    assert ta.getStatus() == TaskAttempt.TaskStatus.COMPLETED
                            || ta.getStatus() == TaskAttempt.TaskStatus.RUNNING;
                    if (taskFilter.directlyMarkAsFailed(ta)) {
                        // Directly mark it as fail, without further aborting.
                        ta.setStatus(TaskAttempt.TaskStatus.FAILED,
                                Collections.singletonList(exceptionGenerator.getException(ta)));
                        ta.setEndTime(System.currentTimeMillis());
                        abort = true;
                    }
                }
                if (abort) {
                    abortTaskCluster(lastTaskClusterAttempt, TaskClusterAttempt.TaskClusterStatus.ABORTED);
                }
            }
            abortDoomedTaskClusters();
        }
    }

    // Returns whether the job has been cancelled.
    private boolean isCancelled() {
        return cancelled;
    }

}
