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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.ActivityCluster;
import edu.uci.ics.hyracks.api.job.ActivityClusterGraph;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.ActivityClusterPlan;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.Task;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterAttempt;
import edu.uci.ics.hyracks.control.cc.partitions.PartitionMatchMaker;
import edu.uci.ics.hyracks.control.cc.work.JobCleanupWork;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;

public class JobScheduler {
    private static final Logger LOGGER = Logger.getLogger(JobScheduler.class.getName());

    private final ClusterControllerService ccs;

    private final JobRun jobRun;

    private final PartitionConstraintSolver solver;

    private final Map<PartitionId, TaskCluster> partitionProducingTaskClusterMap;

    private final Set<TaskCluster> inProgressTaskClusters;

    public JobScheduler(ClusterControllerService ccs, JobRun jobRun, Collection<Constraint> constraints) {
        this.ccs = ccs;
        this.jobRun = jobRun;
        solver = new PartitionConstraintSolver();
        partitionProducingTaskClusterMap = new HashMap<PartitionId, TaskCluster>();
        inProgressTaskClusters = new HashSet<TaskCluster>();
        solver.addConstraints(constraints);
    }

    public JobRun getJobRun() {
        return jobRun;
    }

    public PartitionConstraintSolver getSolver() {
        return solver;
    }

    public void startJob() throws HyracksException {
        startRunnableActivityClusters();
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
                    if (tc.getProducedPartitions().isEmpty()) {
                        TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
                        if (tca == null || tca.getStatus() != TaskClusterAttempt.TaskClusterStatus.COMPLETED) {
                            tcRootsComplete = false;
                            break;
                        }
                    }
                }
                if (!tcRootsComplete) {
                    depsComplete = false;
                    findRunnableTaskClusterRoots(frontier, depAC);
                }
            }
        }
        if (depsComplete) {
            if (!isPlanned(candidate)) {
                ActivityClusterPlanner acp = new ActivityClusterPlanner(this);
                ActivityClusterPlan acPlan = acp.planActivityCluster(candidate);
                jobRun.getActivityClusterPlanMap().put(candidate.getId(), acPlan);
                partitionProducingTaskClusterMap.putAll(acp.getPartitionProducingTaskClusterMap());
            }
            for (TaskCluster tc : getActivityClusterPlan(candidate).getTaskClusters()) {
                if (tc.getProducedPartitions().isEmpty()) {
                    TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
                    if (tca == null || tca.getStatus() != TaskClusterAttempt.TaskClusterStatus.COMPLETED) {
                        frontier.add(tc);
                    }
                }
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
        Set<TaskCluster> taskClusterRoots = new HashSet<TaskCluster>();
        findRunnableTaskClusterRoots(taskClusterRoots, jobRun.getActivityClusterGraph().getActivityClusterMap()
                .values());
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Runnable TC roots: " + taskClusterRoots + ", inProgressTaskClusters: "
                    + inProgressTaskClusters);
        }
        if (taskClusterRoots.isEmpty() && inProgressTaskClusters.isEmpty()) {
            ccs.getWorkQueue().schedule(new JobCleanupWork(ccs, jobRun.getJobId(), JobStatus.TERMINATED, null));
            return;
        }
        startRunnableTaskClusters(taskClusterRoots);
    }

    private void startRunnableTaskClusters(Set<TaskCluster> tcRoots) throws HyracksException {
        Map<TaskCluster, Runnability> runnabilityMap = new HashMap<TaskCluster, Runnability>();
        for (TaskCluster tc : tcRoots) {
            assignRunnabilityRank(tc, runnabilityMap);
        }

        PriorityQueue<RankedRunnableTaskCluster> queue = new PriorityQueue<RankedRunnableTaskCluster>();
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
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Ranked TCs: " + queue);
        }

        Map<String, List<TaskAttemptDescriptor>> taskAttemptMap = new HashMap<String, List<TaskAttemptDescriptor>>();
        for (RankedRunnableTaskCluster rrtc : queue) {
            TaskCluster tc = rrtc.getTaskCluster();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Found runnable TC: " + tc);
                List<TaskClusterAttempt> attempts = tc.getAttempts();
                LOGGER.fine("Attempts so far:" + attempts.size());
                for (TaskClusterAttempt tcAttempt : attempts) {
                    LOGGER.fine("Status: " + tcAttempt.getStatus());
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
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Computing runnability: " + goal);
        }
        if (runnabilityMap.containsKey(goal)) {
            return runnabilityMap.get(goal);
        }
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(goal);
        if (lastAttempt != null) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Last Attempt Status: " + lastAttempt.getStatus());
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
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Inspecting required partition: " + pid);
            }
            Runnability runnability;
            ConnectorDescriptorId cdId = pid.getConnectorDescriptorId();
            IConnectorPolicy cPolicy = connectorPolicyMap.get(cdId);
            PartitionState maxState = pmm.getMaximumAvailableState(pid);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Policy: " + cPolicy + " maxState: " + maxState);
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
                }
            }
            aggregateRunnability = Runnability.getWorstCase(aggregateRunnability, runnability);
            if (aggregateRunnability.getTag() == Runnability.Tag.NOT_RUNNABLE) {
                // already not runnable -- cannot get better. bail.
                break;
            }
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("aggregateRunnability: " + aggregateRunnability);
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
        Map<TaskId, TaskAttempt> taskAttempts = new HashMap<TaskId, TaskAttempt>();
        Map<TaskId, LValueConstraintExpression> locationMap = new HashMap<TaskId, LValueConstraintExpression>();
        for (int i = 0; i < tasks.length; ++i) {
            Task ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = new TaskAttempt(tcAttempt, new TaskAttemptId(new TaskId(tid.getActivityId(),
                    tid.getPartition()), attempts), ts);
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
                tads = new ArrayList<TaskAttemptDescriptor>();
                taskAttemptMap.put(nodeId, tads);
            }
            ActivityPartitionDetails apd = ts.getActivityPlan().getActivityPartitionDetails();
            TaskAttemptDescriptor tad = new TaskAttemptDescriptor(taskAttempt.getTaskAttemptId(),
                    apd.getPartitionCount(), apd.getInputPartitionCounts(), apd.getOutputPartitionCounts());
            tads.add(tad);
        }
        tcAttempt.initializePendingTaskCounter();
        tcAttempts.add(tcAttempt);

        /* TODO - Further improvement for reducing messages -- not yet complete.
        for (Map.Entry<String, List<TaskAttemptDescriptor>> e : taskAttemptMap.entrySet()) {
            List<TaskAttemptDescriptor> tads = e.getValue();
            for (TaskAttemptDescriptor tad : tads) {
                TaskId tid = tad.getTaskAttemptId().getTaskId();
                ActivityId aid = tid.getActivityId();
                List<IConnectorDescriptor> inConnectors = jag.getActivityInputConnectorDescriptors(aid);
                int[] inPartitionCounts = tad.getInputPartitionCounts();
                NetworkAddress[][] partitionLocations = new NetworkAddress[inPartitionCounts.length][];
                for (int i = 0; i < inPartitionCounts.length; ++i) {
                    ConnectorDescriptorId cdId = inConnectors.get(i).getConnectorId();
                    ActivityId producerAid = jag.getProducerActivity(cdId);
                    partitionLocations[i] = new NetworkAddress[inPartitionCounts[i]];
                    for (int j = 0; j < inPartitionCounts[i]; ++j) {
                        TaskId producerTaskId = new TaskId(producerAid, j);
                        String nodeId = findTaskLocation(producerTaskId);
                        partitionLocations[i][j] = ccs.getNodeMap().get(nodeId).getDataPort();
                    }
                }
                tad.setInputPartitionLocations(partitionLocations);
            }
        }
        */

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
        Set<String> liveNodes = ccs.getNodeMap().keySet();
        if (nodeId == null) {
            LValueConstraintExpression pLocationExpr = locationMap.get(tid);
            Object location = solver.getValue(pLocationExpr);
            if (location == null) {
                // pick any
                nodeId = liveNodes.toArray(new String[liveNodes.size()])[Math.abs(new Random().nextInt())
                        % liveNodes.size()];
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
            throw new HyracksException("Node " + nodeId + " not live");
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
        final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = new HashMap<ConnectorDescriptorId, IConnectorPolicy>(
                jobRun.getConnectorPolicyMap());
        for (Map.Entry<String, List<TaskAttemptDescriptor>> entry : taskAttemptMap.entrySet()) {
            String nodeId = entry.getKey();
            final List<TaskAttemptDescriptor> taskDescriptors = entry.getValue();
            final NodeControllerState node = ccs.getNodeMap().get(nodeId);
            if (node != null) {
                node.getActiveJobIds().add(jobRun.getJobId());
                boolean changed = jobRun.getParticipatingNodeIds().add(nodeId);
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Starting: " + taskDescriptors + " at " + entry.getKey());
                }
                try {
                    byte[] jagBytes = changed ? JavaSerializationUtils.serialize(acg) : null;
                    node.getNodeController().startTasks(deploymentId, jobId, jagBytes, taskDescriptors,
                            connectorPolicies, jobRun.getFlags());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void abortJob(List<Exception> exceptions) {
        Set<TaskCluster> inProgressTaskClustersCopy = new HashSet<TaskCluster>(inProgressTaskClusters);
        for (TaskCluster tc : inProgressTaskClustersCopy) {
            abortTaskCluster(findLastTaskClusterAttempt(tc), TaskClusterAttempt.TaskClusterStatus.ABORTED);
        }
        assert inProgressTaskClusters.isEmpty();
        ccs.getWorkQueue().schedule(new JobCleanupWork(ccs, jobRun.getJobId(), JobStatus.FAILURE, exceptions));
    }

    private void abortTaskCluster(TaskClusterAttempt tcAttempt,
            TaskClusterAttempt.TaskClusterStatus failedOrAbortedStatus) {
        LOGGER.fine("Aborting task cluster: " + tcAttempt.getAttempt());
        Set<TaskAttemptId> abortTaskIds = new HashSet<TaskAttemptId>();
        Map<String, List<TaskAttemptId>> abortTaskAttemptMap = new HashMap<String, List<TaskAttemptId>>();
        for (TaskAttempt ta : tcAttempt.getTaskAttempts().values()) {
            TaskAttemptId taId = ta.getTaskAttemptId();
            TaskAttempt.TaskStatus status = ta.getStatus();
            abortTaskIds.add(taId);
            LOGGER.fine("Checking " + taId + ": " + ta.getStatus());
            if (status == TaskAttempt.TaskStatus.RUNNING || status == TaskAttempt.TaskStatus.COMPLETED) {
                ta.setStatus(TaskAttempt.TaskStatus.ABORTED, null);
                ta.setEndTime(System.currentTimeMillis());
                List<TaskAttemptId> abortTaskAttempts = abortTaskAttemptMap.get(ta.getNodeId());
                if (status == TaskAttempt.TaskStatus.RUNNING && abortTaskAttempts == null) {
                    abortTaskAttempts = new ArrayList<TaskAttemptId>();
                    abortTaskAttemptMap.put(ta.getNodeId(), abortTaskAttempts);
                }
                if (status == TaskAttempt.TaskStatus.RUNNING) {
                    abortTaskAttempts.add(taId);
                }
            }
        }
        final JobId jobId = jobRun.getJobId();
        LOGGER.fine("Abort map for job: " + jobId + ": " + abortTaskAttemptMap);
        for (Map.Entry<String, List<TaskAttemptId>> entry : abortTaskAttemptMap.entrySet()) {
            final NodeControllerState node = ccs.getNodeMap().get(entry.getKey());
            final List<TaskAttemptId> abortTaskAttempts = entry.getValue();
            if (node != null) {
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine("Aborting: " + abortTaskAttempts + " at " + entry.getKey());
                }
                try {
                    node.getNodeController().abortTasks(jobId, abortTaskAttempts);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        inProgressTaskClusters.remove(tcAttempt.getTaskCluster());
        TaskCluster tc = tcAttempt.getTaskCluster();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        pmm.removeUncommittedPartitions(tc.getProducedPartitions(), abortTaskIds);
        pmm.removePartitionRequests(tc.getRequiredPartitions(), abortTaskIds);

        tcAttempt.setStatus(failedOrAbortedStatus);
        tcAttempt.setEndTime(System.currentTimeMillis());
    }

    private void abortDoomedTaskClusters() throws HyracksException {
        Set<TaskCluster> doomedTaskClusters = new HashSet<TaskCluster>();
        for (TaskCluster tc : inProgressTaskClusters) {
            // Start search at TCs that produce no outputs (sinks)
            if (tc.getProducedPartitions().isEmpty()) {
                findDoomedTaskClusters(tc, doomedTaskClusters);
            }
        }

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
                    return true;

                case COMPLETED:
                    return false;
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
            if (maxState == null
                    || (cPolicy.consumerWaitsForProducerToFinish() && maxState != PartitionState.COMMITTED)) {
                if (findDoomedTaskClusters(partitionProducingTaskClusterMap.get(pid), doomedTaskClusters)) {
                    doomed = true;
                }
            }
        }
        if (doomed) {
            doomedTaskClusters.add(tc);
        }
        return doomed;
    }

    public void notifyTaskComplete(TaskAttempt ta) throws HyracksException {
        TaskAttemptId taId = ta.getTaskAttemptId();
        TaskCluster tc = ta.getTask().getTaskCluster();
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
        if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.COMPLETED, null);
                ta.setEndTime(System.currentTimeMillis());
                if (lastAttempt.decrementPendingTasksCounter() == 0) {
                    lastAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.COMPLETED);
                    lastAttempt.setEndTime(System.currentTimeMillis());
                    inProgressTaskClusters.remove(tc);
                    startRunnableActivityClusters();
                }
            } else {
                LOGGER.warning("Spurious task complete notification: " + taId + " Current state = " + taStatus);
            }
        } else {
            LOGGER.warning("Ignoring task complete notification: " + taId + " -- Current last attempt = " + lastAttempt);
        }
    }

    /**
     * Indicates that a single task attempt has encountered a failure.
     * 
     * @param ta
     *            - Failed Task Attempt
     * @param ac
     *            - Activity Cluster that owns this Task
     * @param details
     *            - Cause of the failure
     */
    public void notifyTaskFailure(TaskAttempt ta, ActivityCluster ac, List<Exception> exceptions) {
        try {
            LOGGER.fine("Received failure notification for TaskAttempt " + ta.getTaskAttemptId());
            TaskAttemptId taId = ta.getTaskAttemptId();
            TaskCluster tc = ta.getTask().getTaskCluster();
            TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
            if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
                LOGGER.fine("Marking TaskAttempt " + ta.getTaskAttemptId() + " as failed");
                ta.setStatus(TaskAttempt.TaskStatus.FAILED, exceptions);
                abortTaskCluster(lastAttempt, TaskClusterAttempt.TaskClusterStatus.FAILED);
                abortDoomedTaskClusters();
                if (lastAttempt.getAttempt() >= jobRun.getActivityClusterGraph().getMaxReattempts()) {
                    abortJob(exceptions);
                    return;
                }
                startRunnableActivityClusters();
            } else {
                LOGGER.warning("Ignoring task failure notification: " + taId + " -- Current last attempt = "
                        + lastAttempt);
            }
        } catch (Exception e) {
            abortJob(Collections.singletonList(e));
        }
    }

    /**
     * Indicates that the provided set of nodes have left the cluster.
     * 
     * @param deadNodes
     *            - Set of failed nodes
     */
    public void notifyNodeFailures(Set<String> deadNodes) {
        try {
            jobRun.getPartitionMatchMaker().notifyNodeFailures(deadNodes);
            jobRun.getParticipatingNodeIds().removeAll(deadNodes);
            jobRun.getCleanupPendingNodeIds().removeAll(deadNodes);
            if (jobRun.getPendingStatus() != null && jobRun.getCleanupPendingNodeIds().isEmpty()) {
                finishJob(jobRun);
                return;
            }
            for (ActivityCluster ac : jobRun.getActivityClusterGraph().getActivityClusterMap().values()) {
                if (isPlanned(ac)) {
                    TaskCluster[] taskClusters = getActivityClusterPlan(ac).getTaskClusters();
                    if (taskClusters != null) {
                        for (TaskCluster tc : taskClusters) {
                            TaskClusterAttempt lastTaskClusterAttempt = findLastTaskClusterAttempt(tc);
                            if (lastTaskClusterAttempt != null
                                    && (lastTaskClusterAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.COMPLETED || lastTaskClusterAttempt
                                            .getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING)) {
                                boolean abort = false;
                                for (TaskAttempt ta : lastTaskClusterAttempt.getTaskAttempts().values()) {
                                    assert (ta.getStatus() == TaskAttempt.TaskStatus.COMPLETED || ta.getStatus() == TaskAttempt.TaskStatus.RUNNING);
                                    if (deadNodes.contains(ta.getNodeId())) {
                                        ta.setStatus(
                                                TaskAttempt.TaskStatus.FAILED,
                                                Collections.singletonList(new Exception("Node " + ta.getNodeId()
                                                        + " failed")));
                                        ta.setEndTime(System.currentTimeMillis());
                                        abort = true;
                                    }
                                }
                                if (abort) {
                                    abortTaskCluster(lastTaskClusterAttempt,
                                            TaskClusterAttempt.TaskClusterStatus.ABORTED);
                                }
                            }
                        }
                        abortDoomedTaskClusters();
                    }
                }
            }
            startRunnableActivityClusters();
        } catch (Exception e) {
            abortJob(Collections.singletonList(e));
        }
    }

    private void finishJob(final JobRun run) {
        JobId jobId = run.getJobId();
        CCApplicationContext appCtx = ccs.getApplicationContext();
        if (appCtx != null) {
            try {
                appCtx.notifyJobFinish(jobId);
            } catch (HyracksException e) {
                e.printStackTrace();
            }
        }
        run.setStatus(run.getPendingStatus(), run.getPendingExceptions());
        ccs.getActiveRunMap().remove(jobId);
        ccs.getRunMapArchive().put(jobId, run);
        ccs.getRunHistory().put(jobId, run.getExceptions());
        try {
            ccs.getJobLogFile().log(createJobLogObject(run));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JSONObject createJobLogObject(final JobRun run) {
        JSONObject jobLogObject = new JSONObject();
        try {
            ActivityClusterGraph acg = run.getActivityClusterGraph();
            jobLogObject.put("activity-cluster-graph", acg.toJSON());
            jobLogObject.put("job-run", run.toJSON());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return jobLogObject;
    }
}