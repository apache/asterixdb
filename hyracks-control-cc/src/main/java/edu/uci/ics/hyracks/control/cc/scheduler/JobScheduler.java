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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.IConstraintAcceptor;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.IConnectorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.IOperatorDescriptorVisitor;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.PlanUtils;
import edu.uci.ics.hyracks.control.cc.job.Task;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterAttempt;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobCleanupEvent;
import edu.uci.ics.hyracks.control.cc.partitions.PartitionMatchMaker;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;

public class JobScheduler {
    private static final Logger LOGGER = Logger.getLogger(JobScheduler.class.getName());

    private final ClusterControllerService ccs;

    private final JobRun jobRun;

    private final Set<ActivityCluster> completedClusters;

    private final Set<ActivityCluster> inProgressClusters;

    private final PartitionConstraintSolver solver;

    private Set<ActivityCluster> rootActivityClusters;

    public JobScheduler(ClusterControllerService ccs, JobRun jobRun) {
        this.ccs = ccs;
        this.jobRun = jobRun;
        completedClusters = new HashSet<ActivityCluster>();
        inProgressClusters = new HashSet<ActivityCluster>();
        solver = new PartitionConstraintSolver();
    }

    public void startJob() throws HyracksException {
        analyze();
        startRunnableActivityClusters();
    }

    private void analyze() throws HyracksException {
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

        ActivityClusterGraphBuilder acgb = new ActivityClusterGraphBuilder(jobRun);
        rootActivityClusters = acgb.inferActivityClusters(jag);
    }

    private void findPrerequisiteActivities(Set<ActivityId> prereqs, ActivityCluster ac) {
        TaskCluster[] taskClusters = ac.getTaskClusters();
        if (taskClusters == null) {
            JobActivityGraph jag = jobRun.getJobActivityGraph();
            for (ActivityId aid : ac.getActivities()) {
                Set<ActivityId> deps = jag.getBlocked2BlockerMap().get(aid);
                prereqs.addAll(deps);
            }
        } else {

        }
    }

    private void findRunnableActivityClusters(Set<ActivityCluster> frontier, Set<ActivityCluster> roots) {
        for (ActivityCluster root : roots) {
            findRunnableActivityClusters(frontier, root);
        }
    }

    private void findRunnableActivityClusters(Set<ActivityCluster> frontier, ActivityCluster candidate) {
        if (frontier.contains(candidate) || inProgressClusters.contains(candidate)) {
            return;
        }
        boolean depsComplete = true;
        Set<ActivityId> prereqs = new HashSet<ActivityId>();
        findPrerequisiteActivities(prereqs, candidate);
        Map<ActivityCluster, Set<ActivityId>> prereqACMap = new HashMap<ActivityCluster, Set<ActivityId>>();
        for (ActivityId aid : prereqs) {
            ActivityCluster ac = jobRun.getActivityClusterMap().get(aid);
            Set<ActivityId> pSet = prereqACMap.get(ac);
            if (pSet == null) {
                pSet = new HashSet<ActivityId>();
                prereqACMap.put(ac, pSet);
            }
            pSet.add(aid);
        }
        for (ActivityCluster depAC : candidate.getDependencies()) {
            if (!completedClusters.contains(depAC)) {
                depsComplete = false;
                findRunnableActivityClusters(frontier, depAC);
            } else {
                Set<ActivityId> pSet = prereqACMap.get(depAC);
                if (pSet != null) {

                }
                // TODO
            }
        }
        if (depsComplete) {
            if (runnable && candidate != rootActivityCluster) {
                frontier.add(candidate);
            }

        }
    }

    private void startRunnableActivityClusters() throws HyracksException {
        Set<ActivityCluster> runnableClusters = new HashSet<ActivityCluster>();
        findRunnableActivityClusters(runnableClusters, rootActivityClusters);
        if (runnableClusters.isEmpty() && inProgressClusters.isEmpty()) {
            ccs.getJobQueue().schedule(new JobCleanupEvent(ccs, jobRun.getJobId(), JobStatus.TERMINATED, null));
            return;
        }
        for (ActivityCluster ac : runnableClusters) {
            inProgressClusters.add(ac);
            TaskClusterBuilder tcb = new TaskClusterBuilder(jobRun, solver);
            tcb.buildTaskClusters(ac);
            startRunnableTaskClusters(ac);
        }
    }

    private void abortActivityCluster(ActivityCluster ac) {
        TaskCluster[] taskClusters = ac.getTaskClusters();
        for (TaskCluster tc : taskClusters) {
            List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
            if (!tcAttempts.isEmpty()) {
                TaskClusterAttempt tcAttempt = tcAttempts.get(tcAttempts.size() - 1);
                if (tcAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING) {
                    abortTaskCluster(tcAttempt, ac);
                    tcAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.ABORTED);
                }
            }
        }
        inProgressClusters.remove(ac);
    }

    private void assignTaskLocations(ActivityCluster ac, TaskCluster tc,
            Map<String, List<TaskAttemptDescriptor>> taskAttemptMap) throws HyracksException {
        JobRun jobRun = ac.getJobRun();
        JobActivityGraph jag = jobRun.getJobActivityGraph();
        Task[] tasks = tc.getTasks();
        List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
        int attempts = tcAttempts.size();
        TaskClusterAttempt tcAttempt = new TaskClusterAttempt(tc, attempts);
        TaskAttempt[] taskAttempts = new TaskAttempt[tasks.length];
        Map<TaskId, LValueConstraintExpression> locationMap = new HashMap<TaskId, LValueConstraintExpression>();
        for (int i = 0; i < tasks.length; ++i) {
            Task ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = new TaskAttempt(tcAttempt, new TaskAttemptId(new TaskId(tid.getActivityId(),
                    tid.getPartition()), attempts), ts);
            taskAttempt.setStatus(TaskAttempt.TaskStatus.INITIALIZED, null);
            locationMap.put(tid,
                    new PartitionLocationExpression(tid.getActivityId().getOperatorDescriptorId(), tid.getPartition()));
            taskAttempts[i] = taskAttempt;
        }
        tcAttempt.setTaskAttempts(taskAttempts);
        solver.solve(locationMap.values());
        for (int i = 0; i < tasks.length; ++i) {
            Task ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = taskAttempts[i];
            ActivityId aid = tid.getActivityId();
            Set<ActivityId> blockers = jag.getBlocked2BlockerMap().get(aid);
            String nodeId = null;
            if (blockers != null) {
                for (ActivityId blocker : blockers) {
                    nodeId = findLocationOfBlocker(jobRun, jag, new TaskId(blocker, tid.getPartition()));
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
                        throw new HyracksException("No satisfiable location found for "
                                + taskAttempt.getTaskAttemptId());
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
            taskAttempt.setNodeId(nodeId);
            taskAttempt.setStatus(TaskAttempt.TaskStatus.RUNNING, null);
            List<TaskAttemptDescriptor> tads = taskAttemptMap.get(nodeId);
            if (tads == null) {
                tads = new ArrayList<TaskAttemptDescriptor>();
                taskAttemptMap.put(nodeId, tads);
            }
            ActivityPartitionDetails apd = ts.getActivityPartitionDetails();
            tads.add(new TaskAttemptDescriptor(taskAttempt.getTaskAttemptId(), apd.getPartitionCount(), apd
                    .getInputPartitionCounts(), apd.getOutputPartitionCounts()));
        }
        tcAttempt.initializePendingTaskCounter();
        tcAttempts.add(tcAttempt);
        tcAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.RUNNING);
        ac.getInProgressTaskClusters().add(tc);
    }

    private static String findLocationOfBlocker(JobRun jobRun, JobActivityGraph jag, TaskId tid) {
        ActivityId blockerAID = tid.getActivityId();
        ActivityCluster blockerAC = jobRun.getActivityClusterMap().get(blockerAID);
        Task[] blockerTasks = blockerAC.getTaskMap().get(blockerAID);
        List<TaskClusterAttempt> tcAttempts = blockerTasks[tid.getPartition()].getTaskCluster().getAttempts();
        if (tcAttempts == null || tcAttempts.isEmpty()) {
            return null;
        }
        TaskClusterAttempt lastTCA = tcAttempts.get(tcAttempts.size() - 1);
        for (TaskAttempt ta : lastTCA.getTaskAttempts()) {
            TaskId blockerTID = ta.getTaskAttemptId().getTaskId();
            if (tid.equals(blockerTID)) {
                return ta.getNodeId();
            }
        }
        return null;
    }

    private static TaskClusterAttempt findLastTaskClusterAttempt(TaskCluster tc) {
        List<TaskClusterAttempt> attempts = tc.getAttempts();
        if (!attempts.isEmpty()) {
            return attempts.get(attempts.size() - 1);
        }
        return null;
    }

    public void notifyTaskComplete(TaskAttempt ta, ActivityCluster ac) throws HyracksException {
        TaskAttemptId taId = ta.getTaskAttemptId();
        TaskCluster tc = ta.getTaskState().getTaskCluster();
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
        if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.COMPLETED, null);
                if (lastAttempt.decrementPendingTasksCounter() == 0) {
                    lastAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.COMPLETED);
                    ac.getInProgressTaskClusters().remove(tc);
                    startRunnableTaskClusters(ac);
                }
            } else {
                LOGGER.warning("Spurious task complete notification: " + taId + " Current state = " + taStatus);
            }
        } else {
            LOGGER.warning("Ignoring task complete notification: " + taId + " -- Current last attempt = " + lastAttempt);
        }
    }

    private void startRunnableTaskClusters(ActivityCluster ac) throws HyracksException {
        Map<TaskCluster, Runnability> runnabilityMap = computeRunnabilityRanks(ac);

        PriorityQueue<RankedRunnableTaskCluster> queue = new PriorityQueue<RankedRunnableTaskCluster>();
        for (Map.Entry<TaskCluster, Runnability> e : runnabilityMap.entrySet()) {
            TaskCluster tc = e.getKey();
            Runnability runnability = e.getValue();
            assert runnability.getTag() != Runnability.Tag.UNSATISFIED_PREREQUISITES;
            if (runnability.getTag() != Runnability.Tag.RUNNABLE) {
                continue;
            }
            int priority = runnability.getPriority();
            if (priority >= 0 && priority < Integer.MAX_VALUE) {
                queue.add(new RankedRunnableTaskCluster(priority, tc));
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ranked TCs: " + queue);
        }

        Map<String, List<TaskAttemptDescriptor>> taskAttemptMap = new HashMap<String, List<TaskAttemptDescriptor>>();
        for (RankedRunnableTaskCluster rrtc : queue) {
            TaskCluster tc = rrtc.getTaskCluster();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Found runnable TC: " + tc);
                List<TaskClusterAttempt> attempts = tc.getAttempts();
                LOGGER.info("Attempts so far:" + attempts.size());
                for (TaskClusterAttempt tcAttempt : attempts) {
                    LOGGER.info("Status: " + tcAttempt.getStatus());
                }
            }
            assignTaskLocations(ac, tc, taskAttemptMap);
        }

        if (taskAttemptMap.isEmpty()) {
            if (ac.getInProgressTaskClusters().isEmpty()) {
                completedClusters.add(ac);
                inProgressClusters.remove(ac);
                startRunnableActivityClusters();
            }
            return;
        }

        startTasks(ac, taskAttemptMap);
    }

    private void abortJob(Exception exception) {
        for (ActivityCluster ac : inProgressClusters) {
            abortActivityCluster(ac);
        }
        jobRun.setStatus(JobStatus.FAILURE, exception);
    }

    private Map<TaskCluster, Runnability> computeRunnabilityRanks(ActivityCluster ac) {
        TaskCluster[] taskClusters = ac.getTaskClusters();

        Map<TaskCluster, Runnability> runnabilityMap = new HashMap<TaskCluster, Runnability>();
        for (TaskCluster tc : taskClusters) {
            // Start search at TCs that produce no outputs (sinks)
            if (tc.getProducedPartitions().isEmpty()) {
                assignRunnabilityRank(tc, runnabilityMap, ac);
            }
        }
        return runnabilityMap;
    }

    /*
     * Runnability rank has the following semantics
     * Runnability(Runnable TaskCluster depending on completed TaskClusters) = {RUNNABLE, 0}
     * Runnability(Runnable TaskCluster) = max(Rank(Dependent TaskClusters)) + 1
     * Runnability(Non-schedulable TaskCluster) = {NOT_RUNNABLE, _} 
     */
    private Runnability assignRunnabilityRank(TaskCluster goal, Map<TaskCluster, Runnability> runnabilityMap,
            ActivityCluster ac) {
        if (runnabilityMap.containsKey(goal)) {
            return runnabilityMap.get(goal);
        }
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(goal);
        if (lastAttempt != null) {
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
        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap = ac.getConnectorPolicyMap();
        JobRun jobRun = ac.getJobRun();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        Runnability aggregateRunnability = new Runnability(Runnability.Tag.RUNNABLE, 0);
        for (PartitionId pid : goal.getRequiredPartitions()) {
            Runnability runnability;
            ConnectorDescriptorId cdId = pid.getConnectorDescriptorId();
            IConnectorPolicy cPolicy = connectorPolicyMap.get(cdId);
            PartitionState maxState = pmm.getMaximumAvailableState(pid);
            if (PartitionState.COMMITTED.equals(maxState)) {
                runnability = new Runnability(Runnability.Tag.RUNNABLE, 0);
            } else if (PartitionState.STARTED.equals(maxState) && !cPolicy.consumerWaitsForProducerToFinish()) {
                runnability = new Runnability(Runnability.Tag.RUNNABLE, 1);
            } else {
                runnability = assignRunnabilityRank(ac.getPartitionProducingTaskClusterMap().get(pid), runnabilityMap,
                        ac);
                if (runnability.getTag() == Runnability.Tag.RUNNABLE && runnability.getPriority() > 0
                        && cPolicy.consumerWaitsForProducerToFinish()) {
                    runnability = new Runnability(Runnability.Tag.NOT_RUNNABLE, Integer.MAX_VALUE);
                }
            }
            aggregateRunnability = Runnability.getWorstCase(aggregateRunnability, runnability);
        }
        runnabilityMap.put(goal, aggregateRunnability);
        return aggregateRunnability;
    }

    private void startTasks(ActivityCluster ac, Map<String, List<TaskAttemptDescriptor>> taskAttemptMap)
            throws HyracksException {
        Executor executor = ccs.getExecutor();
        JobRun jobRun = ac.getJobRun();
        final UUID jobId = jobRun.getJobId();
        final JobActivityGraph jag = jobRun.getJobActivityGraph();
        final String appName = jag.getApplicationName();
        final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicies = ac.getConnectorPolicyMap();
        for (Map.Entry<String, List<TaskAttemptDescriptor>> e : taskAttemptMap.entrySet()) {
            String nodeId = e.getKey();
            final List<TaskAttemptDescriptor> taskDescriptors = e.getValue();
            final NodeControllerState node = ccs.getNodeMap().get(nodeId);
            if (node != null) {
                node.getActiveJobIds().add(jobRun.getJobId());
                jobRun.getParticipatingNodeIds().add(nodeId);
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            node.getNodeController().startTasks(appName, jobId, JavaSerializationUtils.serialize(jag),
                                    taskDescriptors, connectorPolicies, null);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }

    private void abortTaskCluster(TaskClusterAttempt tcAttempt, ActivityCluster ac) {
        Set<TaskAttemptId> abortTaskIds = new HashSet<TaskAttemptId>();
        Map<String, List<TaskAttemptId>> abortTaskAttemptMap = new HashMap<String, List<TaskAttemptId>>();
        for (TaskAttempt ta : tcAttempt.getTaskAttempts()) {
            TaskAttemptId taId = ta.getTaskAttemptId();
            abortTaskIds.add(taId);
            if (ta.getStatus() == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.ABORTED, null);
                List<TaskAttemptId> abortTaskAttempts = abortTaskAttemptMap.get(ta.getNodeId());
                if (abortTaskAttempts == null) {
                    abortTaskAttempts = new ArrayList<TaskAttemptId>();
                    abortTaskAttemptMap.put(ta.getNodeId(), abortTaskAttempts);
                }
                abortTaskAttempts.add(taId);
            }
        }
        JobRun jobRun = ac.getJobRun();
        final UUID jobId = jobRun.getJobId();
        for (Map.Entry<String, List<TaskAttemptId>> e : abortTaskAttemptMap.entrySet()) {
            final NodeControllerState node = ccs.getNodeMap().get(e.getKey());
            final List<TaskAttemptId> abortTaskAttempts = e.getValue();
            if (node != null) {
                LOGGER.info("Aborting: " + abortTaskAttempts + " at " + e.getKey());
                ccs.getExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            node.getNodeController().abortTasks(jobId, abortTaskAttempts);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
        ac.getInProgressTaskClusters().remove(tcAttempt.getTaskCluster());
        TaskCluster tc = tcAttempt.getTaskCluster();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        pmm.removeUncommittedPartitions(tc.getProducedPartitions(), abortTaskIds);
        pmm.removePartitionRequests(tc.getRequiredPartitions(), abortTaskIds);
    }

    private void abortDoomedTaskClusters(ActivityCluster ac) throws HyracksException {
        TaskCluster[] taskClusters = ac.getTaskClusters();

        Set<TaskCluster> doomedTaskClusters = new HashSet<TaskCluster>();
        for (TaskCluster tc : taskClusters) {
            // Start search at TCs that produce no outputs (sinks)
            if (tc.getProducedPartitions().isEmpty()) {
                findDoomedTaskClusters(tc, ac, doomedTaskClusters);
            }
        }

        for (TaskCluster tc : doomedTaskClusters) {
            TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
            abortTaskCluster(tca, ac);
            tca.setStatus(TaskClusterAttempt.TaskClusterStatus.ABORTED);
        }
    }

    private boolean findDoomedTaskClusters(TaskCluster tc, ActivityCluster ac, Set<TaskCluster> doomedTaskClusters) {
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
        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap = ac.getConnectorPolicyMap();
        JobRun jobRun = ac.getJobRun();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        boolean doomed = false;
        for (PartitionId pid : tc.getRequiredPartitions()) {
            ConnectorDescriptorId cdId = pid.getConnectorDescriptorId();
            IConnectorPolicy cPolicy = connectorPolicyMap.get(cdId);
            PartitionState maxState = pmm.getMaximumAvailableState(pid);
            if (maxState == null
                    || (cPolicy.consumerWaitsForProducerToFinish() && maxState != PartitionState.COMMITTED)) {
                if (findDoomedTaskClusters(ac.getPartitionProducingTaskClusterMap().get(pid), ac, doomedTaskClusters)) {
                    doomed = true;
                }
            }
        }
        if (doomed) {
            doomedTaskClusters.add(tc);
        }
        return doomed;
    }

    /**
     * Indicates that a single task attempt has encountered a failure.
     * 
     * @param ta
     *            - Failed Task Attempt
     * @param ac
     *            - Activity Cluster that owns this Task
     * @param exception
     *            - Cause of the failure
     */
    public void notifyTaskFailure(TaskAttempt ta, ActivityCluster ac, Exception exception) {
        try {
            TaskAttemptId taId = ta.getTaskAttemptId();
            TaskCluster tc = ta.getTaskState().getTaskCluster();
            TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
            if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
                TaskAttempt.TaskStatus taStatus = ta.getStatus();
                if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                    ta.setStatus(TaskAttempt.TaskStatus.FAILED, exception);
                    abortTaskCluster(lastAttempt, ac);
                    lastAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.FAILED);
                    abortDoomedTaskClusters(ac);
                    if (lastAttempt.getAttempt() >= ac.getMaxTaskClusterAttempts()) {
                        abortActivityCluster(ac);
                        return;
                    }
                    startRunnableTaskClusters(ac);
                } else {
                    LOGGER.warning("Spurious task failure notification: " + taId + " Current state = " + taStatus);
                }
            } else {
                LOGGER.warning("Ignoring task failure notification: " + taId + " -- Current last attempt = "
                        + lastAttempt);
            }
        } catch (Exception e) {
            abortJob(e);
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
            for (ActivityCluster ac : jobRun.getActivityClusters()) {
                TaskCluster[] taskClusters = ac.getTaskClusters();
                if (taskClusters != null) {
                    for (TaskCluster tc : taskClusters) {
                        TaskClusterAttempt lastTaskClusterAttempt = findLastTaskClusterAttempt(tc);
                        if (lastTaskClusterAttempt != null
                                && (lastTaskClusterAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.COMPLETED || lastTaskClusterAttempt
                                        .getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING)) {
                            boolean abort = false;
                            for (TaskAttempt ta : lastTaskClusterAttempt.getTaskAttempts()) {
                                assert (ta.getStatus() == TaskAttempt.TaskStatus.COMPLETED || ta.getStatus() == TaskAttempt.TaskStatus.RUNNING);
                                if (deadNodes.contains(ta.getNodeId())) {
                                    ta.setStatus(TaskAttempt.TaskStatus.FAILED,
                                            new HyracksException("Node " + ta.getNodeId() + " failed"));
                                    TaskId tid = ta.getTaskAttemptId().getTaskId();
                                    ActivityId aid = tid.getActivityId();

                                    abort = true;
                                }
                            }
                            if (abort) {
                                abortTaskCluster(lastTaskClusterAttempt, ac);
                            }
                        }
                    }
                    abortDoomedTaskClusters(ac);
                }
            }
            startRunnableActivityClusters();
        } catch (Exception e) {
            abortJob(e);
        }
    }
}