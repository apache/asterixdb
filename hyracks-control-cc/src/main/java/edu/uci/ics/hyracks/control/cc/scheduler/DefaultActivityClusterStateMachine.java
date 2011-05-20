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
import java.util.Comparator;
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

import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.Task;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterAttempt;
import edu.uci.ics.hyracks.control.cc.partitions.PartitionMatchMaker;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;

public class DefaultActivityClusterStateMachine implements IActivityClusterStateMachine {
    private static final Logger LOGGER = Logger.getLogger(DefaultActivityClusterStateMachine.class.getName());

    private final ClusterControllerService ccs;

    private final DefaultJobRunStateMachine jsm;

    private final ActivityCluster ac;

    private final PriorityQueue<RankedRunnableTaskCluster> runnableQueue;

    private final Set<TaskCluster> inProgressTaskClusters;

    public DefaultActivityClusterStateMachine(ClusterControllerService ccs, DefaultJobRunStateMachine jsm,
            ActivityCluster ac) {
        this.ccs = ccs;
        this.jsm = jsm;
        this.ac = ac;
        runnableQueue = new PriorityQueue<RankedRunnableTaskCluster>(ac.getTaskClusters().length,
                new Comparator<RankedRunnableTaskCluster>() {
                    @Override
                    public int compare(RankedRunnableTaskCluster o1, RankedRunnableTaskCluster o2) {
                        int cmp = o1.getRank() - o2.getRank();
                        return cmp < 0 ? -1 : (cmp > 0 ? 1 : 0);
                    }
                });
        inProgressTaskClusters = new HashSet<TaskCluster>();
    }

    @Override
    public void schedule() throws HyracksException {
        startRunnableTaskClusters();
    }

    private void assignTaskLocations(TaskCluster tc, Map<String, List<TaskAttemptDescriptor>> taskAttemptMap)
            throws HyracksException {
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
        PartitionConstraintSolver solver = jsm.getSolver();
        solver.solve(locationMap.values());
        Map<OperatorDescriptorId, String> operatorLocationAssignmentMap = jsm.getOperatorLocationAssignmentMap();
        for (int i = 0; i < tasks.length; ++i) {
            Task ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = taskAttempts[i];
            String nodeId = operatorLocationAssignmentMap.get(tid.getActivityId().getOperatorDescriptorId());
            if (nodeId == null) {
                LValueConstraintExpression pLocationExpr = locationMap.get(tid);
                Object location = solver.getValue(pLocationExpr);
                Set<String> liveNodes = ccs.getNodeMap().keySet();
                if (location == null) {
                    // pick any
                    nodeId = liveNodes.toArray(new String[liveNodes.size()])[Math.abs(new Random().nextInt())
                            % liveNodes.size()];
                } else if (location instanceof String) {
                    nodeId = (String) location;
                    if (!liveNodes.contains(nodeId)) {
                        throw new HyracksException("Node " + nodeId + " not live");
                    }
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
                operatorLocationAssignmentMap.put(tid.getActivityId().getOperatorDescriptorId(), nodeId);
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
        inProgressTaskClusters.add(tc);
    }

    private TaskClusterAttempt findLastTaskClusterAttempt(TaskCluster tc) {
        List<TaskClusterAttempt> attempts = tc.getAttempts();
        if (!attempts.isEmpty()) {
            return attempts.get(attempts.size() - 1);
        }
        return null;
    }

    @Override
    public void notifyTaskComplete(TaskAttempt ta) throws HyracksException {
        TaskAttemptId taId = ta.getTaskAttemptId();
        TaskCluster tc = ta.getTaskState().getTaskCluster();
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
        if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.COMPLETED, null);
                if (lastAttempt.decrementPendingTasksCounter() == 0) {
                    lastAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.COMPLETED);
                    inProgressTaskClusters.remove(tc);
                    startRunnableTaskClusters();
                }
            } else {
                LOGGER.warning("Spurious task complete notification: " + taId + " Current state = " + taStatus);
            }
        } else {
            LOGGER.warning("Ignoring task complete notification: " + taId + " -- Current last attempt = " + lastAttempt);
        }
    }

    private void startRunnableTaskClusters() throws HyracksException {
        findRunnableTaskClusters();
        Map<String, List<TaskAttemptDescriptor>> taskAttemptMap = new HashMap<String, List<TaskAttemptDescriptor>>();
        for (RankedRunnableTaskCluster rrtc : runnableQueue) {
            TaskCluster tc = rrtc.getTaskCluster();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Found runnable TC: " + tc);
                List<TaskClusterAttempt> attempts = tc.getAttempts();
                LOGGER.info("Attempts so far:" + attempts.size());
                for (TaskClusterAttempt tcAttempt : attempts) {
                    LOGGER.info("Status: " + tcAttempt.getStatus());
                }
            }
            assignTaskLocations(tc, taskAttemptMap);
        }

        if (taskAttemptMap.isEmpty()) {
            if (inProgressTaskClusters.isEmpty()) {
                ac.notifyActivityClusterComplete();
            }
            return;
        }

        startTasks(taskAttemptMap);
    }

    private void findRunnableTaskClusters() {
        TaskCluster[] taskClusters = ac.getTaskClusters();

        Map<TaskCluster, Integer> runnabilityRanks = new HashMap<TaskCluster, Integer>();
        for (TaskCluster tc : taskClusters) {
            // Start search at TCs that produce no outputs (sinks)
            if (tc.getProducedPartitions().isEmpty()) {
                assignRunnabilityRank(tc, runnabilityRanks);
            }
        }

        runnableQueue.clear();
        for (Map.Entry<TaskCluster, Integer> e : runnabilityRanks.entrySet()) {
            TaskCluster tc = e.getKey();
            int rank = e.getValue();
            if (rank >= 0 && rank < Integer.MAX_VALUE) {
                runnableQueue.add(new RankedRunnableTaskCluster(rank, tc));
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ranked TCs: " + runnableQueue);
        }
    }

    /*
     * Runnability rank has the following semantics
     * Rank(Completed TaskCluster) = -2
     * Rank(Running TaskCluster) = -1
     * Rank(Runnable TaskCluster depending on completed TaskClusters) = 0
     * Rank(Runnable TaskCluster) = max(Rank(Dependent TaskClusters)) + 1
     * Rank(Non-schedulable TaskCluster) = MAX_VALUE 
     */
    private int assignRunnabilityRank(TaskCluster goal, Map<TaskCluster, Integer> runnabilityRank) {
        if (runnabilityRank.containsKey(goal)) {
            return runnabilityRank.get(goal);
        }
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(goal);
        if (lastAttempt != null) {
            if (lastAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.COMPLETED
                    || lastAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING) {
                int rank = -1;
                runnabilityRank.put(goal, rank);
                return rank;
            }
        }
        Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap = ac.getConnectorPolicyMap();
        JobRun jobRun = ac.getJobRun();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        int maxInputRank = -1;
        for (PartitionId pid : goal.getRequiredPartitions()) {
            int rank = -1;
            ConnectorDescriptorId cdId = pid.getConnectorDescriptorId();
            IConnectorPolicy cPolicy = connectorPolicyMap.get(cdId);
            PartitionState maxState = pmm.getMaximumAvailableState(pid);
            if (PartitionState.COMMITTED.equals(maxState)
                    || (PartitionState.STARTED.equals(maxState) && !cPolicy.consumerWaitsForProducerToFinish())) {
                rank = -1;
            } else {
                rank = assignRunnabilityRank(ac.getPartitionProducingTaskClusterMap().get(pid), runnabilityRank);
                if (rank >= 0 && cPolicy.consumerWaitsForProducerToFinish()) {
                    rank = Integer.MAX_VALUE;
                }
            }
            maxInputRank = Math.max(maxInputRank, rank);
        }
        int rank = maxInputRank < Integer.MAX_VALUE ? maxInputRank + 1 : Integer.MAX_VALUE;
        runnabilityRank.put(goal, rank);
        return rank;
    }

    private void startTasks(Map<String, List<TaskAttemptDescriptor>> taskAttemptMap) throws HyracksException {
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
                                    taskDescriptors, connectorPolicies);
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

    private void abortTaskCluster(TaskClusterAttempt tcAttempt) throws HyracksException {
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
        TaskCluster tc = tcAttempt.getTaskCluster();
        PartitionMatchMaker pmm = jobRun.getPartitionMatchMaker();
        pmm.removeUncommittedPartitions(tc.getProducedPartitions(), abortTaskIds);
        pmm.removePartitionRequests(tc.getRequiredPartitions(), abortTaskIds);
    }

    @Override
    public void notifyTaskFailure(TaskAttempt ta, Exception exception) throws HyracksException {
        TaskAttemptId taId = ta.getTaskAttemptId();
        TaskCluster tc = ta.getTaskState().getTaskCluster();
        TaskClusterAttempt lastAttempt = findLastTaskClusterAttempt(tc);
        if (lastAttempt != null && taId.getAttempt() == lastAttempt.getAttempt()) {
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.FAILED, exception);
                abortTaskCluster(lastAttempt);
                lastAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.FAILED);
                abortDoomedTaskClusters();
                ac.notifyTaskClusterFailure(lastAttempt, exception);
            } else {
                LOGGER.warning("Spurious task failure notification: " + taId + " Current state = " + taStatus);
            }
        } else {
            LOGGER.warning("Ignoring task failure notification: " + taId + " -- Current last attempt = " + lastAttempt);
        }
    }

    private void abortDoomedTaskClusters() throws HyracksException {
        TaskCluster[] taskClusters = ac.getTaskClusters();

        Set<TaskCluster> doomedTaskClusters = new HashSet<TaskCluster>();
        for (TaskCluster tc : taskClusters) {
            // Start search at TCs that produce no outputs (sinks)
            if (tc.getProducedPartitions().isEmpty()) {
                findDoomedTaskClusters(tc, doomedTaskClusters);
            }
        }

        for (TaskCluster tc : doomedTaskClusters) {
            TaskClusterAttempt tca = findLastTaskClusterAttempt(tc);
            abortTaskCluster(tca);
            tca.setStatus(TaskClusterAttempt.TaskClusterStatus.ABORTED);
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
                if (findDoomedTaskClusters(ac.getPartitionProducingTaskClusterMap().get(pid), doomedTaskClusters)) {
                    doomed = true;
                }
            }
        }
        if (doomed) {
            doomedTaskClusters.add(tc);
        }
        return doomed;
    }

    @Override
    public void abort() throws HyracksException {
        TaskCluster[] taskClusters = ac.getTaskClusters();
        for (TaskCluster tc : taskClusters) {
            List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
            if (!tcAttempts.isEmpty()) {
                TaskClusterAttempt tcAttempt = tcAttempts.get(tcAttempts.size() - 1);
                if (tcAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING) {
                    abortTaskCluster(tcAttempt);
                    tcAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.ABORTED);
                }
            }
        }
    }

    @Override
    public void notifyTaskClusterFailure(TaskClusterAttempt tcAttempt, Exception exception) throws HyracksException {
        TaskCluster tc = tcAttempt.getTaskCluster();
        if (tcAttempt.getAttempt() >= ac.getMaxTaskClusterAttempts()) {
            abort();
            ac.getJobRun().getStateMachine().notifyActivityClusterFailure(ac, exception);
            return;
        }
        Map<String, List<TaskAttemptDescriptor>> taskAttemptMap = new HashMap<String, List<TaskAttemptDescriptor>>();
        assignTaskLocations(tc, taskAttemptMap);
        startTasks(taskAttemptMap);
    }

    @Override
    public void notifyNodeFailures(Set<String> deadNodes) throws HyracksException {
        for (TaskCluster tc : ac.getTaskClusters()) {
            TaskClusterAttempt lastTaskClusterAttempt = findLastTaskClusterAttempt(tc);
            if (lastTaskClusterAttempt != null
                    && (lastTaskClusterAttempt.getStatus() == TaskClusterAttempt.TaskClusterStatus.COMPLETED || lastTaskClusterAttempt
                            .getStatus() == TaskClusterAttempt.TaskClusterStatus.RUNNING)) {
                boolean abort = false;
                for (TaskAttempt ta : lastTaskClusterAttempt.getTaskAttempts()) {
                    if (deadNodes.contains(ta.getNodeId())) {
                        ta.setStatus(TaskAttempt.TaskStatus.FAILED, new HyracksException("Node " + ta.getNodeId()
                                + " failed"));
                        abort = true;
                    }
                }
                if (abort) {
                    abortTaskCluster(lastTaskClusterAttempt);
                }
            }
        }
        abortDoomedTaskClusters();
        startRunnableTaskClusters();
    }
}