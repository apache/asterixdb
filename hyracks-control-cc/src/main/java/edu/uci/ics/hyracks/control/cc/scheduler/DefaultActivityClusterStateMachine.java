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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.job.ActivityCluster;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.TaskAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskCluster;
import edu.uci.ics.hyracks.control.cc.job.TaskClusterAttempt;
import edu.uci.ics.hyracks.control.cc.job.TaskState;

public class DefaultActivityClusterStateMachine implements IActivityClusterStateMachine {
    private static final Logger LOGGER = Logger.getLogger(DefaultActivityClusterStateMachine.class.getName());

    private final ClusterControllerService ccs;

    private final DefaultJobRunStateMachine jsm;

    private final ActivityCluster ac;

    public DefaultActivityClusterStateMachine(ClusterControllerService ccs, DefaultJobRunStateMachine jsm,
            ActivityCluster ac) {
        this.ccs = ccs;
        this.jsm = jsm;
        this.ac = ac;
    }

    @Override
    public void schedule() throws HyracksException {
        startRunnableTaskClusters();
    }

    private void assignTaskLocations(TaskCluster tc, Map<String, List<TaskAttemptId>> taskAttemptMap)
            throws HyracksException {
        TaskState[] tasks = tc.getTasks();
        List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
        int attempts = tcAttempts.size();
        TaskAttempt[] taskAttempts = new TaskAttempt[tasks.length];
        Map<TaskId, LValueConstraintExpression> locationMap = new HashMap<TaskId, LValueConstraintExpression>();
        for (int i = 0; i < tasks.length; ++i) {
            TaskState ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = new TaskAttempt(new TaskAttemptId(new TaskId(tid.getActivityId(),
                    tid.getPartition()), attempts), ts);
            locationMap.put(tid,
                    new PartitionLocationExpression(tid.getActivityId().getOperatorDescriptorId(), tid.getPartition()));
            taskAttempts[i] = taskAttempt;
        }
        PartitionConstraintSolver solver = jsm.getSolver();
        solver.solve(locationMap.values());
        for (int i = 0; i < tasks.length; ++i) {
            TaskState ts = tasks[i];
            TaskId tid = ts.getTaskId();
            TaskAttempt taskAttempt = taskAttempts[i];
            LValueConstraintExpression pLocationExpr = locationMap.get(tid);
            Object location = solver.getValue(pLocationExpr);
            String nodeId = null;
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
                    throw new HyracksException("No satisfiable location found for " + taskAttempt.getTaskAttemptId());
                }
            } else {
                throw new HyracksException("Unknown type of value for " + pLocationExpr + ": " + location + "("
                        + location.getClass() + ")");
            }
            taskAttempt.setNodeId(nodeId);
            List<TaskAttemptId> taIds = taskAttemptMap.get(nodeId);
            if (taIds == null) {
                taIds = new ArrayList<TaskAttemptId>();
                taskAttemptMap.put(nodeId, taIds);
            }
            taIds.add(taskAttempt.getTaskAttemptId());
        }
        TaskClusterAttempt tcAttempt = new TaskClusterAttempt(taskAttempts);
        tcAttempt.initializePendingTaskCounter();
        tcAttempts.add(tcAttempt);

    }

    @Override
    public void notifyTaskComplete(TaskAttempt ta) throws HyracksException {
        TaskAttemptId taId = ta.getTaskAttemptId();
        TaskCluster tc = ta.getTaskState().getTaskCluster();
        List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
        int lastAttempt = tcAttempts.size() - 1;
        if (taId.getAttempt() == lastAttempt) {
            TaskClusterAttempt tcAttempt = tcAttempts.get(lastAttempt);
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.COMPLETED, null);
                if (tcAttempt.decrementPendingTasksCounter() == 0) {
                    tcAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.COMPLETED);
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
        TaskCluster[] taskClusters = ac.getTaskClusters();

        Map<String, List<TaskAttemptId>> taskAttemptMap = new HashMap<String, List<TaskAttemptId>>();
        for (TaskCluster tc : taskClusters) {
            Set<TaskCluster> dependencies = tc.getDependencies();
            boolean runnable = true;
            for (TaskCluster depTC : dependencies) {
                List<TaskClusterAttempt> tcAttempts = depTC.getAttempts();
                if (tcAttempts.isEmpty()) {
                    runnable = false;
                    break;
                }
                TaskClusterAttempt tcAttempt = tcAttempts.get(tcAttempts.size() - 1);
                if (tcAttempt.getStatus() != TaskClusterAttempt.TaskClusterStatus.COMPLETED) {
                    runnable = false;
                    break;
                }
            }
            if (runnable) {
                assignTaskLocations(tc, taskAttemptMap);
            }
        }

        startTasks(taskAttemptMap);
    }

    private void startTasks(Map<String, List<TaskAttemptId>> taskAttemptMap) {
        Executor executor = ccs.getExecutor();
        JobRun jobRun = ac.getJobRun();
        final UUID jobId = jobRun.getJobId();
        final JobActivityGraph jag = jobRun.getJobActivityGraph();
        final String appName = jag.getApplicationName();
        for (Map.Entry<String, List<TaskAttemptId>> e : taskAttemptMap.entrySet()) {
            String nodeId = e.getKey();
            final List<TaskAttemptId> taskIds = e.getValue();
            final NodeControllerState node = ccs.getNodeMap().get(nodeId);
            if (node != null) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            node.getNodeController().startTasks(appName, jobId, JavaSerializationUtils.serialize(jag),
                                    taskIds, null);
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

    @Override
    public void notifyTaskFailure(TaskAttempt ta, Exception exception) throws HyracksException {
        TaskAttemptId taId = ta.getTaskAttemptId();
        TaskCluster tc = ta.getTaskState().getTaskCluster();
        List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
        int lastAttempt = tcAttempts.size() - 1;
        if (taId.getAttempt() == lastAttempt) {
            TaskClusterAttempt tcAttempt = tcAttempts.get(lastAttempt);
            TaskAttempt.TaskStatus taStatus = ta.getStatus();
            Map<String, List<TaskAttemptId>> abortTaskAttemptMap = new HashMap<String, List<TaskAttemptId>>();
            if (taStatus == TaskAttempt.TaskStatus.RUNNING) {
                ta.setStatus(TaskAttempt.TaskStatus.FAILED, exception);
                for (TaskAttempt ta2 : tcAttempt.getTaskAttempts()) {
                    if (ta2.getStatus() == TaskAttempt.TaskStatus.RUNNING
                            || ta2.getStatus() == TaskAttempt.TaskStatus.INITIALIZED) {
                        ta2.setStatus(TaskAttempt.TaskStatus.ABORTED, null);
                        List<TaskAttemptId> abortTaskAttempts = abortTaskAttemptMap.get(ta2.getNodeId());
                        if (abortTaskAttempts == null) {
                            abortTaskAttempts = new ArrayList<TaskAttemptId>();
                            abortTaskAttemptMap.put(ta2.getNodeId(), abortTaskAttempts);
                        }
                        abortTaskAttempts.add(ta2.getTaskAttemptId());
                    }
                }
                JobRun jobRun = ac.getJobRun();
                final UUID jobId = jobRun.getJobId();
                for (Map.Entry<String, List<TaskAttemptId>> e : abortTaskAttemptMap.entrySet()) {
                    final NodeControllerState node = ccs.getNodeMap().get(e.getKey());
                    final List<TaskAttemptId> abortTaskAttempts = e.getValue();
                    if (node != null) {
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
                tcAttempt.setStatus(TaskClusterAttempt.TaskClusterStatus.FAILED);
                Map<String, List<TaskAttemptId>> taskAttemptMap = new HashMap<String, List<TaskAttemptId>>();
                assignTaskLocations(tc, taskAttemptMap);
                startTasks(taskAttemptMap);
            } else {
                LOGGER.warning("Spurious task complete notification: " + taId + " Current state = " + taStatus);
            }
        } else {
            LOGGER.warning("Ignoring task complete notification: " + taId + " -- Current last attempt = " + lastAttempt);
        }
    }
}