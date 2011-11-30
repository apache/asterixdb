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
package edu.uci.ics.hyracks.control.nc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Executor;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.state.ITaskState;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.IJobletEventListener;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobletProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.PartitionProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class Joblet implements IHyracksJobletContext, ICounterContext {
    private final NodeControllerService nodeController;

    private final INCApplicationContext appCtx;

    private final JobId jobId;

    private final Map<PartitionId, IPartitionCollector> partitionRequestMap;

    private final Map<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>> envMap;

    private final Map<TaskId, ITaskState> taskStateMap;

    private final Map<TaskAttemptId, Task> taskMap;

    private final Map<String, Counter> counterMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private IJobletEventListener jobletEventListener;

    public Joblet(NodeControllerService nodeController, JobId jobId, INCApplicationContext appCtx) {
        this.nodeController = nodeController;
        this.appCtx = appCtx;
        this.jobId = jobId;
        partitionRequestMap = new HashMap<PartitionId, IPartitionCollector>();
        envMap = new HashMap<OperatorDescriptorId, Map<Integer, IOperatorEnvironment>>();
        taskStateMap = new HashMap<TaskId, ITaskState>();
        taskMap = new HashMap<TaskAttemptId, Task>();
        counterMap = new HashMap<String, Counter>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(this, (IOManager) appCtx.getRootContext().getIOManager());
    }

    @Override
    public JobId getJobId() {
        return jobId;
    }

    public synchronized IOperatorEnvironment getEnvironment(OperatorDescriptorId opId, int partition) {
        if (!envMap.containsKey(opId)) {
            envMap.put(opId, new HashMap<Integer, IOperatorEnvironment>());
        }
        Map<Integer, IOperatorEnvironment> opEnvMap = envMap.get(opId);
        if (!opEnvMap.containsKey(partition)) {
            opEnvMap.put(partition, new OperatorEnvironmentImpl(nodeController.getId()));
        }
        return opEnvMap.get(partition);
    }

    public void addTask(Task task) {
        taskMap.put(task.getTaskAttemptId(), task);
    }

    public Map<TaskAttemptId, Task> getTaskMap() {
        return taskMap;
    }

    private final class OperatorEnvironmentImpl implements IOperatorEnvironment {
        private final String nodeId;

        public OperatorEnvironmentImpl(String nodeId) {
            this.nodeId = nodeId;
        }

        public String toString() {
            return super.toString() + "@" + nodeId;
        }

        @Override
        public void setTaskState(ITaskState taskState) {
            taskStateMap.put(taskState.getTaskId(), taskState);
        }

        @Override
        public ITaskState getTaskState(TaskId taskId) {
            return taskStateMap.get(taskId);
        }
    }

    public Executor getExecutor() {
        return nodeController.getExecutor();
    }

    public synchronized void notifyTaskComplete(Task task) throws Exception {
        taskMap.remove(task);
        TaskProfile taskProfile = new TaskProfile(task.getTaskAttemptId(), task.getPartitionSendProfile());
        task.dumpProfile(taskProfile);
        nodeController.getClusterController().notifyTaskComplete(jobId, task.getTaskAttemptId(),
                nodeController.getId(), taskProfile);
    }

    public synchronized void notifyTaskFailed(Task task, String details) throws Exception {
        taskMap.remove(task);
        nodeController.getClusterController().notifyTaskFailure(jobId, task.getTaskAttemptId(), nodeController.getId(),
                details);
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public synchronized void dumpProfile(JobletProfile jProfile) {
        Map<String, Long> counters = jProfile.getCounters();
        for (Map.Entry<String, Counter> e : counterMap.entrySet()) {
            counters.put(e.getKey(), e.getValue().get());
        }
        for (Task task : taskMap.values()) {
            TaskProfile taskProfile = new TaskProfile(task.getTaskAttemptId(),
                    new Hashtable<PartitionId, PartitionProfile>(task.getPartitionSendProfile()));
            task.dumpProfile(taskProfile);
            jProfile.getTaskProfiles().put(task.getTaskAttemptId(), taskProfile);
        }
    }

    @Override
    public INCApplicationContext getApplicationContext() {
        return appCtx;
    }

    @Override
    public ICounterContext getCounterContext() {
        return this;
    }

    @Override
    public void registerDeallocatable(IDeallocatable deallocatable) {
        deallocatableRegistry.registerDeallocatable(deallocatable);
    }

    public void close() {
        deallocatableRegistry.close();
    }

    @Override
    public ByteBuffer allocateFrame() {
        return appCtx.getRootContext().allocateFrame();
    }

    @Override
    public int getFrameSize() {
        return appCtx.getRootContext().getFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return appCtx.getRootContext().getIOManager();
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createManagedWorkspaceFile(prefix);
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createUnmanagedWorkspaceFile(prefix);
    }

    @Override
    public synchronized ICounter getCounter(String name, boolean create) {
        Counter counter = counterMap.get(name);
        if (counter == null && create) {
            counter = new Counter(name);
            counterMap.put(name, counter);
        }
        return counter;
    }

    public synchronized void advertisePartitionRequest(TaskAttemptId taId, Collection<PartitionId> pids,
            IPartitionCollector collector, PartitionState minState) throws Exception {
        for (PartitionId pid : pids) {
            partitionRequestMap.put(pid, collector);
            PartitionRequest req = new PartitionRequest(pid, nodeController.getId(), taId, minState);
            nodeController.getClusterController().registerPartitionRequest(req);
        }
    }

    public synchronized void reportPartitionAvailability(PartitionChannel channel) throws HyracksException {
        IPartitionCollector collector = partitionRequestMap.get(channel.getPartitionId());
        if (collector != null) {
            collector.addPartitions(Collections.singleton(channel));
        }
    }

    public IJobletEventListener getJobletEventListener() {
        return jobletEventListener;
    }

    public void setJobletEventListener(IJobletEventListener jobletEventListener) {
        this.jobletEventListener = jobletEventListener;
    }
}