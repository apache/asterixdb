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

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.IJobletEventListener;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
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

    private final JobActivityGraph jag;

    private final Map<PartitionId, IPartitionCollector> partitionRequestMap;

    private final IOperatorEnvironment env;

    private final Map<Object, IStateObject> stateObjectMap;

    private final Map<TaskAttemptId, Task> taskMap;

    private final Map<String, Counter> counterMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private IJobletEventListener jobletEventListener;

    private JobStatus cleanupStatus;

    private boolean cleanupPending;

    public Joblet(NodeControllerService nodeController, JobId jobId, INCApplicationContext appCtx, JobActivityGraph jag) {
        this.nodeController = nodeController;
        this.appCtx = appCtx;
        this.jobId = jobId;
        this.jag = jag;
        partitionRequestMap = new HashMap<PartitionId, IPartitionCollector>();
        env = new OperatorEnvironmentImpl(nodeController.getId());
        stateObjectMap = new HashMap<Object, IStateObject>();
        taskMap = new HashMap<TaskAttemptId, Task>();
        counterMap = new HashMap<String, Counter>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(this, (IOManager) appCtx.getRootContext().getIOManager());
        cleanupPending = false;
    }

    @Override
    public JobId getJobId() {
        return jobId;
    }

    public JobActivityGraph getJobActivityGraph() {
        return jag;
    }

    public IOperatorEnvironment getEnvironment() {
        return env;
    }

    public void addTask(Task task) {
        taskMap.put(task.getTaskAttemptId(), task);
    }

    public void removeTask(Task task) {
        taskMap.remove(task.getTaskAttemptId());
        if (cleanupPending && taskMap.isEmpty()) {
            performCleanup();
        }
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
        public void setStateObject(IStateObject taskState) {
            stateObjectMap.put(taskState.getId(), taskState);
        }

        @Override
        public IStateObject getStateObject(Object id) {
            return stateObjectMap.get(id);
        }
    }

    public NodeControllerService getNodeController() {
        return nodeController;
    }

    public void dumpProfile(JobletProfile jProfile) {
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
        nodeController.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                deallocatableRegistry.close();
            }
        });
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

    public void cleanup(JobStatus status) {
        cleanupStatus = status;
        cleanupPending = true;
        if (taskMap.isEmpty()) {
            performCleanup();
        }
    }

    private void performCleanup() {
        nodeController.getJobletMap().remove(jobId);
        IJobletEventListener listener = getJobletEventListener();
        if (listener != null) {
            listener.jobletFinish(cleanupStatus);
        }
        close();
        cleanupPending = false;
        try {
            nodeController.getClusterController().notifyJobletCleanup(jobId, nodeController.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}