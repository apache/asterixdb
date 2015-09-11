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
package org.apache.hyracks.control.nc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.IGlobalJobDataFactory;
import org.apache.hyracks.api.job.IJobletEventListener;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.IOperatorEnvironment;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.control.common.deployment.DeploymentUtils;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.common.job.profiling.counters.Counter;
import org.apache.hyracks.control.common.job.profiling.om.JobletProfile;
import org.apache.hyracks.control.common.job.profiling.om.PartitionProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;

public class Joblet implements IHyracksJobletContext, ICounterContext {
    private static final Logger LOGGER = Logger.getLogger(Joblet.class.getName());

    private final NodeControllerService nodeController;

    private final INCApplicationContext appCtx;

    private final DeploymentId deploymentId;

    private final JobId jobId;

    private final ActivityClusterGraph acg;

    private final Map<PartitionId, IPartitionCollector> partitionRequestMap;

    private final IOperatorEnvironment env;

    private final Map<Object, IStateObject> stateObjectMap;

    private final Map<TaskAttemptId, Task> taskMap;

    private final Map<String, Counter> counterMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private final Object globalJobData;

    private final IJobletEventListener jobletEventListener;

    private final FrameManager frameManager;

    private final AtomicLong memoryAllocation;

    private JobStatus cleanupStatus;

    private boolean cleanupPending;

    public Joblet(NodeControllerService nodeController, DeploymentId deploymentId, JobId jobId,
            INCApplicationContext appCtx, ActivityClusterGraph acg) {
        this.nodeController = nodeController;
        this.appCtx = appCtx;
        this.deploymentId = deploymentId;
        this.jobId = jobId;
        this.frameManager = new FrameManager(acg.getFrameSize());
        memoryAllocation = new AtomicLong();
        this.acg = acg;
        partitionRequestMap = new HashMap<PartitionId, IPartitionCollector>();
        env = new OperatorEnvironmentImpl(nodeController.getId());
        stateObjectMap = new HashMap<Object, IStateObject>();
        taskMap = new HashMap<TaskAttemptId, Task>();
        counterMap = new HashMap<String, Counter>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(this, (IOManager) appCtx.getRootContext().getIOManager());
        cleanupPending = false;
        IJobletEventListenerFactory jelf = acg.getJobletEventListenerFactory();
        if (jelf != null) {
            IJobletEventListener listener = jelf.createListener(this);
            this.jobletEventListener = listener;
            listener.jobletStart();
        } else {
            jobletEventListener = null;
        }
        IGlobalJobDataFactory gjdf = acg.getGlobalJobDataFactory();
        globalJobData = gjdf != null ? gjdf.createGlobalJobData(this) : null;
    }

    @Override
    public JobId getJobId() {
        return jobId;
    }

    public ActivityClusterGraph getActivityClusterGraph() {
        return acg;
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
        public synchronized void setStateObject(IStateObject taskState) {
            stateObjectMap.put(taskState.getId(), taskState);
        }

        @Override
        public synchronized IStateObject getStateObject(Object id) {
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
        long stillAllocated = memoryAllocation.get();
        if (stillAllocated > 0) {
            LOGGER.warning("Freeing leaked " + stillAllocated + " bytes");
            appCtx.getMemoryManager().deallocate(stillAllocated);
        }
        nodeController.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                deallocatableRegistry.close();
            }
        });
    }

    ByteBuffer allocateFrame() throws HyracksDataException {
        return frameManager.allocateFrame();
    }

    ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
        if (appCtx.getMemoryManager().allocate(bytes)) {
            memoryAllocation.addAndGet(bytes);
            return frameManager.allocateFrame(bytes);
       }
        throw new HyracksDataException("Unable to allocate frame: Not enough memory");
    }

    ByteBuffer reallocateFrame(ByteBuffer usedBuffer, int newFrameSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        return frameManager.reallocateFrame(usedBuffer, newFrameSizeInBytes, copyOldData);
    }

    void deallocateFrames(int bytes) {
        memoryAllocation.addAndGet(bytes);
        appCtx.getMemoryManager().deallocate(bytes);
        frameManager.deallocateFrames(bytes);
    }

    public final int getFrameSize() {
        return frameManager.getInitialFrameSize();
    }

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

    @Override
    public Object getGlobalJobData() {
        return globalJobData;
    }

    public IJobletEventListener getJobletEventListener() {
        return jobletEventListener;
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

    @Override
    public Class<?> loadClass(String className) {
        try {
            return DeploymentUtils.loadClass(className, deploymentId, appCtx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClassLoader getClassLoader() {
        try {
            return DeploymentUtils.getClassLoader(deploymentId, appCtx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}