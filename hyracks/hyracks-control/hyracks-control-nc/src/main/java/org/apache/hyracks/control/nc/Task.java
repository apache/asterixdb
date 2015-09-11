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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.IOperatorEnvironment;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.common.job.profiling.counters.Counter;
import org.apache.hyracks.control.common.job.profiling.om.PartitionProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.control.common.utils.ExceptionUtils;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import org.apache.hyracks.control.nc.work.NotifyTaskCompleteWork;
import org.apache.hyracks.control.nc.work.NotifyTaskFailureWork;

public class Task implements IHyracksTaskContext, ICounterContext, Runnable {
    private final Joblet joblet;

    private final TaskAttemptId taskAttemptId;

    private final String displayName;

    private final Executor executor;

    private final IWorkspaceFileFactory fileFactory;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final Map<String, Counter> counterMap;

    private final IOperatorEnvironment opEnv;

    private final Map<PartitionId, PartitionProfile> partitionSendProfile;

    private final Set<Thread> pendingThreads;

    private IPartitionCollector[] collectors;

    private IOperatorNodePushable operator;

    private final List<Exception> exceptions;

    private List<Throwable> caughtExceptions;

    private volatile boolean aborted;

    private NodeControllerService ncs;

    private List<List<PartitionChannel>> inputChannelsFromConnectors;

    public Task(Joblet joblet, TaskAttemptId taskId, String displayName, Executor executor, NodeControllerService ncs,
            List<List<PartitionChannel>> inputChannelsFromConnectors) {
        this.joblet = joblet;
        this.taskAttemptId = taskId;
        this.displayName = displayName;
        this.executor = executor;
        fileFactory = new WorkspaceFileFactory(this, (IOManager) joblet.getIOManager());
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        counterMap = new HashMap<String, Counter>();
        opEnv = joblet.getEnvironment();
        partitionSendProfile = new Hashtable<PartitionId, PartitionProfile>();
        pendingThreads = new LinkedHashSet<Thread>();
        exceptions = new ArrayList<>();
        this.ncs = ncs;
        this.inputChannelsFromConnectors = inputChannelsFromConnectors;
    }

    public void setTaskRuntime(IPartitionCollector[] collectors, IOperatorNodePushable operator) {
        this.collectors = collectors;
        this.operator = operator;
    }

    @Override
    public ByteBuffer allocateFrame() throws HyracksDataException {
        return joblet.allocateFrame();
    }

    @Override
    public ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
        return joblet.allocateFrame(bytes);
    }

    @Override
    public ByteBuffer reallocateFrame(ByteBuffer usedBuffer, int newSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        return joblet.reallocateFrame(usedBuffer, newSizeInBytes, copyOldData);
    }

    @Override
    public void deallocateFrames(int bytes) {
        joblet.deallocateFrames(bytes);
    }

    @Override
    public int getInitialFrameSize() {
        return joblet.getFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return joblet.getIOManager();
    }

    @Override
    public FileReference createUnmanagedWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createUnmanagedWorkspaceFile(prefix);
    }

    @Override
    public FileReference createManagedWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createManagedWorkspaceFile(prefix);
    }

    @Override
    public void registerDeallocatable(IDeallocatable deallocatable) {
        deallocatableRegistry.registerDeallocatable(deallocatable);
    }

    public void close() {
        deallocatableRegistry.close();
    }

    @Override
    public IHyracksJobletContext getJobletContext() {
        return joblet;
    }

    @Override
    public TaskAttemptId getTaskAttemptId() {
        return taskAttemptId;
    }

    @Override
    public ICounter getCounter(String name, boolean create) {
        Counter counter = counterMap.get(name);
        if (counter == null && create) {
            counter = new Counter(name);
            counterMap.put(name, counter);
        }
        return counter;
    }

    @Override
    public ICounterContext getCounterContext() {
        return this;
    }

    public Joblet getJoblet() {
        return joblet;
    }

    public Map<PartitionId, PartitionProfile> getPartitionSendProfile() {
        return partitionSendProfile;
    }

    public synchronized void dumpProfile(TaskProfile tProfile) {
        Map<String, Long> dumpMap = tProfile.getCounters();
        for (Counter c : counterMap.values()) {
            dumpMap.put(c.getName(), c.get());
        }
    }

    public void setPartitionSendProfile(PartitionProfile profile) {
        partitionSendProfile.put(profile.getPartitionId(), profile);
    }

    public void start() throws HyracksException {
        aborted = false;
        executor.execute(this);
    }

    public synchronized void abort() {
        aborted = true;
        for (IPartitionCollector c : collectors) {
            c.abort();
        }
        for (Thread t : pendingThreads) {
            t.interrupt();
        }
    }

    private synchronized void addPendingThread(Thread t) {
        pendingThreads.add(t);
    }

    private synchronized void removePendingThread(Thread t) {
        pendingThreads.remove(t);
        if (pendingThreads.isEmpty()) {
            notifyAll();
        }
    }

    public synchronized void waitForCompletion() throws InterruptedException {
        while (!pendingThreads.isEmpty()) {
            wait();
        }
    }

    @Override
    public void run() {
        Thread ct = Thread.currentThread();
        String threadName = ct.getName();
        addPendingThread(ct);
        try {
            ct.setName(displayName + ":" + taskAttemptId + ":" + 0);
            operator.initialize();
            try {
                if (collectors.length > 0) {
                    final Semaphore sem = new Semaphore(collectors.length - 1);
                    for (int i = 1; i < collectors.length; ++i) {
                        final IPartitionCollector collector = collectors[i];
                        final IFrameWriter writer = operator.getInputFrameWriter(i);
                        sem.acquire();
                        final int cIdx = i;
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                if (aborted) {
                                    return;
                                }
                                Thread thread = Thread.currentThread();
                                addPendingThread(thread);
                                String oldName = thread.getName();
                                thread.setName(displayName + ":" + taskAttemptId + ":" + cIdx);
                                thread.setPriority(Thread.MIN_PRIORITY);
                                try {
                                    pushFrames(collector, inputChannelsFromConnectors.get(cIdx), writer);
                                } catch (HyracksDataException e) {
                                    synchronized (Task.this) {
                                        exceptions.add(e);
                                    }
                                } finally {
                                    thread.setName(oldName);
                                    sem.release();
                                    removePendingThread(thread);
                                }
                            }
                        });
                    }
                    try {
                        pushFrames(collectors[0], inputChannelsFromConnectors.get(0), operator.getInputFrameWriter(0));
                    } finally {
                        sem.acquire(collectors.length - 1);
                    }
                }
            } finally {
                operator.deinitialize();
            }
            NodeControllerService ncs = joblet.getNodeController();
            ncs.getWorkQueue().schedule(new NotifyTaskCompleteWork(ncs, this));
        } catch (Exception e) {
            exceptions.add(e);
        } finally {
            ct.setName(threadName);
            close();
            removePendingThread(ct);
        }
        if (!exceptions.isEmpty()) {
            for (Exception e : exceptions) {
                e.printStackTrace();
            }
            NodeControllerService ncs = joblet.getNodeController();
            ExceptionUtils.setNodeIds(exceptions, ncs.getId());
            ncs.getWorkQueue().schedule(new NotifyTaskFailureWork(ncs, this, exceptions));
        }
    }

    private void pushFrames(IPartitionCollector collector, List<PartitionChannel> inputChannels, IFrameWriter writer)
            throws HyracksDataException {
        if (aborted) {
            return;
        }
        try {
            collector.open();
            try {
                if (inputChannels.size() <= 0) {
                    joblet.advertisePartitionRequest(taskAttemptId, collector.getRequiredPartitionIds(), collector,
                            PartitionState.STARTED);
                } else {
                    collector.addPartitions(inputChannels);
                }
                IFrameReader reader = collector.getReader();
                reader.open();
                try {
                    writer.open();
                    try {
                        VSizeFrame frame = new VSizeFrame(this);
                        while( reader.nextFrame(frame)){
                            if (aborted) {
                                return;
                            }
                            ByteBuffer buffer = frame.getBuffer();
                            writer.nextFrame(buffer);
                            buffer.compact();
                        }
                    } catch (Exception e) {
                        writer.fail();
                        throw e;
                    } finally {
                        writer.close();
                    }
                } finally {
                    reader.close();
                }
            } finally {
                collector.close();
            }
        } catch (HyracksException e) {
            throw new HyracksDataException(e);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void setStateObject(IStateObject taskState) {
        opEnv.setStateObject(taskState);
    }

    @Override
    public IStateObject getStateObject(Object id) {
        return opEnv.getStateObject(id);
    }

    @Override
    public IDatasetPartitionManager getDatasetPartitionManager() {
        return ncs.getDatasetPartitionManager();
    }

    @Override
    public void sendApplicationMessageToCC(byte[] message, DeploymentId deploymentId, String nodeId) throws Exception {
        this.ncs.sendApplicationMessageToCC(message, deploymentId, nodeId);
    }
}