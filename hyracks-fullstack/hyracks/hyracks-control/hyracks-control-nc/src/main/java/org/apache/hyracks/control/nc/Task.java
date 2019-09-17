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

import static org.apache.hyracks.api.exceptions.ErrorCode.TASK_ABORTED;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.IOperatorEnvironment;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.common.job.profiling.StatsCollector;
import org.apache.hyracks.control.common.job.profiling.om.PartitionProfile;
import org.apache.hyracks.control.common.job.profiling.om.TaskProfile;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import org.apache.hyracks.control.nc.work.NotifyTaskCompleteWork;
import org.apache.hyracks.control.nc.work.NotifyTaskFailureWork;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.IThreadStatsCollector;
import org.apache.hyracks.util.ThreadStats;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Task implements IHyracksTaskContext, ICounterContext, Runnable {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Joblet joblet;

    private final TaskAttemptId taskAttemptId;

    private final String displayName;

    private final ExecutorService executorService;

    private final IWorkspaceFileFactory fileFactory;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final Map<String, Counter> counterMap;

    private final IOperatorEnvironment opEnv;

    private final Map<PartitionId, PartitionProfile> partitionSendProfile;

    private final Set<Thread> pendingThreads;

    private IPartitionCollector[] collectors;

    private IOperatorNodePushable operator;

    private final List<Exception> exceptions;

    private volatile boolean aborted;

    private final NodeControllerService ncs;

    private List<List<PartitionChannel>> inputChannelsFromConnectors;

    private Object sharedObject;

    private final Set<JobFlag> jobFlags;

    private final IStatsCollector statsCollector;

    private volatile boolean completed = false;

    private final Set<Warning> warnings;

    private final IWarningCollector warningCollector;

    private final Set<IThreadStatsCollector> threadStatsCollectors = new HashSet<>();

    private final Map<Long, IThreadStats> perThreadStats = new HashMap<>();

    public Task(Joblet joblet, Set<JobFlag> jobFlags, TaskAttemptId taskId, String displayName,
            ExecutorService executor, NodeControllerService ncs,
            List<List<PartitionChannel>> inputChannelsFromConnectors) {
        this.joblet = joblet;
        this.jobFlags = jobFlags;
        this.taskAttemptId = taskId;
        this.displayName = displayName;
        this.executorService = executor;
        fileFactory = new WorkspaceFileFactory(this, joblet.getIOManager());
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        counterMap = new HashMap<>();
        opEnv = joblet.getEnvironment();
        partitionSendProfile = new Hashtable<>();
        pendingThreads = new LinkedHashSet<>();
        exceptions = new CopyOnWriteArrayList<>(); // Multiple threads could add exceptions to this list.
        this.ncs = ncs;
        this.inputChannelsFromConnectors = inputChannelsFromConnectors;
        statsCollector = new StatsCollector();
        warnings = ConcurrentHashMap.newKeySet();
        warningCollector = createWarningCollector(joblet.getMaxWarnings());
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
    public IIOManager getIoManager() {
        return joblet.getIOManager();
    }

    @Override
    public ExecutorService getExecutorService() {
        return executorService;
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
        threadStatsCollectors.forEach(IThreadStatsCollector::unsubscribe);
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

    public NodeControllerService getNodeControllerService() {
        return ncs;
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
        executorService.execute(this);
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

    private synchronized boolean addPendingThread(Thread t) {
        if (aborted) {
            return false;
        }
        return pendingThreads.add(t);
    }

    public synchronized List<Thread> getPendingThreads() {
        return new ArrayList<>(pendingThreads);
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
        // Calls synchronized addPendingThread(..) to make sure that in the abort() method,
        // the thread is not escaped from interruption.
        if (!addPendingThread(ct)) {
            exceptions.add(HyracksDataException.create(TASK_ABORTED, getTaskAttemptId()));
            ExceptionUtils.setNodeIds(exceptions, ncs.getId());
            ncs.getWorkQueue()
                    .schedule(new NotifyTaskFailureWork(ncs, this, exceptions, joblet.getJobId(), taskAttemptId));
            return;
        }
        ct.setName(displayName + ":" + joblet.getJobId() + ":" + taskAttemptId + ":" + 0);
        try {
            Throwable operatorException = null;
            try {
                operator.initialize();
                if (collectors.length > 0) {
                    final Semaphore sem = new Semaphore(collectors.length - 1);
                    for (int i = 1; i < collectors.length; ++i) {
                        // Q. Do we ever have a task that has more than one collector?
                        final IPartitionCollector collector = collectors[i];
                        final IFrameWriter writer = operator.getInputFrameWriter(i);
                        sem.acquireUninterruptibly();
                        final int cIdx = i;
                        executorService.execute(() -> {
                            try {
                                Thread thread = Thread.currentThread();
                                if (!addPendingThread(thread)) {
                                    return;
                                }
                                thread.setName(
                                        displayName + ":" + joblet.getJobId() + ":" + taskAttemptId + ":" + cIdx);
                                thread.setPriority(Thread.MIN_PRIORITY);
                                try {
                                    pushFrames(collector, inputChannelsFromConnectors.get(cIdx), writer);
                                } catch (HyracksDataException e) {
                                    synchronized (Task.this) {
                                        exceptions.add(e);
                                    }
                                } finally {
                                    removePendingThread(thread);
                                }
                            } finally {
                                unsubscribeThreadFromStats();
                                sem.release();
                            }
                        });
                    }
                    try {
                        pushFrames(collectors[0], inputChannelsFromConnectors.get(0), operator.getInputFrameWriter(0));
                    } finally {
                        unsubscribeThreadFromStats();
                        sem.acquireUninterruptibly(collectors.length - 1);
                    }
                }
            } catch (Throwable e) { // NOSONAR: Must catch all failures
                operatorException = e;
            } finally {
                try {
                    operator.deinitialize();
                } catch (Throwable e) { // NOSONAR: Must catch all failures
                    operatorException = ExceptionUtils.suppress(operatorException, e);
                }
            }
            if (operatorException != null) {
                throw operatorException;
            }
            ncs.getWorkQueue().schedule(new NotifyTaskCompleteWork(ncs, this));
        } catch (Throwable e) { // NOSONAR: Catch all failures
            exceptions.add(HyracksDataException.create(e));
        } finally {
            close();
            removePendingThread(ct);
            completed = true;
        }
        if (!exceptions.isEmpty()) {
            if (LOGGER.isWarnEnabled()) {
                for (int i = 0; i < exceptions.size(); i++) {
                    Exception e = exceptions.get(i);
                    LOGGER.log(ExceptionUtils.causedByInterrupt(e) ? Level.DEBUG : Level.WARN,
                            "Task failed with exception"
                                    + (exceptions.size() > 1 ? "s (" + (i + 1) + "/" + exceptions.size() + ")" : ""),
                            e);
                }
            }
            ExceptionUtils.setNodeIds(exceptions, ncs.getId());
            ncs.getWorkQueue()
                    .schedule(new NotifyTaskFailureWork(ncs, this, exceptions, joblet.getJobId(), taskAttemptId));
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
                if (inputChannels.isEmpty()) {
                    joblet.advertisePartitionRequest(taskAttemptId, collector.getRequiredPartitionIds(), collector,
                            PartitionState.STARTED);
                } else {
                    collector.addPartitions(inputChannels);
                }
                IFrameReader reader = collector.getReader();
                reader.open();
                try {
                    try {
                        writer.open();
                        VSizeFrame frame = new VSizeFrame(this);
                        while (reader.nextFrame(frame)) {
                            if (aborted) {
                                return;
                            }
                            ByteBuffer buffer = frame.getBuffer();
                            writer.nextFrame(buffer);
                            buffer.compact();
                        }
                    } catch (Exception e) {
                        try {
                            writer.fail();
                        } catch (HyracksDataException e1) {
                            e.addSuppressed(e1);
                        }
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
        } catch (Exception e) {
            throw HyracksDataException.create(e);
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
    public IResultPartitionManager getResultPartitionManager() {
        return ncs.getResultPartitionManager();
    }

    @Override
    public void sendApplicationMessageToCC(byte[] message, DeploymentId deploymentId) throws Exception {
        this.ncs.sendApplicationMessageToCC(getJobletContext().getJobId().getCcId(), message, deploymentId);
    }

    @Override
    public void sendApplicationMessageToCC(Serializable message, DeploymentId deploymentId) throws Exception {
        this.ncs.sendApplicationMessageToCC(getJobletContext().getJobId().getCcId(),
                JavaSerializationUtils.serialize(message), deploymentId);
    }

    @Override
    public void setSharedObject(Object object) {
        this.sharedObject = object;
    }

    @Override
    public Object getSharedObject() {
        return sharedObject;
    }

    @Override
    public byte[] getJobParameter(byte[] name, int start, int length) throws HyracksException {
        return ncs.createOrGetJobParameterByteStore(joblet.getJobId()).getParameterValue(name, start, length);
    }

    @Override
    public Set<JobFlag> getJobFlags() {
        return jobFlags;
    }

    @Override
    public IStatsCollector getStatsCollector() {
        return statsCollector;
    }

    @Override
    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }

    @Override
    public IThreadStats getThreadStats() {
        synchronized (threadStatsCollectors) {
            return perThreadStats.computeIfAbsent(Thread.currentThread().getId(), threadId -> new ThreadStats());
        }
    }

    @Override
    public synchronized void subscribeThreadToStats(IThreadStatsCollector threadStatsCollector) {
        //TODO do this only when profiling is enabled
        synchronized (threadStatsCollectors) {
            threadStatsCollectors.add(threadStatsCollector);
            final long threadId = Thread.currentThread().getId();
            IThreadStats threadStat = perThreadStats.computeIfAbsent(threadId, id -> new ThreadStats());
            threadStatsCollector.subscribe(threadStat);
        }
    }

    @Override
    public synchronized void unsubscribeThreadFromStats() {
        synchronized (threadStatsCollectors) {
            threadStatsCollectors.forEach(IThreadStatsCollector::unsubscribe);
        }
    }

    public boolean isCompleted() {
        return completed;
    }

    public Set<Warning> getWarnings() {
        return warnings;
    }

    private IWarningCollector createWarningCollector(long maxWarnings) {
        return new IWarningCollector() {

            private final AtomicLong warningsCount = new AtomicLong();

            @Override
            public void warn(Warning warning) {
                warnings.add(warning);
            }

            @Override
            public boolean shouldWarn() {
                long currentCount = warningsCount.getAndUpdate(count -> count < Long.MAX_VALUE ? count + 1 : count);
                return currentCount < maxWarnings;
            }

            @Override
            public long getTotalWarningsCount() {
                return warningsCount.get();
            }
        };
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\", \"node\" : \"" + ncs.getId() + "\" \"jobId\" : \""
                + joblet.getJobId() + "\", \"taskId\" : \"" + taskAttemptId + "\" }";
    }
}
