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
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class Task implements IHyracksTaskContext, ICounterContext, Runnable {
    private final Joblet joblet;

    private final TaskAttemptId taskAttemptId;

    private final String displayName;

    private final IWorkspaceFileFactory fileFactory;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final Map<String, Counter> counterMap;

    private IPartitionCollector collector;

    private IOperatorNodePushable operator;

    private volatile boolean aborted;

    public Task(Joblet joblet, TaskAttemptId taskId, String displayName) {
        this.joblet = joblet;
        this.taskAttemptId = taskId;
        this.displayName = displayName;
        fileFactory = new WorkspaceFileFactory(this, (IOManager) joblet.getIOManager());
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        counterMap = new HashMap<String, Counter>();
    }

    public void setTaskRuntime(IPartitionCollector collector, IOperatorNodePushable operator) {
        this.collector = collector;
        this.operator = operator;
    }

    @Override
    public ByteBuffer allocateFrame() {
        return joblet.allocateFrame();
    }

    @Override
    public int getFrameSize() {
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

    public synchronized void dumpProfile(TaskProfile tProfile) {
        Map<String, Long> dumpMap = tProfile.getCounters();
        for (Counter c : counterMap.values()) {
            dumpMap.put(c.getName(), c.get());
        }
    }

    public void start() throws HyracksException {
        aborted = false;
        joblet.getExecutor().execute(this);
    }

    public void abort() {
        aborted = true;
        if (collector != null) {
            collector.abort();
        }
    }

    @Override
    public void run() {
        Thread ct = Thread.currentThread();
        String threadName = ct.getName();
        try {
            ct.setName(displayName + ": " + taskAttemptId);
            operator.initialize();
            try {
                if (collector != null) {
                    if (aborted) {
                        return;
                    }
                    collector.open();
                    try {
                        joblet.advertisePartitionRequest(collector.getRequiredPartitionIds(), collector);
                        IFrameReader reader = collector.getReader();
                        reader.open();
                        try {
                            IFrameWriter writer = operator.getInputFrameWriter(0);
                            writer.open();
                            try {
                                ByteBuffer buffer = allocateFrame();
                                while (reader.nextFrame(buffer)) {
                                    if (aborted) {
                                        return;
                                    }
                                    buffer.flip();
                                    writer.nextFrame(buffer);
                                    buffer.compact();
                                }
                            } finally {
                                writer.close();
                            }
                        } finally {
                            reader.close();
                        }
                    } finally {
                        collector.close();
                    }
                }
            } finally {
                operator.deinitialize();
            }
            joblet.notifyTaskComplete(this);
        } catch (Exception e) {
            e.printStackTrace();
            joblet.notifyTaskFailed(this, e);
        } finally {
            ct.setName(threadName);
            close();
        }
    }
}