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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.naming.MultipartName;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.api.workflow.variables.WorkflowVariableDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.common.job.profiling.counters.Counter;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class Task implements IHyracksTaskContext, ICounterContext, Runnable {
    private final Joblet joblet;

    private final TaskAttemptId taskAttemptId;

    private final String displayName;

    private final Executor executor;

    private final IWorkspaceFileFactory fileFactory;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final Map<String, Counter> counterMap;

    private final Map<MultipartName, Object> inputGlobalVariables;

    private final Map<MultipartName, Object> outputVariables;

    private final Map<MultipartName, WorkflowVariableDescriptor> outputVariableDescriptorMap;

    private IPartitionCollector[] collectors;

    private IOperatorNodePushable operator;

    private volatile boolean aborted;

    public Task(Joblet joblet, TaskAttemptId taskId, String displayName,
            Executor executor) {
        this.joblet = joblet;
        this.taskAttemptId = taskId;
        this.displayName = displayName;
        this.executor = executor;
        fileFactory = new WorkspaceFileFactory(this, (IOManager) joblet.getIOManager());
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        counterMap = new HashMap<String, Counter>();
//        this.inputGlobalVariables = inputGlobalVariables;
        inputGlobalVariables = Collections.emptyMap();
        outputVariables = new HashMap<MultipartName, Object>();
        outputVariableDescriptorMap = new HashMap<MultipartName, WorkflowVariableDescriptor>();
//        for (WorkflowVariableDescriptor wvd : outputVariableDescriptors) {
//            outputVariableDescriptorMap.put(wvd.getName(), wvd);
//        }
    }

    public void setTaskRuntime(IPartitionCollector[] collectors, IOperatorNodePushable operator) {
        this.collectors = collectors;
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

    @Override
    public Object lookupGlobalVariable(ActivityId producerActivity, int partition, String varName)
            throws HyracksDataException {
        MultipartName var = new MultipartName(producerActivity, partition, varName);
        if (!inputGlobalVariables.containsKey(var)) {
            throw new HyracksDataException("Unknown Variable: " + var);
        }
        return inputGlobalVariables.get(var);
    }

    @Override
    public Object lookupLocalVariable(ActivityId producerActivity, int partition, String varName)
            throws HyracksDataException {
        return joblet.lookupLocalVariable(new MultipartName(producerActivity, partition, varName));
    }

    @Override
    public void setVariable(String name, Object value) {
        outputVariables.put(new MultipartName(taskAttemptId.getTaskId().getActivityId(), taskAttemptId.getTaskId()
                .getPartition(), name), value);
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
        for (IPartitionCollector c : collectors) {
            c.abort();
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
                if (collectors.length > 0) {
                    final Semaphore sem = new Semaphore(collectors.length - 1);
                    for (int i = 1; i < collectors.length; ++i) {
                        final IPartitionCollector collector = collectors[i];
                        final IFrameWriter writer = operator.getInputFrameWriter(i);
                        sem.acquire();
                        executor.execute(new Runnable() {
                            public void run() {
                                try {
                                    pushFrames(collector, writer);
                                } catch (HyracksDataException e) {
                                } finally {
                                    sem.release();
                                }
                            }
                        });
                    }
                    try {
                        pushFrames(collectors[0], operator.getInputFrameWriter(0));
                    } finally {
                        sem.acquire(collectors.length - 1);
                    }
                }
            } finally {
                operator.deinitialize();
            }
            Map<MultipartName, Object> outputGlobalVariables = new HashMap<MultipartName, Object>();
            for (Map.Entry<MultipartName, Object> e : outputVariables.entrySet()) {
                MultipartName varName = e.getKey();
                WorkflowVariableDescriptor wvd = outputVariableDescriptorMap.get(varName);
                if (wvd == null) {
                    throw new HyracksDataException("Unknown variable found: " + varName);
                }
                switch (wvd.getScope()) {
                    case LOCAL:
                        joblet.setLocalVariable(varName, e.getValue());
                        break;

                    case GLOBAL:
                        outputGlobalVariables.put(varName, e.getValue());
                        break;
                }
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

    private void pushFrames(IPartitionCollector collector, IFrameWriter writer) throws HyracksDataException {
        if (aborted) {
            return;
        }
        try {
            collector.open();
            try {
                joblet.advertisePartitionRequest(taskAttemptId, collector.getRequiredPartitionIds(), collector,
                        PartitionState.STARTED);
                IFrameReader reader = collector.getReader();
                reader.open();
                try {
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
        } catch (HyracksException e) {
            throw new HyracksDataException(e);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}