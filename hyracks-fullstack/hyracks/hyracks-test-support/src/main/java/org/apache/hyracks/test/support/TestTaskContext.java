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
package org.apache.hyracks.test.support;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.control.common.job.profiling.StatsCollector;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.util.IThreadStats;
import org.apache.hyracks.util.IThreadStatsCollector;
import org.apache.hyracks.util.ThreadStats;

public class TestTaskContext implements IHyracksTaskContext {
    private final TestJobletContext jobletContext;
    private final TaskAttemptId taskId;
    private WorkspaceFileFactory fileFactory;
    private Map<Object, IStateObject> stateObjectMap = new HashMap<>();
    private Object sharedObject;
    private final IStatsCollector statsCollector = new StatsCollector();
    private final ThreadStats threadStats = new ThreadStats();

    public TestTaskContext(TestJobletContext jobletContext, TaskAttemptId taskId) {
        this.jobletContext = jobletContext;
        this.taskId = taskId;
        fileFactory = new WorkspaceFileFactory(this, getIoManager());
    }

    @Override
    public ByteBuffer allocateFrame() throws HyracksDataException {
        return jobletContext.allocateFrame();
    }

    @Override
    public ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
        return jobletContext.allocateFrame(bytes);
    }

    @Override
    public ByteBuffer reallocateFrame(ByteBuffer tobeDeallocate, int newSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        return jobletContext.reallocateFrame(tobeDeallocate, newSizeInBytes, copyOldData);

    }

    @Override
    public void deallocateFrames(int bytes) {
        jobletContext.deallocateFrames(bytes);
    }

    @Override
    public int getInitialFrameSize() {
        return jobletContext.getFrameSize();
    }

    @Override
    public IIOManager getIoManager() {
        return jobletContext.getIOManager();
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
    public IHyracksJobletContext getJobletContext() {
        return jobletContext;
    }

    @Override
    public ICounterContext getCounterContext() {
        return new CounterContext(jobletContext.getJobId() + "." + taskId);
    }

    @Override
    public void registerDeallocatable(final IDeallocatable deallocatable) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                deallocatable.deallocate();
            }
        });
    }

    @Override
    public TaskAttemptId getTaskAttemptId() {
        return taskId;
    }

    @Override
    public synchronized void setStateObject(IStateObject taskState) {
        stateObjectMap.put(taskState.getId(), taskState);
    }

    @Override
    public synchronized IStateObject getStateObject(Object id) {
        return stateObjectMap.get(id);
    }

    @Override
    public IResultPartitionManager getResultPartitionManager() {
        return null;
    }

    @Override
    public void sendApplicationMessageToCC(byte[] message, DeploymentId deploymentId) {

    }

    @Override
    public void sendApplicationMessageToCC(Serializable message, DeploymentId deploymentId) {
    }

    @Override
    public ExecutorService getExecutorService() {
        return null;
    }

    @Override
    public void setSharedObject(Object object) {
        sharedObject = object;
    }

    @Override
    public Object getSharedObject() {
        return sharedObject;
    }

    @Override
    public byte[] getJobParameter(byte[] name, int start, int length) {
        return new byte[0];
    }

    public Set<JobFlag> getJobFlags() {
        return EnumSet.noneOf(JobFlag.class);
    }

    @Override
    public IStatsCollector getStatsCollector() {
        return statsCollector;
    }

    @Override
    public IWarningCollector getWarningCollector() {
        return TestUtils.NOOP_WARNING_COLLECTOR;
    }

    @Override
    public void subscribeThreadToStats(IThreadStatsCollector threadStatsCollector) {
        // no op
    }

    @Override
    public void unsubscribeThreadFromStats() {
        // no op
    }

    @Override
    public IThreadStats getThreadStats() {
        return threadStats;
    }
}
