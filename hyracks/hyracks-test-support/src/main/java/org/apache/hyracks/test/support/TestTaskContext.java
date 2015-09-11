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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataset.IDatasetPartitionManager;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;

public class TestTaskContext implements IHyracksTaskContext {
    private final TestJobletContext jobletContext;
    private final TaskAttemptId taskId;
    private WorkspaceFileFactory fileFactory;

    public TestTaskContext(TestJobletContext jobletContext, TaskAttemptId taskId) throws HyracksException {
        this.jobletContext = jobletContext;
        this.taskId = taskId;
        fileFactory = new WorkspaceFileFactory(this, (IOManager) getIOManager());
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
        return jobletContext.reallocateFrame(tobeDeallocate,newSizeInBytes, copyOldData);

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
    public IIOManager getIOManager() {
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
    public void setStateObject(IStateObject taskState) {

    }

    @Override
    public IStateObject getStateObject(Object id) {
        return null;
    }

    @Override
    public IDatasetPartitionManager getDatasetPartitionManager() {
        return null;
    }

    @Override
    public void sendApplicationMessageToCC(byte[] message, DeploymentId deploymentId, String nodeId) throws Exception {
        // TODO Auto-generated method stub

    }
}
