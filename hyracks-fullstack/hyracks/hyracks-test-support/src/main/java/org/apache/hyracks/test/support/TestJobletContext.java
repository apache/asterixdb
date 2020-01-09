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

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.profiling.counters.ICounterContext;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;

public class TestJobletContext implements IHyracksJobletContext {

    private final INCServiceContext serviceContext;
    private final FrameManager frameManger;
    private final JobId jobId;
    private final WorkspaceFileFactory fileFactory;
    private final long jobStartTime;

    TestJobletContext(int frameSize, INCServiceContext serviceContext, JobId jobId) throws HyracksException {
        this.serviceContext = serviceContext;
        this.jobId = jobId;
        fileFactory = new WorkspaceFileFactory(this, getIoManager());
        this.frameManger = new FrameManager(frameSize);
        this.jobStartTime = System.currentTimeMillis();
    }

    @Override
    public ByteBuffer allocateFrame() throws HyracksDataException {
        return frameManger.allocateFrame();
    }

    @Override
    public ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
        return frameManger.allocateFrame(bytes);
    }

    @Override
    public ByteBuffer reallocateFrame(ByteBuffer tobeDeallocate, int newFrameSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        return frameManger.reallocateFrame(tobeDeallocate, newFrameSizeInBytes, copyOldData);
    }

    public IJobletEventListenerFactory getJobletEventListenerFactory() {
        return null;
    }

    @Override
    public void deallocateFrames(int bytes) {
        frameManger.deallocateFrames(bytes);
    }

    @Override
    public final int getInitialFrameSize() {
        return frameManger.getInitialFrameSize();
    }

    @Override
    public IIOManager getIoManager() {
        return serviceContext.getIoManager();
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
    public ICounterContext getCounterContext() {
        return new CounterContext(jobId.toString());
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
    public INCServiceContext getServiceContext() {
        return serviceContext;
    }

    @Override
    public JobId getJobId() {
        return jobId;
    }

    @Override
    public long getJobStartTime() {
        return jobStartTime;
    }

    @Override
    public Object getGlobalJobData() {
        return null;
    }

    @Override
    public Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ClassLoader getClassLoader() {
        return this.getClass().getClassLoader();
    }

}
