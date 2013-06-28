/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.test.support;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;

public class TestJobletContext implements IHyracksJobletContext {
    private final int frameSize;
    private final INCApplicationContext appContext;
    private JobId jobId;
    private WorkspaceFileFactory fileFactory;

    public TestJobletContext(int frameSize, INCApplicationContext appContext, JobId jobId) throws HyracksException {
        this.frameSize = frameSize;
        this.appContext = appContext;
        this.jobId = jobId;
        fileFactory = new WorkspaceFileFactory(this, (IOManager) getIOManager());
    }

    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(frameSize);
    }

    public int getFrameSize() {
        return frameSize;
    }

    public IIOManager getIOManager() {
        return appContext.getRootContext().getIOManager();
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
    public INCApplicationContext getApplicationContext() {
        return appContext;
    }

    @Override
    public JobId getJobId() {
        return jobId;
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