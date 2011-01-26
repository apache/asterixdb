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
package edu.uci.ics.hyracks.test.support;

import java.nio.ByteBuffer;
import java.util.UUID;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.ManagedWorkspaceFileFactory;

public class TestStageletContext implements IHyracksStageletContext {
    private final IHyracksJobletContext jobletContext;
    private UUID stageId;
    private ManagedWorkspaceFileFactory fileFactory;

    public TestStageletContext(IHyracksJobletContext jobletContext, UUID stageId) throws HyracksException {
        this.jobletContext = jobletContext;
        this.stageId = stageId;
        fileFactory = new ManagedWorkspaceFileFactory(this, (IOManager) getIOManager());
    }

    @Override
    public ByteBuffer allocateFrame() {
        return jobletContext.allocateFrame();
    }

    @Override
    public int getFrameSize() {
        return jobletContext.getFrameSize();
    }

    @Override
    public IIOManager getIOManager() {
        return jobletContext.getIOManager();
    }

    @Override
    public FileReference createWorkspaceFile(String prefix) throws HyracksDataException {
        return fileFactory.createWorkspaceFile(prefix);
    }

    @Override
    public IHyracksJobletContext getJobletContext() {
        return jobletContext;
    }

    @Override
    public UUID getStageId() {
        return stageId;
    }

    @Override
    public ICounterContext getCounterContext() {
        return new CounterContext(jobletContext.getJobId() + "." + jobletContext.getAttempt() + "." + stageId);
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
}