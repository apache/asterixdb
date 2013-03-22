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
package edu.uci.ics.hyracks.control.nc.dataset;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionWriter;
import edu.uci.ics.hyracks.api.dataset.Page;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.ResultSetPartitionId;

public class DatasetPartitionWriter implements IDatasetPartitionWriter {
    private static final Logger LOGGER = Logger.getLogger(DatasetPartitionWriter.class.getName());

    private static final String FILE_PREFIX = "result_";

    private final IDatasetPartitionManager manager;

    private final JobId jobId;

    private final ResultSetId resultSetId;

    private final int partition;

    private final DatasetMemoryManager datasetMemoryManager;

    private final ResultSetPartitionId resultSetPartitionId;

    private final ResultState resultState;

    private IFileHandle fileHandle;

    public DatasetPartitionWriter(IHyracksTaskContext ctx, IDatasetPartitionManager manager, JobId jobId,
            ResultSetId rsId, int partition, DatasetMemoryManager datasetMemoryManager) {
        this.manager = manager;
        this.jobId = jobId;
        this.resultSetId = rsId;
        this.partition = partition;
        this.datasetMemoryManager = datasetMemoryManager;

        resultSetPartitionId = new ResultSetPartitionId(jobId, rsId, partition);
        resultState = new ResultState(resultSetPartitionId, ctx.getIOManager(), ctx.getFrameSize());
    }

    public ResultState getResultState() {
        return resultState;
    }

    @Override
    public void open() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("open(" + partition + ")");
        }
        String fName = FILE_PREFIX + String.valueOf(partition);
        FileReference fRef = manager.getFileFactory().createUnmanagedWorkspaceFile(fName);
        fileHandle = resultState.getIOManager().open(fRef, IIOManager.FileReadWriteMode.READ_WRITE,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        resultState.init(fRef);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        int srcOffset = 0;
        Page destPage = resultState.getLastPage();

        while (srcOffset < buffer.limit()) {
            if ((destPage == null) || (destPage.getBuffer().remaining() <= 0)) {
                destPage = datasetMemoryManager.requestPage(resultSetPartitionId, this);
                resultState.addPage(destPage);
            }
            int srcLength = Math.min(buffer.limit() - srcOffset, destPage.getBuffer().remaining());
            destPage.getBuffer().put(buffer.array(), srcOffset, srcLength);
            srcOffset += srcLength;
            resultState.incrementSize(srcLength);
        }

        synchronized (resultState) {
            resultState.notifyAll();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        try {
            manager.reportPartitionFailure(jobId, resultSetId, partition);
        } catch (HyracksException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("close(" + partition + ")");
        }

        try {
            synchronized (resultState) {
                resultState.setEOS(true);
                resultState.notifyAll();
            }
            manager.reportPartitionWriteCompletion(jobId, resultSetId, partition);
        } catch (HyracksException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public Page returnPage() throws HyracksDataException {
        Page page = resultState.removePage(0);

        IIOManager ioManager = resultState.getIOManager();

        // If we do not have any pages to be given back close the write channel since we don't write any more, return null.
        if (page == null) {
            ioManager.close(fileHandle);
            return null;
        }

        page.getBuffer().flip();

        long delta = ioManager.syncWrite(fileHandle, resultState.getPersistentSize(), page.getBuffer());
        resultState.incrementPersistentSize(delta);
        return page;
    }
}
