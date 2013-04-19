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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.dataset.Page;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.ResultSetPartitionId;

public class ResultState implements IStateObject {
    private final ResultSetPartitionId resultSetPartitionId;

    private final int frameSize;

    private final IIOManager ioManager;

    private final AtomicBoolean eos;

    private final List<Page> localPageList;

    private FileReference fileRef;

    private IFileHandle writeFileHandle;

    private IFileHandle readFileHandle;

    private long size;

    private long persistentSize;

    ResultState(ResultSetPartitionId resultSetPartitionId, IIOManager ioManager, int frameSize) {
        this.resultSetPartitionId = resultSetPartitionId;
        this.ioManager = ioManager;
        this.frameSize = frameSize;
        eos = new AtomicBoolean(false);
        localPageList = new ArrayList<Page>();

        writeFileHandle = null;
    }

    public synchronized void open(FileReference fileRef) throws HyracksDataException {
        this.fileRef = fileRef;

        size = 0;
        persistentSize = 0;
        notifyAll();
    }

    public synchronized void close() {
        eos.set(true);
        notifyAll();
    }

    public synchronized void closeAndDelete() {
        if (writeFileHandle != null) {
            try {
                ioManager.close(writeFileHandle);
            } catch (IOException e) {
                // Since file handle could not be closed, just ignore.
            }
        }
        fileRef.delete();
    }

    public synchronized void write(DatasetMemoryManager datasetMemoryManager, ByteBuffer buffer)
            throws HyracksDataException {
        int srcOffset = 0;
        Page destPage = getPage(localPageList.size() - 1);

        while (srcOffset < buffer.limit()) {
            if ((destPage == null) || (destPage.getBuffer().remaining() <= 0)) {
                destPage = datasetMemoryManager.requestPage(resultSetPartitionId, this);
                localPageList.add(destPage);
            }
            int srcLength = Math.min(buffer.limit() - srcOffset, destPage.getBuffer().remaining());
            destPage.getBuffer().put(buffer.array(), srcOffset, srcLength);
            srcOffset += srcLength;
            size += srcLength;
        }

        notifyAll();
    }

    public synchronized void readOpen() throws InterruptedException, HyracksDataException {
        while (fileRef == null) {
            wait();
        }
        readFileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_ONLY,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
    }

    public synchronized void readClose() throws InterruptedException, HyracksDataException {
        while (fileRef == null) {
            wait();
        }
        readFileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_ONLY,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
    }

    public synchronized long read(DatasetMemoryManager datasetMemoryManager, long offset, ByteBuffer buffer)
            throws HyracksDataException {
        long readSize = 0;
        synchronized (this) {
            while (offset >= size && !eos.get()) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }
        }

        if (offset >= size && eos.get()) {
            return readSize;
        }

        if (offset < persistentSize) {
            readSize = ioManager.syncRead(readFileHandle, offset, buffer);
        }

        if (readSize < buffer.capacity()) {
            long localPageOffset = offset - persistentSize;
            int localPageIndex = (int) (localPageOffset / datasetMemoryManager.getPageSize());
            int pageOffset = (int) (localPageOffset % datasetMemoryManager.getPageSize());
            Page page = getPage(localPageIndex);
            if (page == null) {
                return readSize;
            }
            readSize += buffer.remaining();
            buffer.put(page.getBuffer().array(), pageOffset, buffer.remaining());
        }

        datasetMemoryManager.pageReferenced(resultSetPartitionId);
        return readSize;
    }

    public synchronized Page returnPage() throws HyracksDataException {
        Page page = removePage(0);

        // If we do not have any pages to be given back close the write channel since we don't write any more, return null.
        if (page == null) {
            ioManager.close(writeFileHandle);
            return null;
        }

        page.getBuffer().flip();

        if (writeFileHandle == null) {
            writeFileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        }

        long delta = ioManager.syncWrite(writeFileHandle, persistentSize, page.getBuffer());
        persistentSize += delta;
        return page;
    }

    public synchronized void setEOS(boolean eos) {
        this.eos.set(eos);
    }

    public ResultSetPartitionId getResultSetPartitionId() {
        return resultSetPartitionId;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public IIOManager getIOManager() {
        return ioManager;
    }

    @Override
    public JobId getJobId() {
        return resultSetPartitionId.getJobId();
    }

    @Override
    public Object getId() {
        return resultSetPartitionId;
    }

    @Override
    public long getMemoryOccupancy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    private Page getPage(int index) {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.get(index);
        }
        return page;
    }

    private Page removePage(int index) {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.remove(index);
        }
        return page;
    }
}