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
package edu.uci.ics.hyracks.control.nc.dataset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.ResultSetPartitionId;

public class ResultState implements IStateObject {
    private static final String FILE_PREFIX = "result_";

    private final ResultSetPartitionId resultSetPartitionId;

    private final boolean asyncMode;

    private final int frameSize;

    private final IIOManager ioManager;

    private final IWorkspaceFileFactory fileFactory;

    private final AtomicBoolean eos;

    private final AtomicBoolean failed;

    private final List<Page> localPageList;

    private FileReference fileRef;

    private IFileHandle writeFileHandle;

    private IFileHandle readFileHandle;

    private long size;

    private long persistentSize;

    ResultState(ResultSetPartitionId resultSetPartitionId, boolean asyncMode, IIOManager ioManager,
            IWorkspaceFileFactory fileFactory, int frameSize) {
        this.resultSetPartitionId = resultSetPartitionId;
        this.asyncMode = asyncMode;
        this.ioManager = ioManager;
        this.fileFactory = fileFactory;
        this.frameSize = frameSize;
        eos = new AtomicBoolean(false);
        failed = new AtomicBoolean(false);
        localPageList = new ArrayList<Page>();

        fileRef = null;
        writeFileHandle = null;
    }

    public synchronized void open() {
        size = 0;
        persistentSize = 0;
    }

    public synchronized void close() {
        eos.set(true);
        notifyAll();
    }

    public synchronized void closeAndDelete() {
        // Deleting a job is equivalent to aborting the job for all practical purposes, so the same action, needs
        // to be taken when there are more requests to these result states.
        failed.set(true);
        if (writeFileHandle != null) {
            try {
                ioManager.close(writeFileHandle);
            } catch (IOException e) {
                // Since file handle could not be closed, just ignore.
            }
        }
        if (fileRef != null) {
            fileRef.delete();
        }
    }

    public synchronized void write(ByteBuffer buffer) throws HyracksDataException {
        if (fileRef == null) {
            String fName = FILE_PREFIX + String.valueOf(resultSetPartitionId.getPartition());
            fileRef = fileFactory.createUnmanagedWorkspaceFile(fName);
            writeFileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        }

        size += ioManager.syncWrite(writeFileHandle, size, buffer);

        notifyAll();
    }

    public synchronized void write(DatasetMemoryManager datasetMemoryManager, ByteBuffer buffer)
            throws HyracksDataException {
        int srcOffset = 0;
        Page destPage = null;

        if (!localPageList.isEmpty()) {
            destPage = localPageList.get(localPageList.size() - 1);
        }

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

    public synchronized void readOpen() {
        // It is a noOp for now, leaving here to keep the API stable for future usage.
    }

    public synchronized void readClose() throws HyracksDataException {
        if (readFileHandle != null) {
            ioManager.close(readFileHandle);
        }
    }

    public synchronized long read(long offset, ByteBuffer buffer) throws HyracksDataException {
        long readSize = 0;

        while (offset >= size && !eos.get() && !failed.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        if ((offset >= size && eos.get()) || failed.get()) {
            return readSize;
        }

        if (readFileHandle == null) {
            initReadFileHandle();
        }
        readSize = ioManager.syncRead(readFileHandle, offset, buffer);

        return readSize;
    }

    public long read(DatasetMemoryManager datasetMemoryManager, long offset, ByteBuffer buffer)
            throws HyracksDataException {
        long readSize = 0;
        synchronized (this) {
            while (offset >= size && !eos.get() && !failed.get()) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                }
            }

            if ((offset >= size && eos.get()) || failed.get()) {
                return readSize;
            }

            if (offset < persistentSize) {
                if (readFileHandle == null) {
                    initReadFileHandle();
                }
                readSize = ioManager.syncRead(readFileHandle, offset, buffer);
            }

            if (readSize < buffer.capacity()) {
                long localPageOffset = offset - persistentSize;
                int localPageIndex = (int) (localPageOffset / DatasetMemoryManager.getPageSize());
                int pageOffset = (int) (localPageOffset % DatasetMemoryManager.getPageSize());
                Page page = getPage(localPageIndex);
                if (page == null) {
                    return readSize;
                }
                readSize += buffer.remaining();
                buffer.put(page.getBuffer().array(), pageOffset, buffer.remaining());
            }
        }
        datasetMemoryManager.pageReferenced(resultSetPartitionId);
        return readSize;
    }

    public synchronized void abort() {
        failed.set(true);
        notifyAll();
    }

    public synchronized Page returnPage() throws HyracksDataException {
        Page page = removePage();

        // If we do not have any pages to be given back close the write channel since we don't write any more, return null.
        if (page == null) {
            ioManager.close(writeFileHandle);
            return null;
        }

        page.getBuffer().flip();

        if (fileRef == null) {
            String fName = FILE_PREFIX + String.valueOf(resultSetPartitionId.getPartition());
            fileRef = fileFactory.createUnmanagedWorkspaceFile(fName);
            writeFileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            notifyAll();
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

    public boolean getAsyncMode() {
        return asyncMode;
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

    private Page removePage() {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.remove(localPageList.size() - 1);
        }
        return page;
    }

    private void initReadFileHandle() throws HyracksDataException {
        while (fileRef == null && !failed.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        if (failed.get()) {
            return;
        }

        readFileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_ONLY,
                IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
    }
}