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
package org.apache.hyracks.control.nc.result;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.ResultSetPartitionId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

    private IFileHandle fileHandle;

    private volatile int referenceCount = 0;

    private long size;

    private long persistentSize;
    private long remainingReads;

    ResultState(ResultSetPartitionId resultSetPartitionId, boolean asyncMode, IIOManager ioManager,
            IWorkspaceFileFactory fileFactory, int frameSize, long maxReads) {
        if (maxReads <= 0) {
            throw new IllegalArgumentException("maxReads must be > 0");
        }
        this.resultSetPartitionId = resultSetPartitionId;
        this.asyncMode = asyncMode;
        this.ioManager = ioManager;
        this.fileFactory = fileFactory;
        this.frameSize = frameSize;
        remainingReads = maxReads;
        eos = new AtomicBoolean(false);
        failed = new AtomicBoolean(false);
        localPageList = new ArrayList<>();

        fileRef = null;
        fileHandle = null;
    }

    public synchronized void open() {
        size = 0;
        persistentSize = 0;
        referenceCount = 0;
    }

    public synchronized void close() {
        eos.set(true);
        closeWriteFileHandle();
        notifyAll();
    }

    public synchronized void closeAndDelete() {
        // Deleting a job is equivalent to aborting the job for all practical purposes, so the same action, needs
        // to be taken when there are more requests to these result states.
        failed.set(true);
        closeWriteFileHandle();
        if (fileRef != null) {
            fileRef.delete();
            fileRef = null;
        }
    }

    private void closeWriteFileHandle() {
        if (fileHandle != null) {
            doCloseFileHandle();
        }
    }

    private void doCloseFileHandle() {
        if (--referenceCount == 0) {
            // close the file if there is no more reference
            try {
                ioManager.close(fileHandle);
            } catch (IOException e) {
                // Since file handle could not be closed, just ignore.
            }
            fileHandle = null;
        }
    }

    public synchronized void write(ByteBuffer buffer) throws HyracksDataException {
        if (fileRef == null) {
            initWriteFileHandle();
        }

        size += ioManager.syncWrite(fileHandle, size, buffer);
        notifyAll();
    }

    public synchronized void write(ResultMemoryManager resultMemoryManager, ByteBuffer buffer)
            throws HyracksDataException {
        int srcOffset = 0;
        Page destPage = null;

        if (!localPageList.isEmpty()) {
            destPage = localPageList.get(localPageList.size() - 1);
        }

        while (srcOffset < buffer.limit()) {
            if ((destPage == null) || (destPage.getBuffer().remaining() <= 0)) {
                destPage = resultMemoryManager.requestPage(resultSetPartitionId, this);
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
        if (isExhausted()) {
            throw new IllegalStateException("Result reads exhausted");
        }
        remainingReads--;
    }

    public synchronized void readClose() throws HyracksDataException {
        if (fileHandle != null) {
            doCloseFileHandle();
        }
    }

    public synchronized long read(long offset, ByteBuffer buffer) throws HyracksDataException {
        long readSize = 0;

        while (offset >= size && !eos.get() && !failed.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }
        if ((offset >= size && eos.get()) || failed.get()) {
            return readSize;
        }

        if (fileHandle == null) {
            initReadFileHandle();
        }
        readSize = ioManager.syncRead(fileHandle, offset, buffer);

        return readSize;
    }

    public synchronized long read(ResultMemoryManager resultMemoryManager, long offset, ByteBuffer buffer)
            throws HyracksDataException {
        long readSize = 0;
        while (offset >= size && !eos.get() && !failed.get()) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }

        if ((offset >= size && eos.get()) || failed.get()) {
            return readSize;
        }

        if (offset < persistentSize) {
            if (fileHandle == null) {
                initReadFileHandle();
            }
            readSize = ioManager.syncRead(fileHandle, offset, buffer);
            if (readSize < 0) {
                throw new HyracksDataException("Premature end of file");
            }
        }

        if (readSize < buffer.capacity()) {
            long localPageOffset = offset - persistentSize;
            int localPageIndex = (int) (localPageOffset / ResultMemoryManager.getPageSize());
            int pageOffset = (int) (localPageOffset % ResultMemoryManager.getPageSize());
            Page page = getPage(localPageIndex);
            if (page == null) {
                return readSize;
            }
            readSize += buffer.remaining();
            buffer.put(page.getBuffer().array(), pageOffset, buffer.remaining());
        }
        resultMemoryManager.pageReferenced(resultSetPartitionId);
        return readSize;
    }

    public synchronized void abort() {
        failed.set(true);
        notifyAll();
    }

    public synchronized Page returnPage() throws HyracksDataException {
        Page page = removePage();

        // If we do not have any pages to be given back close the write channel since we don't write any more, return
        // null.
        if (page == null) {
            ioManager.close(fileHandle);
            return null;
        }

        page.getBuffer().flip();

        if (fileRef == null) {
            initWriteFileHandle();
        }

        long delta = ioManager.syncWrite(fileHandle, persistentSize, page.getBuffer());
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

    private void initWriteFileHandle() throws HyracksDataException {
        if (fileHandle == null) {
            String fName = FILE_PREFIX + String.valueOf(resultSetPartitionId.getPartition());
            fileRef = fileFactory.createUnmanagedWorkspaceFile(fName);
            fileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            if (referenceCount != 0) {
                throw new IllegalStateException("Illegal reference count " + referenceCount);
            }
            referenceCount = 1;
            notifyAll(); // NOSONAR: always called from a synchronized block
        }
    }

    private void initReadFileHandle() throws HyracksDataException {
        while (fileRef == null && !failed.get()) {
            // wait for writer to create the file
            try {
                wait();
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }
        if (failed.get()) {
            return;
        }
        if (fileHandle == null) {
            // fileHandle has been closed by the writer, create it again
            fileHandle = ioManager.open(fileRef, IIOManager.FileReadWriteMode.READ_ONLY,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
        }
        referenceCount++;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper om = new ObjectMapper();
            ObjectNode on = om.createObjectNode();
            on.put("rspid", resultSetPartitionId.toString());
            on.put("async", asyncMode);
            on.put("remainingReads", remainingReads);
            on.put("eos", eos.get());
            on.put("failed", failed.get());
            on.put("fileRef", String.valueOf(fileRef));
            return om.writer(new MinimalPrettyPrinter()).writeValueAsString(on);
        } catch (JsonProcessingException e) { // NOSONAR
            return e.getMessage();
        }
    }

    public synchronized boolean isExhausted() {
        return remainingReads == 0;
    }
}
