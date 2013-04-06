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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.dataset.Page;
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

    private final AtomicBoolean readEOS;

    private final List<Page> localPageList;

    private FileReference fileRef;

    private IFileHandle writeFileHandle;

    private long size;

    private long persistentSize;

    ResultState(ResultSetPartitionId resultSetPartitionId, IIOManager ioManager, int frameSize) {
        this.resultSetPartitionId = resultSetPartitionId;
        this.ioManager = ioManager;
        this.frameSize = frameSize;
        eos = new AtomicBoolean(false);
        readEOS = new AtomicBoolean(false);
        localPageList = new ArrayList<Page>();
    }

    public synchronized void init(FileReference fileRef, IFileHandle writeFileHandle) {
        this.fileRef = fileRef;
        this.writeFileHandle = writeFileHandle;

        size = 0;
        persistentSize = 0;
        notifyAll();
    }

    public synchronized void deinit() {
        if (writeFileHandle != null) {
            try {
                ioManager.close(writeFileHandle);
            } catch (IOException e) {
                // Since file handle could not be closed, just ignore.
            }
        }
        fileRef.delete();
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

    public synchronized void incrementSize(long delta) {
        size += delta;
    }

    public synchronized long getSize() {
        return size;
    }

    public synchronized void incrementPersistentSize(long delta) {
        persistentSize += delta;
    }

    public synchronized long getPersistentSize() {
        return persistentSize;
    }

    public void setEOS(boolean eos) {
        this.eos.set(eos);
    }

    public boolean getEOS() {
        return eos.get();
    }

    public boolean getReadEOS() {
        return readEOS.get();
    }

    public synchronized void addPage(Page page) {
        localPageList.add(page);
    }

    public synchronized Page removePage(int index) {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.remove(index);
        }
        return page;
    }

    public synchronized Page getPage(int index) {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.get(index);
        }
        return page;
    }

    public synchronized Page getLastPage() {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.get(localPageList.size() - 1);
        }
        return page;
    }

    public synchronized Page getFirstPage() {
        Page page = null;
        if (!localPageList.isEmpty()) {
            page = localPageList.get(0);
        }
        return page;
    }

    public synchronized FileReference getValidFileReference() throws InterruptedException {
        while (fileRef == null)
            wait();
        return fileRef;
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
}