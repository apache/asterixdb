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
package edu.uci.ics.hyracks.control.nc.partitions;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.api.partitions.PartitionId;

public class PipelinedPartition implements IFrameWriter, IPartition {
    private final PartitionManager manager;

    private final PartitionId pid;

    private IFrameWriter delegate;

    public PipelinedPartition(PartitionManager manager, PartitionId pid) {
        this.manager = manager;
        this.pid = pid;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void deallocate() {
        // do nothing
    }

    @Override
    public synchronized void writeTo(IFrameWriter writer) {
        delegate = writer;
        notifyAll();
    }

    @Override
    public synchronized void open() throws HyracksDataException {
        manager.registerPartition(pid, this);
        while (delegate == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        delegate.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        delegate.nextFrame(buffer);
    }

    @Override
    public void flush() throws HyracksDataException {
        delegate.flush();
    }

    @Override
    public void close() throws HyracksDataException {
        delegate.close();
    }
}