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

package edu.uci.ics.pregelix.dataflow.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;

/**
 * The buffer to hold updates.
 * We do a batch update for the B-tree during index search and join so that
 * avoid to open/close cursors frequently.
 */
public class UpdateBuffer {

    private int currentInUse = 0;
    private final int pageLimit;
    private final List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    private final FrameTupleAppender appender;
    private final IHyracksTaskContext ctx;
    private final FrameTupleReference tuple = new FrameTupleReference();
    private final FrameTupleReference lastTuple = new FrameTupleReference();
    private final int frameSize;
    private IFrameTupleAccessor fta;

    public UpdateBuffer(int numPages, IHyracksTaskContext ctx, int fieldCount) throws HyracksDataException {
        this.appender = new FrameTupleAppender(ctx.getFrameSize());
        ByteBuffer buffer = ctx.allocateFrame();
        this.buffers.add(buffer);
        this.appender.reset(buffer, true);
        this.pageLimit = numPages;
        this.ctx = ctx;
        this.frameSize = ctx.getFrameSize();
        this.fta = new UpdateBufferTupleAccessor(frameSize, fieldCount);
    }

    public UpdateBuffer(IHyracksTaskContext ctx, int fieldCount) throws HyracksDataException {
        //by default, the update buffer has 1000 pages
        this(1000, ctx, fieldCount);
    }

    public void setFieldCount(int fieldCount) {
        if (fta.getFieldCount() != fieldCount) {
            this.fta = new UpdateBufferTupleAccessor(frameSize, fieldCount);
        }
    }

    public boolean appendTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            if (currentInUse + 1 < pageLimit) {
                // move to the new buffer
                currentInUse++;
                allocate(currentInUse);
                ByteBuffer buffer = buffers.get(currentInUse);
                appender.reset(buffer, true);

                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new HyracksDataException("tuple cannot be appended to a new frame!");
                }
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public void updateIndex(IIndexAccessor bta) throws HyracksDataException, IndexException {
        // batch update
        for (int i = 0; i <= currentInUse; i++) {
            ByteBuffer buffer = buffers.get(i);
            fta.reset(buffer);
            for (int j = 0; j < fta.getTupleCount(); j++) {
                tuple.reset(fta, j);
                try {
                    bta.update(tuple);
                } catch (TreeIndexNonExistentKeyException e) {
                    // ignore non-existent key exception
                    bta.insert(tuple);
                }
            }
        }

        //cleanup the buffer
        currentInUse = 0;
        ByteBuffer buffer = buffers.get(0);
        appender.reset(buffer, true);
    }

    /**
     * return the last updated
     * 
     * @throws HyracksDataException
     */
    public ITupleReference getLastTuple() throws HyracksDataException {
        fta.reset(buffers.get(currentInUse));
        int tupleIndex = fta.getTupleCount() - 1;
        if (tupleIndex < 0) {
            return null;
        }
        lastTuple.reset(fta, tupleIndex);
        return lastTuple;
    }

    private void allocate(int index) throws HyracksDataException {
        if (index >= buffers.size()) {
            buffers.add(ctx.allocateFrame());
        }
    }
}
