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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAppender;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;

/**
 * Byte-buffer backed storage for intermediate and final results of inverted-index searches.
 */
// TODO: Rename members.
public class SearchResult {
    protected final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    protected final IHyracksCommonContext ctx;
    protected final FixedSizeFrameTupleAppender appender;
    protected final FixedSizeFrameTupleAccessor accessor;
    protected final FixedSizeTupleReference tuple;
    protected final ITypeTraits[] typeTraits;
    protected final int invListElementSize;

    protected int currBufIdx;
    protected int numResults;

    public SearchResult(ITypeTraits[] invListFields, IHyracksCommonContext ctx) throws HyracksDataException {
        typeTraits = new ITypeTraits[invListFields.length + 1];
        int tmp = 0;
        for (int i = 0; i < invListFields.length; i++) {
            typeTraits[i] = invListFields[i];
            tmp += invListFields[i].getFixedLength();
        }
        invListElementSize = tmp;
        // Integer for counting occurrences.
        typeTraits[invListFields.length] = IntegerPointable.TYPE_TRAITS;
        this.ctx = ctx;
        appender = new FixedSizeFrameTupleAppender(ctx.getFrameSize(), typeTraits);
        accessor = new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), typeTraits);
        tuple = new FixedSizeTupleReference(typeTraits);
        buffers.add(ctx.allocateFrame());
    }

    /**
     * Initialize from other search-result object to share member instances except for result buffers.
     * 
     * @throws HyracksDataException
     */
    public SearchResult(SearchResult other) throws HyracksDataException {
        this.ctx = other.ctx;
        this.appender = other.appender;
        this.accessor = other.accessor;
        this.tuple = other.tuple;
        this.typeTraits = other.typeTraits;
        this.invListElementSize = other.invListElementSize;
        buffers.add(ctx.allocateFrame());
    }

    public FixedSizeFrameTupleAccessor getAccessor() {
        return accessor;
    }

    public FixedSizeFrameTupleAppender getAppender() {
        return appender;
    }

    public FixedSizeTupleReference getTuple() {
        return tuple;
    }

    public ArrayList<ByteBuffer> getBuffers() {
        return buffers;
    }

    public void reset() {
        currBufIdx = 0;
        numResults = 0;
        appender.reset(buffers.get(0), true);
    }

    public void clear() {
        currBufIdx = 0;
        numResults = 0;
        for (ByteBuffer buffer : buffers) {
            appender.reset(buffer, true);
        }
    }

    public void append(ITupleReference invListElement, int count) throws HyracksDataException {
        ByteBuffer currentBuffer = buffers.get(currBufIdx);
        if (!appender.hasSpace()) {
            currBufIdx++;
            if (currBufIdx >= buffers.size()) {
                buffers.add(ctx.allocateFrame());
            }
            currentBuffer = buffers.get(currBufIdx);
            appender.reset(currentBuffer, true);
        }
        // Append inverted-list element.
        if (!appender.append(invListElement.getFieldData(0), invListElement.getFieldStart(0), invListElementSize)) {
            throw new IllegalStateException();
        }
        // Append count.
        if (!appender.append(count)) {
            throw new IllegalStateException();
        }
        appender.incrementTupleCount(1);
        numResults++;
    }

    public int getCurrentBufferIndex() {
        return currBufIdx;
    }

    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
    }

    public int getNumResults() {
        return numResults;
    }

    // TODO: This code may help to clean up the core list-merging algorithms.
    /*
    public SearchResultCursor getCursor() {
        cursor.reset();
        return cursor;
    }
    
    public class SearchResultCursor {
        private int bufferIndex;
        private int resultIndex;
        private int frameResultIndex;
        private ByteBuffer currentBuffer;

        public void reset() {
            bufferIndex = 0;
            resultIndex = 0;
            frameResultIndex = 0;
            currentBuffer = buffers.get(0);
            resultFrameTupleAcc.reset(currentBuffer);
        }

        public boolean hasNext() {
            return resultIndex < numResults;
        }

        public void next() {
            resultTuple.reset(currentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(frameResultIndex));            
            if (frameResultIndex < resultFrameTupleAcc.getTupleCount()) {
                frameResultIndex++;
            } else {
                bufferIndex++;
                currentBuffer = buffers.get(bufferIndex);
                resultFrameTupleAcc.reset(currentBuffer);
                frameResultIndex = 0;
            }            
            resultIndex++;
        }

        public ITupleReference getTuple() {
            return resultTuple;
        }
    }
    */
}
