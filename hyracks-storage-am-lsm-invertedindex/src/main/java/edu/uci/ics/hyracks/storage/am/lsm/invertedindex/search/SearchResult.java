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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
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
    protected final FixedSizeFrameTupleAppender resultFrameTupleApp;
    protected final FixedSizeFrameTupleAccessor resultFrameTupleAcc;
    protected final FixedSizeTupleReference resultTuple;
    protected final ITypeTraits[] typeTraits;
    protected final int invListElementSize;
    
    protected int currBufIdx;
    protected int numResults;

    public SearchResult(ITypeTraits[] invListFields, IHyracksCommonContext ctx) {
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
        resultFrameTupleApp = new FixedSizeFrameTupleAppender(ctx.getFrameSize(), typeTraits);
        resultFrameTupleAcc = new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), typeTraits);
        resultTuple = new FixedSizeTupleReference(typeTraits);
        buffers.add(ctx.allocateFrame());
    }

    /**
     * Initialize from other search-result object to share member instances except for result buffers.
     */
    public SearchResult(SearchResult other) {
        this.ctx = other.ctx;
        this.resultFrameTupleApp = other.resultFrameTupleApp;
        this.resultFrameTupleAcc = other.resultFrameTupleAcc;
        this.resultTuple = other.resultTuple;
        this.typeTraits = other.typeTraits;
        this.invListElementSize = other.invListElementSize;
        buffers.add(ctx.allocateFrame());
    }

    public FixedSizeFrameTupleAccessor getAccessor() {
        return resultFrameTupleAcc;
    }
    
    public FixedSizeFrameTupleAppender getAppender() {
        return resultFrameTupleApp;
    }
    
    public FixedSizeTupleReference getTuple() {
        return resultTuple;
    }
    
    public ArrayList<ByteBuffer> getBuffers() {
        return buffers;
    }
    
    public void reset() {
        currBufIdx = 0;
        numResults = 0;
        resultFrameTupleApp.reset(buffers.get(0), true);
    }
    
    public void clear() {
        currBufIdx = 0;
        numResults = 0;
        for (ByteBuffer buffer : buffers) {
            resultFrameTupleApp.reset(buffer, true);
        }
    }

    public void append(ITupleReference invListElement, int count) {
        ByteBuffer currentBuffer = buffers.get(currBufIdx);
        if (!resultFrameTupleApp.hasSpace()) {
            currBufIdx++;
            if (currBufIdx >= buffers.size()) {
                buffers.add(ctx.allocateFrame());
            }
            currentBuffer = buffers.get(currBufIdx);
            resultFrameTupleApp.reset(currentBuffer, true);
        }
        // Append inverted-list element.
        if (!resultFrameTupleApp.append(invListElement.getFieldData(0), invListElement.getFieldStart(0),
                invListElementSize)) {
            throw new IllegalStateException();
        }
        // Append count.
        if (!resultFrameTupleApp.append(count)) {
            throw new IllegalStateException();
        }
        resultFrameTupleApp.incrementTupleCount(1);
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
}
