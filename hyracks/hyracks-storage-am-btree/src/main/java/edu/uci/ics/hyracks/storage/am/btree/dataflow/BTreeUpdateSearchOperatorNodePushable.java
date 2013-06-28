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

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleUpdater;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;

public class BTreeUpdateSearchOperatorNodePushable extends BTreeSearchOperatorNodePushable {
    private final ITupleUpdater tupleUpdater;

    public BTreeUpdateSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, ITupleUpdater tupleUpdater) {
        super(opDesc, ctx, partition, recordDescProvider, lowKeyFields, highKeyFields, lowKeyInclusive,
                highKeyInclusive);
        this.tupleUpdater = tupleUpdater;
    }

    @Override
    protected ITreeIndexCursor createCursor() {
        ITreeIndex treeIndex = (ITreeIndex) index;
        ITreeIndexFrame cursorFrame = treeIndex.getLeafFrameFactory().createFrame();
        return new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame, true);
    }

    @Override
    protected void writeSearchResults(int tupleIndex) throws Exception {
        while (cursor.hasNext()) {
            tb.reset();
            cursor.next();
            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            ITupleReference tuple = cursor.getTuple();
            tupleUpdater.updateTuple(tuple);
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                FrameUtils.flushFrame(writeBuffer, writer);
                appender.reset(writeBuffer, true);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size ("
                            + appender.getBuffer().capacity() + ")");
                }
            }
        }
    }
}
