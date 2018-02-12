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

package org.apache.hyracks.storage.am.btree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITupleUpdater;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.IIndexCursor;

public class BTreeUpdateSearchOperatorNodePushable extends BTreeSearchOperatorNodePushable {
    private final ITupleUpdater tupleUpdater;

    public BTreeUpdateSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleUpdater tupleUpdater, boolean appendIndexFilter) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive, null, null,
                indexHelperFactory, retainInput, retainMissing, missingWriterFactory, searchCallbackFactory,
                appendIndexFilter);
        this.tupleUpdater = tupleUpdater;
    }

    @Override
    protected IIndexCursor createCursor() {
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
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }
    }
}
