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

package edu.uci.ics.hyracks.storage.am.rtree.dataflow;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;

public class RTreeSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private TreeIndexOpHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;

    private RTree rtree;
    private PermutingFrameTupleReference searchKey;
    private SearchPredicate searchPred;
    private MultiComparator cmp;
    private ITreeIndexCursor cursor;
    private ITreeIndexFrame interiorFrame;
    private ITreeIndexFrame leafFrame;
    private RTreeOpContext opCtx;

    private RecordDescriptor recDesc;

    public RTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int[] keyFields) {
        treeIndexOpHelper = opDesc.getTreeIndexOpHelperFactory().createTreeIndexOpHelper(opDesc, ctx, partition,
                IndexHelperOpenMode.OPEN);

        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        if (keyFields != null && keyFields.length > 0) {
            searchKey = new PermutingFrameTupleReference();
            searchKey.setFieldPermutation(keyFields);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor opDesc = (AbstractTreeIndexOperatorDescriptor) treeIndexOpHelper
                .getOperatorDescriptor();
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksTaskContext().getFrameSize(), recDesc);

        interiorFrame = opDesc.getTreeIndexInteriorFactory().createFrame();
        leafFrame = opDesc.getTreeIndexLeafFactory().createFrame();
        cursor = new RTreeSearchCursor((IRTreeInteriorFrame) interiorFrame, (IRTreeLeafFrame) leafFrame);

        try {

            treeIndexOpHelper.init();
            writer.open();
            try {
                rtree = (RTree) treeIndexOpHelper.getTreeIndex();

                int keySearchFields = rtree.getCmp().getComparators().length;

                IBinaryComparator[] keySearchComparators = new IBinaryComparator[keySearchFields];
                for (int i = 0; i < keySearchFields; i++) {
                    keySearchComparators[i] = rtree.getCmp().getComparators()[i];
                }

                cmp = new MultiComparator(keySearchComparators);

                searchPred = new SearchPredicate(searchKey, cmp);

                writeBuffer = treeIndexOpHelper.getHyracksTaskContext().allocateFrame();
                tb = new ArrayTupleBuilder(rtree.getFieldCount());
                dos = tb.getDataOutput();
                appender = new FrameTupleAppender(treeIndexOpHelper.getHyracksTaskContext().getFrameSize());
                appender.reset(writeBuffer, true);

                opCtx = rtree.createOpContext(IndexOp.SEARCH);
            } catch (Exception e) {
                writer.fail();
                throw e;
            }

        } catch (Exception e) {
            treeIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    private void writeSearchResults() throws Exception {
        while (cursor.hasNext()) {
            tb.reset();
            cursor.next();

            ITupleReference frameTuple = cursor.getTuple();
            for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }

            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                FrameUtils.flushFrame(writeBuffer, writer);
                appender.reset(writeBuffer, true);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                searchKey.reset(accessor, i);

                searchPred.setSearchKey(searchKey);
                cursor.reset();
                rtree.search(cursor, searchPred, opCtx);
                writeSearchResults();
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
            try {
                cursor.close();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        } finally {
            treeIndexOpHelper.deinit();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}