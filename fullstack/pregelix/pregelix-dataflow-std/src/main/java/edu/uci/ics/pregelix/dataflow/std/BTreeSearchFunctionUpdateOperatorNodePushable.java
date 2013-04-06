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
package edu.uci.ics.pregelix.dataflow.std;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.util.FunctionProxy;
import edu.uci.ics.pregelix.dataflow.util.UpdateBuffer;

public class BTreeSearchFunctionUpdateOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
    protected TreeIndexDataflowHelper treeIndexHelper;
    protected FrameTupleAccessor accessor;

    protected ByteBuffer writeBuffer;
    protected FrameTupleAppender appender;
    protected ArrayTupleBuilder tb;
    protected DataOutput dos;

    protected BTree btree;
    protected boolean isForward;
    protected PermutingFrameTupleReference lowKey;
    protected PermutingFrameTupleReference highKey;
    protected boolean lowKeyInclusive;
    protected boolean highKeyInclusive;
    protected RangePredicate rangePred;
    protected MultiComparator lowKeySearchCmp;
    protected MultiComparator highKeySearchCmp;
    protected ITreeIndexCursor cursor;
    protected ITreeIndexFrame cursorFrame;
    protected ITreeIndexAccessor indexAccessor;

    protected RecordDescriptor recDesc;

    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;
    private ArrayTupleBuilder cloneUpdateTb;
    private final UpdateBuffer updateBuffer;

    public BTreeSearchFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IUpdateFunctionFactory functionFactory, IRuntimeHookFactory preHookFactory,
            IRuntimeHookFactory postHookFactory, IRecordDescriptorFactory inputRdFactory, int outputArity) {
        treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.isForward = isForward;
        this.lowKeyInclusive = lowKeyInclusive;
        this.highKeyInclusive = highKeyInclusive;
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }

        this.writers = new IFrameWriter[outputArity];
        this.functionProxy = new FunctionProxy(ctx, functionFactory, preHookFactory, postHookFactory, inputRdFactory,
                writers);
        this.updateBuffer = new UpdateBuffer(ctx, 2);
    }

    @Override
    public void open() throws HyracksDataException {
        /**
         * open the function
         */
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(treeIndexHelper.getTaskContext().getFrameSize(), recDesc);

        try {
            treeIndexHelper.open();
            btree = (BTree) treeIndexHelper.getIndexInstance();
            cursorFrame = btree.getLeafFrameFactory().createFrame();
            setCursor();

            // Construct range predicate.
            lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(btree.getComparatorFactories(), lowKey);
            highKeySearchCmp = BTreeUtils.getSearchMultiComparator(btree.getComparatorFactories(), highKey);
            rangePred = new RangePredicate(null, null, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp,
                    highKeySearchCmp);

            writeBuffer = treeIndexHelper.getTaskContext().allocateFrame();
            tb = new ArrayTupleBuilder(btree.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(treeIndexHelper.getTaskContext().getFrameSize());
            appender.reset(writeBuffer, true);
            indexAccessor = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

            cloneUpdateTb = new ArrayTupleBuilder(btree.getFieldCount());
            updateBuffer.setFieldCount(btree.getFieldCount());
        } catch (Exception e) {
            treeIndexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    protected void setCursor() {
        cursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame, false);
    }

    protected void writeSearchResults() throws Exception {
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference tuple = cursor.getTuple();
            functionProxy.functionCall(tuple, cloneUpdateTb);

            //doing clone update
            if (cloneUpdateTb.getSize() > 0) {
                if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                    //release the cursor/latch
                    cursor.close();
                    //batch update
                    updateBuffer.updateBTree(indexAccessor);

                    //search again
                    cursor.reset();
                    rangePred.setLowKey(tuple, true);
                    rangePred.setHighKey(highKey, highKeyInclusive);
                    indexAccessor.search(cursor, rangePred);
                }
            }
            cloneUpdateTb.reset();
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (lowKey != null) {
                    lowKey.reset(accessor, i);
                }
                if (highKey != null) {
                    highKey.reset(accessor, i);
                }
                rangePred.setLowKey(lowKey, lowKeyInclusive);
                rangePred.setHighKey(highKey, highKeyInclusive);
                cursor.reset();
                indexAccessor.search(cursor, rangePred);
                writeSearchResults();
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            try {
                cursor.close();
                //batch update
                updateBuffer.updateBTree(indexAccessor);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }

            /**
             * close the update function
             */
            functionProxy.functionClose();
        } finally {
            treeIndexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        for (IFrameWriter writer : writers)
            writer.fail();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        writers[index] = writer;
    }

}