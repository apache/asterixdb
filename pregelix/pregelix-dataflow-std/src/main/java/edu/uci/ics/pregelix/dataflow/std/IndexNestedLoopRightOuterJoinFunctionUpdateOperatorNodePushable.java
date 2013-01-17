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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
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
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.util.FunctionProxy;
import edu.uci.ics.pregelix.dataflow.util.UpdateBuffer;

public class IndexNestedLoopRightOuterJoinFunctionUpdateOperatorNodePushable extends
        AbstractUnaryInputOperatorNodePushable {
    private TreeIndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder nullTupleBuilder;
    private DataOutput dos;

    private BTree btree;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private MultiComparator highKeySearchCmp;
    private ITreeIndexCursor cursor;
    private ITreeIndexFrame cursorFrame;
    protected ITreeIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private final RecordDescriptor inputRecDesc;

    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;

    private INullWriter[] nullWriter;
    private ITupleReference currentTopTuple;
    private boolean match;

    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;
    private ArrayTupleBuilder cloneUpdateTb;
    private UpdateBuffer updateBuffer;

    public IndexNestedLoopRightOuterJoinFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, INullWriter[] nullWriter, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory,
            IRecordDescriptorFactory inputRdFactory, int outputArity) {
        inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        treeIndexOpHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);

        if (lowKeyFields != null && lowKeyFields.length > 0) {
            lowKey = new PermutingFrameTupleReference();
            lowKey.setFieldPermutation(lowKeyFields);
        }
        if (highKeyFields != null && highKeyFields.length > 0) {
            highKey = new PermutingFrameTupleReference();
            highKey.setFieldPermutation(highKeyFields);
        }
        this.nullWriter = nullWriter;

        this.writers = new IFrameWriter[outputArity];
        this.functionProxy = new FunctionProxy(ctx, functionFactory, preHookFactory, postHookFactory, inputRdFactory,
                writers);
        this.updateBuffer = new UpdateBuffer(ctx, 2);
    }

    protected void setCursor() {
        cursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame, true);
    }

    @Override
    public void open() throws HyracksDataException {
        /**
         * function open
         */
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksTaskContext().getFrameSize(), recDesc);

        try {
            treeIndexOpHelper.init(false);
            btree = (BTree) treeIndexOpHelper.getIndex();
            cursorFrame = btree.getLeafFrameFactory().createFrame();
            setCursor();

            // construct range predicate
            // TODO: Can we construct the multicmps using helper methods?
            int lowKeySearchFields = btree.getComparatorFactories().length;
            int highKeySearchFields = btree.getComparatorFactories().length;

            IBinaryComparator[] lowKeySearchComparators = new IBinaryComparator[lowKeySearchFields];
            for (int i = 0; i < lowKeySearchFields; i++) {
                lowKeySearchComparators[i] = btree.getComparatorFactories()[i].createBinaryComparator();
            }
            lowKeySearchCmp = new MultiComparator(lowKeySearchComparators);

            if (lowKeySearchFields == highKeySearchFields) {
                highKeySearchCmp = lowKeySearchCmp;
            } else {
                IBinaryComparator[] highKeySearchComparators = new IBinaryComparator[highKeySearchFields];
                for (int i = 0; i < highKeySearchFields; i++) {
                    highKeySearchComparators[i] = btree.getComparatorFactories()[i].createBinaryComparator();
                }
                highKeySearchCmp = new MultiComparator(highKeySearchComparators);
            }

            rangePred = new RangePredicate(null, null, true, true, lowKeySearchCmp, highKeySearchCmp);

            writeBuffer = treeIndexOpHelper.getHyracksTaskContext().allocateFrame();

            nullTupleBuilder = new ArrayTupleBuilder(inputRecDesc.getFields().length);
            dos = nullTupleBuilder.getDataOutput();
            nullTupleBuilder.reset();
            for (int i = 0; i < inputRecDesc.getFields().length; i++) {
                nullWriter[i].writeNull(dos);
                nullTupleBuilder.addFieldEndOffset();
            }

            appender = new FrameTupleAppender(treeIndexOpHelper.getHyracksTaskContext().getFrameSize());
            appender.reset(writeBuffer, true);

            indexAccessor = btree.createAccessor();

            /** set the search cursor */
            rangePred.setLowKey(null, true);
            rangePred.setHighKey(null, true);
            cursor.reset();
            indexAccessor.search(cursor, rangePred);

            /** set up current top tuple */
            if (cursor.hasNext()) {
                cursor.next();
                currentTopTuple = cursor.getTuple();
                match = false;
            }

            cloneUpdateTb = new ArrayTupleBuilder(btree.getFieldCount());
            updateBuffer.setFieldCount(btree.getFieldCount());
        } catch (Exception e) {
            treeIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    //for the join match casesos
    private void writeResults(IFrameTupleAccessor leftAccessor, int tIndex, ITupleReference frameTuple)
            throws Exception {
        /**
         * function call
         */
        functionProxy.functionCall(leftAccessor, tIndex, frameTuple, cloneUpdateTb);

        //doing clone update
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                //release the cursor/latch
                cursor.close();
                //batch update
                updateBuffer.updateBTree(indexAccessor);

                //search again and recover the cursor
                cursor.reset();
                rangePred.setLowKey(frameTuple, true);
                rangePred.setHighKey(null, true);
                indexAccessor.search(cursor, rangePred);
            }
            cloneUpdateTb.reset();
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount && currentTopTuple != null;) {
                if (lowKey != null)
                    lowKey.reset(accessor, i);
                if (highKey != null)
                    highKey.reset(accessor, i);
                // TODO: currently use low key only, check what they mean
                int cmp = compare(lowKey, currentTopTuple);
                if (cmp <= 0) {
                    if (cmp == 0)
                        outputMatch(i);
                    i++;
                } else {
                    moveTreeCursor();
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void outputMatch(int i) throws Exception {
        writeResults(accessor, i, currentTopTuple);
        match = true;
    }

    private void moveTreeCursor() throws Exception {
        if (!match) {
            writeResults(currentTopTuple);
        }
        if (cursor.hasNext()) {
            cursor.next();
            currentTopTuple = cursor.getTuple();
            match = false;
        } else {
            currentTopTuple = null;
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            while (currentTopTuple != null) {
                moveTreeCursor();
            }
            try {
                cursor.close();
                //batch update
                updateBuffer.updateBTree(indexAccessor);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }

            /**
             * function close
             */
            functionProxy.functionClose();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            treeIndexOpHelper.deinit();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        for (IFrameWriter writer : writers)
            writer.fail();
    }

    /** compare tuples */
    private int compare(ITupleReference left, ITupleReference right) throws Exception {
        return lowKeySearchCmp.compare(left, right);
    }

    /** write result for outer case */
    private void writeResults(ITupleReference frameTuple) throws Exception {
        /**
         * function call
         */
        functionProxy.functionCall(nullTupleBuilder, frameTuple, cloneUpdateTb);

        //doing clone update
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                //release the cursor/latch
                cursor.close();
                //batch update
                updateBuffer.updateBTree(indexAccessor);

                //search again and recover the cursor
                cursor.reset();
                rangePred.setLowKey(frameTuple, true);
                rangePred.setHighKey(null, true);
                indexAccessor.search(cursor, rangePred);
            }
            cloneUpdateTb.reset();
        }
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        writers[index] = writer;
    }
}