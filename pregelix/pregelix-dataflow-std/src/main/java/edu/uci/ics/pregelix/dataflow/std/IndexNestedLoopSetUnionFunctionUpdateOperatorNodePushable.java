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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
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

public class IndexNestedLoopSetUnionFunctionUpdateOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
    private TreeIndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;

    private BTree btree;
    private boolean isForward;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private ITreeIndexCursor cursor;
    private ITreeIndexFrame cursorFrame;
    protected ITreeIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;

    private ITupleReference currentTopTuple;
    private boolean match;

    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;
    private ArrayTupleBuilder cloneUpdateTb;
    private UpdateBuffer updateBuffer;

    public IndexNestedLoopSetUnionFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory,
            IRecordDescriptorFactory inputRdFactory, int outputArity) {
        treeIndexOpHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
        this.isForward = isForward;
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

    protected void setCursor() {
        cursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame, true);
    }

    @Override
    public void open() throws HyracksDataException {
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksTaskContext().getFrameSize(), recDesc);

        try {
            treeIndexOpHelper.init(false);
            btree = (BTree) treeIndexOpHelper.getIndex();
            cursorFrame = btree.getLeafFrameFactory().createFrame();
            setCursor();

            rangePred = new RangePredicate(null, null, true, true, null, null);
            int lowKeySearchFields = btree.getComparatorFactories().length;
            IBinaryComparator[] lowKeySearchComparators = new IBinaryComparator[lowKeySearchFields];
            for (int i = 0; i < lowKeySearchFields; i++) {
                lowKeySearchComparators[i] = btree.getComparatorFactories()[i].createBinaryComparator();
            }
            lowKeySearchCmp = new MultiComparator(lowKeySearchComparators);

            writeBuffer = treeIndexOpHelper.getHyracksTaskContext().allocateFrame();
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

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount;) {
                if (lowKey != null)
                    lowKey.reset(accessor, i);
                if (highKey != null)
                    highKey.reset(accessor, i);
                // TODO: currently use low key only, check what they mean
                if (currentTopTuple != null) {
                    int cmp = compare(lowKey, currentTopTuple);
                    if (cmp == 0) {
                        outputMatch(i);
                        i++;
                    } else if ((cmp > 0 && isForward) || (cmp < 0 && !isForward)) {
                        moveTreeCursor();
                    } else {
                        writeLeftResults(accessor, i, null);
                        i++;
                    }
                } else {
                    writeLeftResults(accessor, i, null);
                    i++;
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void outputMatch(int i) throws Exception {
        writeLeftResults(accessor, i, currentTopTuple);
        match = true;
    }

    private void moveTreeCursor() throws Exception {
        if (!match) {
            writeRightResults(currentTopTuple);
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

    /** write the right result */
    private void writeRightResults(ITupleReference frameTuple) throws Exception {
        functionProxy.functionCall(frameTuple, cloneUpdateTb);

        //doing clone update
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                //release the cursor/latch
                cursor.close();
                //batch update
                updateBuffer.updateBTree(indexAccessor);

                //search again
                cursor.reset();
                rangePred.setLowKey(frameTuple, true);
                rangePred.setHighKey(null, true);
                indexAccessor.search(cursor, rangePred);
            }
            cloneUpdateTb.reset();
        }
    }

    /** write the left result */
    private void writeLeftResults(IFrameTupleAccessor leftAccessor, int tIndex, ITupleReference frameTuple)
            throws Exception {
        functionProxy.functionCall(leftAccessor, tIndex, frameTuple, cloneUpdateTb);

        //doing clone update
        if (cloneUpdateTb.getSize() > 0) {
            if (!updateBuffer.appendTuple(cloneUpdateTb)) {
                //release the cursor/latch
                cursor.close();
                //batch update
                updateBuffer.updateBTree(indexAccessor);

                //search again
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