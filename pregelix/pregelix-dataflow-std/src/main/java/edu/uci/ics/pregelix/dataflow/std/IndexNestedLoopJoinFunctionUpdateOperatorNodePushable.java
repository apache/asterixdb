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

public class IndexNestedLoopJoinFunctionUpdateOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
    private TreeIndexDataflowHelper treeIndexOpHelper;
    private FrameTupleAccessor accessor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private BTree btree;
    private PermutingFrameTupleReference lowKey;
    private PermutingFrameTupleReference highKey;
    private boolean lowKeyInclusive;
    private boolean highKeyInclusive;
    private RangePredicate rangePred;
    private MultiComparator lowKeySearchCmp;
    private MultiComparator highKeySearchCmp;
    private ITreeIndexCursor cursor;
    private ITreeIndexFrame cursorFrame;
    protected ITreeIndexAccessor indexAccessor;

    private RecordDescriptor recDesc;
    private final IFrameWriter[] writers;
    private final FunctionProxy functionProxy;

    public IndexNestedLoopJoinFunctionUpdateOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            IUpdateFunctionFactory functionFactory, IRuntimeHookFactory preHookFactory,
            IRuntimeHookFactory postHookFactory, IRecordDescriptorFactory inputRdFactory, int outputArity) {
        treeIndexOpHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition);
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
    }

    protected void setCursor() {
        cursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame, true);
    }

    @Override
    public void open() throws HyracksDataException {
        /**
         * open the function
         */
        functionProxy.functionOpen();
        accessor = new FrameTupleAccessor(treeIndexOpHelper.getHyracksTaskContext().getFrameSize(), recDesc);

        try {
            treeIndexOpHelper.init(false);
            btree = (BTree) treeIndexOpHelper.getIndex();
            btree.open(treeIndexOpHelper.getIndexFileId());
            cursorFrame = btree.getLeafFrameFactory().createFrame();
            setCursor();

            // TODO: Can we construct the multicmps using helper methods?
            int lowKeySearchFields = btree.getComparatorFactories().length;
            int highKeySearchFields = btree.getComparatorFactories().length;
            if (lowKey != null)
                lowKeySearchFields = lowKey.getFieldCount();
            if (highKey != null)
                highKeySearchFields = highKey.getFieldCount();

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

            rangePred = new RangePredicate(null, null, lowKeyInclusive, highKeyInclusive, lowKeySearchCmp,
                    highKeySearchCmp);
            writeBuffer = treeIndexOpHelper.getHyracksTaskContext().allocateFrame();
            appender = new FrameTupleAppender(treeIndexOpHelper.getHyracksTaskContext().getFrameSize());
            appender.reset(writeBuffer, true);

            indexAccessor = btree.createAccessor();
        } catch (Exception e) {
            treeIndexOpHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    private void writeSearchResults(IFrameTupleAccessor leftAccessor, int tIndex) throws Exception {
        while (cursor.hasNext()) {
            cursor.next();
            ITupleReference tupleRef = cursor.getTuple();

            /**
             * call the update function
             */
            functionProxy.functionCall(leftAccessor, tIndex, tupleRef);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (lowKey != null)
                    lowKey.reset(accessor, i);
                if (highKey != null)
                    highKey.reset(accessor, i);
                rangePred.setLowKey(lowKey, lowKeyInclusive);
                rangePred.setHighKey(highKey, highKeyInclusive);

                cursor.reset();
                indexAccessor.search(cursor, rangePred);
                writeSearchResults(accessor, i);
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
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }

            /**
             * close the update function
             */
            functionProxy.functionClose();
        } finally {
            treeIndexOpHelper.deinit();
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