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

package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.OccurrenceThresholdPanicException;

public class InvertedIndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final TreeIndexDataflowHelper btreeDataflowHelper;
    private final InvertedIndexDataflowHelper invIndexDataflowHelper;
    private final int queryField;
    private FrameTupleAccessor accessor;
    private FrameTupleReference tuple;
    private IRecordDescriptorProvider recordDescProvider;
    private InvertedIndex invIndex;

    private final InvertedIndexSearchPredicate searchPred;
    private IIndexAccessor indexAccessor;
    private IIndexCursor resultCursor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;

    private final AbstractInvertedIndexOperatorDescriptor opDesc;
    private final boolean retainInput;

    public InvertedIndexSearchOperatorNodePushable(AbstractInvertedIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int queryField, IInvertedIndexSearchModifier searchModifier,
            IRecordDescriptorProvider recordDescProvider) {
        this.opDesc = opDesc;
        btreeDataflowHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory()
                .createIndexDataflowHelper(opDesc, ctx, partition);
        invIndexDataflowHelper = new InvertedIndexDataflowHelper(btreeDataflowHelper, opDesc, ctx, partition);
        this.queryField = queryField;
        this.searchPred = new InvertedIndexSearchPredicate(opDesc.getTokenizerFactory().createTokenizer(), searchModifier);
        this.recordDescProvider = recordDescProvider;
        this.retainInput = invIndexDataflowHelper.getOperatorDescriptor().getRetainInput();
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(btreeDataflowHelper.getHyracksTaskContext().getFrameSize(), inputRecDesc);
        tuple = new FrameTupleReference();
        // BTree.
        try {
            btreeDataflowHelper.init(false);
        } catch (Exception e) {
            // Cleanup in case of failure/
            btreeDataflowHelper.deinit();
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }
        // Inverted Index.
        try {
            invIndexDataflowHelper.init(false);
            invIndex = (InvertedIndex) invIndexDataflowHelper.getIndex();
        } catch (Exception e) {
            // Cleanup in case of failure.
            invIndexDataflowHelper.deinit();
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }

        writeBuffer = btreeDataflowHelper.getHyracksTaskContext().allocateFrame();
        tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
        dos = tb.getDataOutput();
        appender = new FrameTupleAppender(btreeDataflowHelper.getHyracksTaskContext().getFrameSize());
        appender.reset(writeBuffer, true);

        indexAccessor = invIndex.createAccessor();
        //InvertedIndex.InvertedIndexAccessor accessor =  
        resultCursor = indexAccessor.createSearchCursor();
        writer.open();
    }

    private void writeSearchResults() throws Exception {
        while (resultCursor.hasNext()) {
            resultCursor.next();
            tb.reset();
            if (retainInput) {
                for (int i = 0; i < tuple.getFieldCount(); i++) {
                	dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            ITupleReference invListElement = resultCursor.getTuple();
            int invListFields = opDesc.getInvListsTypeTraits().length;
            for (int i = 0; i < invListFields; i++) {
                dos.write(invListElement.getFieldData(i), invListElement.getFieldStart(i),
                        invListElement.getFieldLength(i));
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
                tuple.reset(accessor, i);
                searchPred.setQueryTuple(tuple);
                searchPred.setQueryFieldIndex(queryField);
                try {
                    resultCursor.reset();
                    indexAccessor.search(resultCursor, searchPred);
                    writeSearchResults();
                } catch (OccurrenceThresholdPanicException e) {
                    // Ignore panic cases for now.
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
        } finally {
            try {
                btreeDataflowHelper.deinit();
            } finally {
                invIndexDataflowHelper.deinit();
            }
        }
    }
}
