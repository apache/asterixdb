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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

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
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;

public abstract class TreeIndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
	protected TreeIndexDataflowHelper treeIndexHelper;
	protected FrameTupleAccessor accessor;

	protected ByteBuffer writeBuffer;
	protected FrameTupleAppender appender;
	protected ArrayTupleBuilder tb;
	protected DataOutput dos;

	protected ITreeIndex treeIndex;
	protected ISearchPredicate searchPred;
	protected ITreeIndexCursor cursor;
	protected ITreeIndexFrame cursorFrame;
	protected ITreeIndexAccessor indexAccessor;

	protected RecordDescriptor recDesc;

    public TreeIndexSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider) {
        treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition, false);
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
    }
    
    protected abstract ISearchPredicate createSearchPredicate();
    
    protected abstract void resetSearchPredicate(int tupleIndex);
    
    protected ITreeIndexCursor createCursor() {
        return indexAccessor.createSearchCursor();
    }
    
    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(treeIndexHelper.getHyracksTaskContext().getFrameSize(), recDesc);
        writer.open();
        try {
            treeIndexHelper.init();
            treeIndex = (ITreeIndex) treeIndexHelper.getIndex();
            cursorFrame = treeIndex.getLeafFrameFactory().createFrame();
            searchPred = createSearchPredicate();
            writeBuffer = treeIndexHelper.getHyracksTaskContext().allocateFrame();
            tb = new ArrayTupleBuilder(treeIndex.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(treeIndexHelper.getHyracksTaskContext().getFrameSize());
            appender.reset(writeBuffer, true);
            indexAccessor = treeIndex.createAccessor();
            cursor = createCursor();
        } catch (Exception e) {
            treeIndexHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    protected void writeSearchResults() throws Exception {
        while (cursor.hasNext()) {
            tb.reset();
            cursor.next();

            ITupleReference tuple = cursor.getTuple();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
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
                resetSearchPredicate(i);
                cursor.reset();
                indexAccessor.search(cursor, searchPred);
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
            treeIndexHelper.deinit();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}