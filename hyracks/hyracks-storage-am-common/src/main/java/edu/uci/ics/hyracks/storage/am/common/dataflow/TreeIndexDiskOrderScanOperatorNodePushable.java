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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;

public class TreeIndexDiskOrderScanOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final AbstractTreeIndexOperatorDescriptor opDesc;
    private final IHyracksTaskContext ctx;
    private final TreeIndexDataflowHelper treeIndexHelper;
    private ITreeIndex treeIndex;

    public TreeIndexDiskOrderScanOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory()
                .createIndexDataflowHelper(opDesc, ctx, partition);
    }

    @Override
    public void initialize() throws HyracksDataException {
        treeIndexHelper.open();
        treeIndex = (ITreeIndex) treeIndexHelper.getIndexInstance();
        try {
            ITreeIndexFrame cursorFrame = treeIndex.getLeafFrameFactory().createFrame();
            ITreeIndexCursor cursor = treeIndexHelper.createDiskOrderScanCursor(cursorFrame);
            ISearchOperationCallback searchCallback = opDesc.getSearchOpCallbackFactory().createSearchOperationCallback(
                    treeIndexHelper.getResourceID(), ctx);
            ITreeIndexAccessor indexAccessor = (ITreeIndexAccessor) treeIndex.createAccessor(
                    NoOpOperationCallback.INSTANCE, searchCallback);
            writer.open();
            try {
                indexAccessor.diskOrderScan(cursor);
                int fieldCount = treeIndex.getFieldCount();
                ByteBuffer frame = ctx.allocateFrame();
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                appender.reset(frame, true);
                ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
                DataOutput dos = tb.getDataOutput();

                while (cursor.hasNext()) {
                    tb.reset();
                    cursor.next();

                    ITupleReference frameTuple = cursor.getTuple();
                    for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                        dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                        tb.addFieldEndOffset();
                    }

                    if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        FrameUtils.flushFrame(frame, writer);
                        appender.reset(frame, true);
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                        }
                    }
                }
                if (appender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame, writer);
                }
            } catch (Exception e) {
                writer.fail();
                throw new HyracksDataException(e);
            } finally {
                cursor.close();
                writer.close();
            }
        } catch (Exception e) {
            treeIndexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        treeIndexHelper.close();
    }
}
