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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;

public class TreeIndexDiskOrderScanOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final TreeIndexDataflowHelper treeIndexHelper;
    private final ITreeIndexOperatorDescriptor opDesc;
    private ITreeIndex treeIndex;

    public TreeIndexDiskOrderScanOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition) {
        treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition, false);
        this.opDesc = opDesc;
    }

    @Override
    public void initialize() throws HyracksDataException {
        ITreeIndexFrame cursorFrame = opDesc.getTreeIndexLeafFactory().createFrame();
        ITreeIndexCursor cursor = treeIndexHelper.createDiskOrderScanCursor(cursorFrame);
        try {
            treeIndexHelper.init();
            treeIndex = (ITreeIndex) treeIndexHelper.getIndex();
            ITreeIndexAccessor indexAccessor = treeIndex.createAccessor();
            writer.open();
            try {
                indexAccessor.diskOrderScan(cursor);
                int fieldCount = treeIndex.getFieldCount();
                ByteBuffer frame = treeIndexHelper.getHyracksTaskContext().allocateFrame();
                FrameTupleAppender appender = new FrameTupleAppender(treeIndexHelper.getHyracksTaskContext()
                        .getFrameSize());
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
                            throw new IllegalStateException();
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
            deinitialize();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        treeIndexHelper.deinit();
    }
}