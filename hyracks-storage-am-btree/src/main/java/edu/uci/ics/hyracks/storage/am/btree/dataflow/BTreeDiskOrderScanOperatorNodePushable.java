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
package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.DiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeDiskOrderScanOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final BTreeOpHelper btreeOpHelper;

    public BTreeDiskOrderScanOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksStageletContext ctx,
            int partition) {
        btreeOpHelper = new BTreeOpHelper(opDesc, ctx, partition, IndexHelperOpenMode.OPEN);
    }

    @Override
    public void initialize() throws HyracksDataException {

        IBTreeLeafFrame cursorFrame = btreeOpHelper.getOperatorDescriptor().getBTreeLeafFactory().getFrame();
        DiskOrderScanCursor cursor = new DiskOrderScanCursor(cursorFrame);
        ITreeIndexMetaDataFrame metaFrame = new LIFOMetaDataFrame();

        try {
        
        	btreeOpHelper.init();
        	
        	try {
        		btreeOpHelper.getBTree().diskOrderScan(cursor, cursorFrame, metaFrame);

        		MultiComparator cmp = btreeOpHelper.getBTree().getMultiComparator();
        		ByteBuffer frame = btreeOpHelper.getHyracksStageletContext().allocateFrame();
        		FrameTupleAppender appender = new FrameTupleAppender(btreeOpHelper.getHyracksStageletContext().getFrameSize());
        		appender.reset(frame, true);
        		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFieldCount());
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
        	}
        	finally {
        		cursor.close();
        		writer.close();
        	}

        } catch(Exception e) {
        	deinitialize();
        	throw new HyracksDataException(e);
        }
    }

    @Override
    public void deinitialize() throws HyracksDataException {
    	btreeOpHelper.deinit();
    }
}