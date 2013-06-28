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

package edu.uci.ics.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ConstantTupleSourceOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private IHyracksTaskContext ctx;

    private int[] fieldSlots;
    private byte[] tupleData;
    private int tupleSize;

    public ConstantTupleSourceOperatorNodePushable(IHyracksTaskContext ctx, int[] fieldSlots, byte[] tupleData,
            int tupleSize) {
        super();
        this.fieldSlots = fieldSlots;
        this.tupleData = tupleData;
        this.tupleSize = tupleSize;
        this.ctx = ctx;
    }

    @Override
    public void initialize() throws HyracksDataException {
        ByteBuffer writeBuffer = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(writeBuffer, true);
        if (fieldSlots != null && tupleData != null && tupleSize > 0)
            appender.append(fieldSlots, tupleData, 0, tupleSize);
        writer.open();
        try {
            FrameUtils.flushFrame(writeBuffer, writer);
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
}