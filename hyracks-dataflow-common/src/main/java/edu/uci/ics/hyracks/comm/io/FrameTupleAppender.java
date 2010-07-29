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
package edu.uci.ics.hyracks.comm.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;

public class FrameTupleAppender {
    private final IHyracksContext ctx;

    private ByteBuffer buffer;

    private int tupleCount;

    private int tupleDataEndOffset;

    public FrameTupleAppender(IHyracksContext ctx) {
        this.ctx = ctx;
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        this.buffer = buffer;
        if (clear) {
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx), 0);
            tupleCount = 0;
            tupleDataEndOffset = 0;
        } else {
            tupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(ctx));
            tupleDataEndOffset = tupleCount == 0 ? 0 : buffer.getInt(FrameHelper.getTupleCountOffset(ctx) - tupleCount
                    * 4);
        }
    }

    public boolean append(int[] fieldSlots, byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + fieldSlots.length * 2 + length + 4 + (tupleCount + 1) * 4 <= ctx.getFrameSize()) {
            for (int i = 0; i < fieldSlots.length; ++i) {
                buffer.putShort(tupleDataEndOffset + i * 2, (short) fieldSlots[i]);
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + fieldSlots.length * 2, length);
            tupleDataEndOffset += fieldSlots.length * 2 + length;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(FrameTupleAccessor tupleAccessor, int tIndex) {
        int startOffset = tupleAccessor.getTupleStartOffset(tIndex);
        int endOffset = tupleAccessor.getTupleEndOffset(tIndex);
        int length = endOffset - startOffset;
        if (tupleDataEndOffset + length + 4 + (tupleCount + 1) * 4 <= ctx.getFrameSize()) {
            ByteBuffer src = tupleAccessor.getBuffer();
            System.arraycopy(src.array(), startOffset, buffer.array(), tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendConcat(FrameTupleAccessor accessor0, int tIndex0, FrameTupleAccessor accessor1, int tIndex1) {
        int startOffset0 = accessor0.getTupleStartOffset(tIndex0);
        int endOffset0 = accessor0.getTupleEndOffset(tIndex0);
        int length0 = endOffset0 - startOffset0;

        int startOffset1 = accessor1.getTupleStartOffset(tIndex1);
        int endOffset1 = accessor1.getTupleEndOffset(tIndex1);
        int length1 = endOffset1 - startOffset1;

        if (tupleDataEndOffset + length0 + length1 + 4 + (tupleCount + 1) * 4 <= ctx.getFrameSize()) {
            ByteBuffer src0 = accessor0.getBuffer();
            ByteBuffer src1 = accessor1.getBuffer();
            int slotsLen0 = accessor0.getFieldSlotsLength();
            int slotsLen1 = accessor1.getFieldSlotsLength();
            int dataLen0 = length0 - slotsLen0;
            int dataLen1 = length1 - slotsLen1;
            // Copy slots from accessor0 verbatim
            System.arraycopy(src0.array(), startOffset0, buffer.array(), tupleDataEndOffset, slotsLen0);
            // Copy slots from accessor1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < slotsLen1 / 2; ++i) {
                buffer.putShort(tupleDataEndOffset + slotsLen0 + i * 2,
                        (short) (src1.getShort(startOffset1 + i * 2) + dataLen0));
            }
            // Copy data0
            System.arraycopy(src0.array(), startOffset0 + slotsLen0, buffer.array(), tupleDataEndOffset + slotsLen0
                    + slotsLen1, dataLen0);
            // Copy data1
            System.arraycopy(src1.array(), startOffset1 + slotsLen1, buffer.array(), tupleDataEndOffset + slotsLen0
                    + slotsLen1 + dataLen0, dataLen1);
            tupleDataEndOffset += (length0 + length1);
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendProjection(FrameTupleAccessor accessor, int tIndex, int[] fields) {
        int fTargetSlotsLength = fields.length * 2;
        int length = fTargetSlotsLength;
        for (int i = 0; i < fields.length; ++i) {
            length += (accessor.getFieldEndOffset(tIndex, fields[i]) - accessor.getFieldStartOffset(tIndex, fields[i]));
        }

        if (tupleDataEndOffset + length + 4 + (tupleCount + 1) * 4 <= ctx.getFrameSize()) {
            int fSrcSlotsLength = accessor.getFieldSlotsLength();
            int tStartOffset = accessor.getTupleStartOffset(tIndex);

            int fStartOffset = 0;
            int fEndOffset = 0;
            for (int i = 0; i < fields.length; ++i) {
                int fSrcStart = tStartOffset + fSrcSlotsLength + accessor.getFieldStartOffset(tIndex, fields[i]);
                int fLen = accessor.getFieldEndOffset(tIndex, fields[i])
                        - accessor.getFieldStartOffset(tIndex, fields[i]);
                System.arraycopy(accessor.getBuffer().array(), fSrcStart, buffer.array(), tupleDataEndOffset
                        + fTargetSlotsLength + fStartOffset, fLen);
                fEndOffset += fLen;
                buffer.putShort(tupleDataEndOffset + i * 2, (short) fEndOffset);
                fStartOffset = fEndOffset;
            }
            tupleDataEndOffset += length;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(ctx), tupleCount);
            return true;
        }
        return false;
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}