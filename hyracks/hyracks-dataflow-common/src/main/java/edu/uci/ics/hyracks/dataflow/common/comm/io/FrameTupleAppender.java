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
package edu.uci.ics.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;

public class FrameTupleAppender {
    private final int frameSize;

    private ByteBuffer buffer;

    private int tupleCount;

    private int tupleDataEndOffset;

    public FrameTupleAppender(int frameSize) {
        this.frameSize = frameSize;
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        this.buffer = buffer;
        if (clear) {
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            tupleCount = 0;
            tupleDataEndOffset = 0;
        } else {
            tupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(frameSize));
            tupleDataEndOffset = tupleCount == 0 ? 0 : buffer.getInt(FrameHelper.getTupleCountOffset(frameSize)
                    - tupleCount * 4);
        }
    }

    public boolean append(int[] fieldSlots, byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + fieldSlots.length * 4 + length + 4 + (tupleCount + 1) * 4 <= frameSize) {
            for (int i = 0; i < fieldSlots.length; ++i) {
                buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots[i]);
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + fieldSlots.length * 4, length);
            tupleDataEndOffset += fieldSlots.length * 4 + length;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + length + 4 + (tupleCount + 1) * 4 <= frameSize) {
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + fieldSlots.length * 4 + length + 4 + (tupleCount + 1) * 4 <= frameSize) {
            int effectiveSlots = 0;
            for (int i = 0; i < fieldSlots.length; ++i) {
                if (fieldSlots[i] > 0) {
                    buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots[i]);
                    effectiveSlots++;
                }
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + effectiveSlots * 4, length);
            tupleDataEndOffset += effectiveSlots * 4 + length;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset) {
        int length = tEndOffset - tStartOffset;
        if (tupleDataEndOffset + length + 4 + (tupleCount + 1) * 4 <= frameSize) {
            ByteBuffer src = tupleAccessor.getBuffer();
            System.arraycopy(src.array(), tStartOffset, buffer.array(), tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(IFrameTupleAccessor tupleAccessor, int tIndex) {
        int tStartOffset = tupleAccessor.getTupleStartOffset(tIndex);
        int tEndOffset = tupleAccessor.getTupleEndOffset(tIndex);
        return append(tupleAccessor, tStartOffset, tEndOffset);
    }

    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1) {
        int startOffset0 = accessor0.getTupleStartOffset(tIndex0);
        int endOffset0 = accessor0.getTupleEndOffset(tIndex0);
        int length0 = endOffset0 - startOffset0;

        int startOffset1 = accessor1.getTupleStartOffset(tIndex1);
        int endOffset1 = accessor1.getTupleEndOffset(tIndex1);
        int length1 = endOffset1 - startOffset1;

        if (tupleDataEndOffset + length0 + length1 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            ByteBuffer src0 = accessor0.getBuffer();
            ByteBuffer src1 = accessor1.getBuffer();
            int slotsLen0 = accessor0.getFieldSlotsLength();
            int slotsLen1 = accessor1.getFieldSlotsLength();
            int dataLen0 = length0 - slotsLen0;
            int dataLen1 = length1 - slotsLen1;
            // Copy slots from accessor0 verbatim
            System.arraycopy(src0.array(), startOffset0, buffer.array(), tupleDataEndOffset, slotsLen0);
            // Copy slots from accessor1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < slotsLen1 / 4; ++i) {
                buffer.putInt(tupleDataEndOffset + slotsLen0 + i * 4, src1.getInt(startOffset1 + i * 4) + dataLen0);
            }
            // Copy data0
            System.arraycopy(src0.array(), startOffset0 + slotsLen0, buffer.array(), tupleDataEndOffset + slotsLen0
                    + slotsLen1, dataLen0);
            // Copy data1
            System.arraycopy(src1.array(), startOffset1 + slotsLen1, buffer.array(), tupleDataEndOffset + slotsLen0
                    + slotsLen1 + dataLen0, dataLen1);
            tupleDataEndOffset += (length0 + length1);
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1,
            int offset1, int dataLen1) {
        int startOffset0 = accessor0.getTupleStartOffset(tIndex0);
        int endOffset0 = accessor0.getTupleEndOffset(tIndex0);
        int length0 = endOffset0 - startOffset0;

        int slotsLen1 = fieldSlots1.length * 4;
        int length1 = slotsLen1 + dataLen1;

        if (tupleDataEndOffset + length0 + length1 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            ByteBuffer src0 = accessor0.getBuffer();
            int slotsLen0 = accessor0.getFieldSlotsLength();
            int dataLen0 = length0 - slotsLen0;
            // Copy slots from accessor0 verbatim
            System.arraycopy(src0.array(), startOffset0, buffer.array(), tupleDataEndOffset, slotsLen0);
            // Copy fieldSlots1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < fieldSlots1.length; ++i) {
                buffer.putInt(tupleDataEndOffset + slotsLen0 + i * 4, (fieldSlots1[i] + dataLen0));
            }
            // Copy data0
            System.arraycopy(src0.array(), startOffset0 + slotsLen0, buffer.array(), tupleDataEndOffset + slotsLen0
                    + slotsLen1, dataLen0);
            // Copy bytes1
            System.arraycopy(bytes1, offset1, buffer.array(), tupleDataEndOffset + slotsLen0 + fieldSlots1.length * 4
                    + dataLen0, dataLen1);
            tupleDataEndOffset += (length0 + length1);
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendConcat(int[] fieldSlots0, byte[] bytes0, int offset0, int dataLen0,
            IFrameTupleAccessor accessor1, int tIndex1) {
        int slotsLen0 = fieldSlots0.length * 4;
        int length0 = slotsLen0 + dataLen0;

        int startOffset1 = accessor1.getTupleStartOffset(tIndex1);
        int endOffset1 = accessor1.getTupleEndOffset(tIndex1);
        int length1 = endOffset1 - startOffset1;

        if (tupleDataEndOffset + length0 + length1 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            ByteBuffer src1 = accessor1.getBuffer();
            int slotsLen1 = accessor1.getFieldSlotsLength();
            int dataLen1 = length1 - slotsLen1;
            // Copy fieldSlots0 verbatim
            for (int i = 0; i < fieldSlots0.length; ++i) {
                buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots0[i]);
            }
            // Copy slots from accessor1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < slotsLen1 / 4; ++i) {
                buffer.putInt(tupleDataEndOffset + slotsLen0 + i * 4, src1.getInt(startOffset1 + i * 4) + dataLen0);
            }
            // Copy bytes0
            System.arraycopy(bytes0, offset0, buffer.array(), tupleDataEndOffset + slotsLen0 + slotsLen1, dataLen0);
            // Copy data1
            System.arraycopy(src1.array(), startOffset1 + slotsLen1, buffer.array(), tupleDataEndOffset + slotsLen0
                    + slotsLen1 + dataLen0, dataLen1);
            tupleDataEndOffset += (length0 + length1);
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields) {
        int fTargetSlotsLength = fields.length * 4;
        int length = fTargetSlotsLength;
        for (int i = 0; i < fields.length; ++i) {
            length += (accessor.getFieldEndOffset(tIndex, fields[i]) - accessor.getFieldStartOffset(tIndex, fields[i]));
        }

        if (tupleDataEndOffset + length + 4 + (tupleCount + 1) * 4 <= frameSize) {
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
                buffer.putInt(tupleDataEndOffset + i * 4, fEndOffset);
                fStartOffset = fEndOffset;
            }
            tupleDataEndOffset += length;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
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
