/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.util.IntSerDeUtils;

public class FrameTupleAppender extends AbstractFrameAppender implements IFrameTupleAppender {

    public FrameTupleAppender() {
    }

    public FrameTupleAppender(IFrame frame) throws HyracksDataException {
        reset(frame, true);
    }

    public FrameTupleAppender(IFrame frame, boolean clear) throws HyracksDataException {
        reset(frame, clear);
    }

    /**
     * append fieldSlots and bytes to the current frame
     */
    @Override
    public boolean append(int[] fieldEndOffsets, byte[] bytes, int offset, int length) throws HyracksDataException {
        if (canHoldNewTuple(fieldEndOffsets.length, length)) {
            for (int i = 0; i < fieldEndOffsets.length; ++i) {
                IntSerDeUtils.putInt(array, tupleDataEndOffset + i * 4, fieldEndOffsets[i]);
            }
            System.arraycopy(bytes, offset, array, tupleDataEndOffset + fieldEndOffsets.length * 4, length);
            tupleDataEndOffset += fieldEndOffsets.length * 4 + length;
            IntSerDeUtils.putInt(getBuffer().array(),
                    FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(getBuffer().array(), FrameHelper.getTupleCountOffset(frame.getFrameSize()),
                    tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(ITupleReference tuple) throws HyracksDataException {
        int length = 0;
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            length += tuple.getFieldLength(i);
        }

        if (canHoldNewTuple(tuple.getFieldCount(), length)) {
            length = 0;
            for (int i = 0; i < tuple.getFieldCount(); ++i) {
                length += tuple.getFieldLength(i);
                IntSerDeUtils.putInt(array, tupleDataEndOffset + i * 4, length);
            }
            length = 0;
            for (int i = 0; i < tuple.getFieldCount(); ++i) {
                System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), array,
                        tupleDataEndOffset + tuple.getFieldCount() * 4 + length, tuple.getFieldLength(i));
                length += tuple.getFieldLength(i);
            }
            tupleDataEndOffset += tuple.getFieldCount() * 4 + length;
            IntSerDeUtils.putInt(getBuffer().array(),
                    FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(getBuffer().array(), FrameHelper.getTupleCountOffset(frame.getFrameSize()),
                    tupleCount);
            return true;
        }
        return false;
    }

    @Override
    public boolean append(byte[] bytes, int offset, int length) throws HyracksDataException {
        if (canHoldNewTuple(0, length)) {
            System.arraycopy(bytes, offset, getBuffer().array(), tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

    @Override
    public boolean appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {
        if (canHoldNewTuple(fieldSlots.length, length)) {
            int effectiveSlots = 0;
            for (int i = 0; i < fieldSlots.length; ++i) {
                if (fieldSlots[i] > 0) {
                    IntSerDeUtils.putInt(array, tupleDataEndOffset + i * 4, fieldSlots[i]);
                    effectiveSlots++;
                }
            }
            System.arraycopy(bytes, offset, array, tupleDataEndOffset + effectiveSlots * 4, length);
            tupleDataEndOffset += effectiveSlots * 4 + length;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

    @Override
    public boolean append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset)
            throws HyracksDataException {
        int length = tEndOffset - tStartOffset;
        if (canHoldNewTuple(0, length)) {
            ByteBuffer src = tupleAccessor.getBuffer();
            System.arraycopy(src.array(), tStartOffset, array, tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

    @Override
    public boolean append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        int tStartOffset = tupleAccessor.getTupleStartOffset(tIndex);
        int tEndOffset = tupleAccessor.getTupleEndOffset(tIndex);
        return append(tupleAccessor, tStartOffset, tEndOffset);
    }

    @Override
    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int startOffset0 = accessor0.getTupleStartOffset(tIndex0);
        int endOffset0 = accessor0.getTupleEndOffset(tIndex0);
        int length0 = endOffset0 - startOffset0;

        int startOffset1 = accessor1.getTupleStartOffset(tIndex1);
        int endOffset1 = accessor1.getTupleEndOffset(tIndex1);
        int length1 = endOffset1 - startOffset1;

        if (canHoldNewTuple(0, length0 + length1)) {
            ByteBuffer src0 = accessor0.getBuffer();
            ByteBuffer src1 = accessor1.getBuffer();
            int slotsLen0 = accessor0.getFieldSlotsLength();
            int slotsLen1 = accessor1.getFieldSlotsLength();
            int dataLen0 = length0 - slotsLen0;
            int dataLen1 = length1 - slotsLen1;
            // Copy slots from accessor0 verbatim
            System.arraycopy(src0.array(), startOffset0, array, tupleDataEndOffset, slotsLen0);
            // Copy slots from accessor1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < slotsLen1 / 4; ++i) {
                IntSerDeUtils.putInt(array, tupleDataEndOffset + slotsLen0 + i * 4,
                        src1.getInt(startOffset1 + i * 4) + dataLen0);
            }
            // Copy data0
            System.arraycopy(src0.array(), startOffset0 + slotsLen0, array, tupleDataEndOffset + slotsLen0 + slotsLen1,
                    dataLen0);
            // Copy data1
            System.arraycopy(src1.array(), startOffset1 + slotsLen1, array,
                    tupleDataEndOffset + slotsLen0 + slotsLen1 + dataLen0, dataLen1);
            tupleDataEndOffset += (length0 + length1);
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

    @Override
    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1,
            int offset1, int dataLen1) throws HyracksDataException {
        int startOffset0 = accessor0.getTupleStartOffset(tIndex0);
        int endOffset0 = accessor0.getTupleEndOffset(tIndex0);
        int length0 = endOffset0 - startOffset0;

        int slotsLen1 = fieldSlots1.length * 4;
        int length1 = slotsLen1 + dataLen1;

        if (canHoldNewTuple(0, length0 + length1)) {
            ByteBuffer src0 = accessor0.getBuffer();
            int slotsLen0 = accessor0.getFieldSlotsLength();
            int dataLen0 = length0 - slotsLen0;
            // Copy slots from accessor0 verbatim
            System.arraycopy(src0.array(), startOffset0, array, tupleDataEndOffset, slotsLen0);
            // Copy fieldSlots1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < fieldSlots1.length; ++i) {
                IntSerDeUtils.putInt(array, tupleDataEndOffset + slotsLen0 + i * 4, (fieldSlots1[i] + dataLen0));
            }
            // Copy data0
            System.arraycopy(src0.array(), startOffset0 + slotsLen0, array, tupleDataEndOffset + slotsLen0 + slotsLen1,
                    dataLen0);
            // Copy bytes1
            System.arraycopy(bytes1, offset1, array, tupleDataEndOffset + slotsLen0 + fieldSlots1.length * 4 + dataLen0,
                    dataLen1);
            tupleDataEndOffset += (length0 + length1);
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendConcat(int[] fieldSlots0, byte[] bytes0, int offset0, int dataLen0,
            IFrameTupleAccessor accessor1, int tIndex1) throws HyracksDataException {
        int slotsLen0 = fieldSlots0.length * 4;
        int length0 = slotsLen0 + dataLen0;

        int startOffset1 = accessor1.getTupleStartOffset(tIndex1);
        int endOffset1 = accessor1.getTupleEndOffset(tIndex1);
        int length1 = endOffset1 - startOffset1;

        if (canHoldNewTuple(0, length0 + length1)) {
            ByteBuffer src1 = accessor1.getBuffer();
            int slotsLen1 = accessor1.getFieldSlotsLength();
            int dataLen1 = length1 - slotsLen1;
            // Copy fieldSlots0 verbatim
            for (int i = 0; i < fieldSlots0.length; ++i) {
                IntSerDeUtils.putInt(array, tupleDataEndOffset + i * 4, fieldSlots0[i]);
            }
            // Copy slots from accessor1 with the following transformation: newSlotIdx = oldSlotIdx + dataLen0
            for (int i = 0; i < slotsLen1 / 4; ++i) {
                IntSerDeUtils.putInt(array, tupleDataEndOffset + slotsLen0 + i * 4,
                        src1.getInt(startOffset1 + i * 4) + dataLen0);
            }
            // Copy bytes0
            System.arraycopy(bytes0, offset0, array, tupleDataEndOffset + slotsLen0 + slotsLen1, dataLen0);
            // Copy data1
            System.arraycopy(src1.array(), startOffset1 + slotsLen1, array,
                    tupleDataEndOffset + slotsLen0 + slotsLen1 + dataLen0, dataLen1);
            tupleDataEndOffset += (length0 + length1);
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

    @Override
    public boolean appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields)
            throws HyracksDataException {
        int fTargetSlotsLength = fields.length * 4;
        int length = fTargetSlotsLength;
        for (int i = 0; i < fields.length; ++i) {
            length += (accessor.getFieldEndOffset(tIndex, fields[i]) - accessor.getFieldStartOffset(tIndex, fields[i]));
        }

        if (canHoldNewTuple(0, length)) {
            int fSrcSlotsLength = accessor.getFieldSlotsLength();
            int tStartOffset = accessor.getTupleStartOffset(tIndex);

            int fStartOffset = 0;
            int fEndOffset = 0;
            for (int i = 0; i < fields.length; ++i) {
                int fSrcStart = tStartOffset + fSrcSlotsLength + accessor.getFieldStartOffset(tIndex, fields[i]);
                int fLen =
                        accessor.getFieldEndOffset(tIndex, fields[i]) - accessor.getFieldStartOffset(tIndex, fields[i]);
                System.arraycopy(accessor.getBuffer().array(), fSrcStart, array,
                        tupleDataEndOffset + fTargetSlotsLength + fStartOffset, fLen);
                fEndOffset += fLen;
                IntSerDeUtils.putInt(array, tupleDataEndOffset + i * 4, fEndOffset);
                fStartOffset = fEndOffset;
            }
            tupleDataEndOffset += length;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                    tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);
            return true;
        }
        return false;
    }

}
