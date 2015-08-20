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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

/**
 *
 */
public class FrameToolsForGroupers {

    public static void writeFields(byte[] buf, int offset, int length, ArrayTupleBuilder tupleBuilder)
            throws HyracksDataException {
        writeFields(buf, offset, length, tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize());
    }

    public static void writeFields(byte[] buf, int offset, int length, int[] fieldsOffset, byte[] data, int dataOffset,
            int dataLength) throws HyracksDataException {
        if (dataLength + 4 * fieldsOffset.length > length) {
            throw new HyracksDataException("Out of buffer bound: try to write too much data (" + dataLength
                    + ") to the given bound (" + length + ").");
        }

        ByteBuffer buffer = ByteBuffer.wrap(buf, offset, length);
        for (int i = 0; i < fieldsOffset.length; i++) {
            buffer.putInt(fieldsOffset[i]);
        }
        buffer.put(data, dataOffset, dataLength);
    }

    public static void updateFrameMetaForNewTuple(ByteBuffer buffer, int addedTupleLength) throws HyracksDataException {
        int currentTupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()));
        int currentTupleEndOffset = buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()) - 4
                * currentTupleCount);
        int newTupleEndOffset = currentTupleEndOffset + addedTupleLength;

        // update tuple end offset
        buffer.putInt(FrameHelper.getTupleCountOffset(buffer.capacity()) - 4 * (currentTupleCount + 1),
                newTupleEndOffset);
        // Update the tuple count
        buffer.putInt(FrameHelper.getTupleCountOffset(buffer.capacity()), currentTupleCount + 1);
    }

    public static void updateFrameMetaForNewTuple(ByteBuffer buffer, int addedTupleLength, boolean isReset)
            throws HyracksDataException {
        int currentTupleCount;
        int currentTupleEndOffset;
        if (isReset) {
            currentTupleCount = 0;
            currentTupleEndOffset = 0;
        } else {
            currentTupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()));
            currentTupleEndOffset = buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()) - 4
                    * currentTupleCount);
        }
        int newTupleEndOffset = currentTupleEndOffset + addedTupleLength;

        // update tuple end offset
        buffer.putInt(FrameHelper.getTupleCountOffset(buffer.capacity()) - 4 * (currentTupleCount + 1),
                newTupleEndOffset);
        // Update the tuple count
        buffer.putInt(FrameHelper.getTupleCountOffset(buffer.capacity()), currentTupleCount + 1);
    }

    public static boolean isFrameOverflowing(ByteBuffer buffer, int length, boolean isReset)
            throws HyracksDataException {

        int currentTupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()));
        if (currentTupleCount == 0 || isReset) {
            return length + 4 + 4 > buffer.capacity();
        }
        int currentTupleEndOffset = buffer.getInt(FrameHelper.getTupleCountOffset(buffer.capacity()) - 4
                * currentTupleCount);
        return currentTupleEndOffset + length + 4 + (currentTupleCount + 1) * 4 > buffer.capacity();
    }
}
