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
package org.apache.hyracks.api.comm;

import java.nio.ByteBuffer;

import org.apache.hyracks.util.IntSerDeUtils;

public class FrameHelper {
    public static int getTupleCountOffset(int frameSize) {
        return frameSize - FrameConstants.SIZE_LEN;
    }

    /**
     * The actual frameSize = frameCount * intitialFrameSize
     * This method is used to put that frameCount into the first int of the frame buffer.
     * @param outputFrame
     * @param numberOfMinFrame
     */
    public static void serializeFrameSize(ByteBuffer outputFrame, int numberOfMinFrame) {
        serializeFrameSize(outputFrame, 0, numberOfMinFrame);
    }

    public static void serializeFrameSize(ByteBuffer outputFrame, int start, int numberOfMinFrame) {
        IntSerDeUtils.putInt(outputFrame.array(), start + FrameConstants.META_DATA_FRAME_COUNT_OFFSET,
                numberOfMinFrame);
    }

    public static int deserializeNumOfMinFrame(ByteBuffer frame) {
        return deserializeNumOfMinFrame(frame, 0);
    }

    public static int deserializeNumOfMinFrame(ByteBuffer buffer, int start) {
        return IntSerDeUtils.getInt(buffer.array(), start + FrameConstants.META_DATA_FRAME_COUNT_OFFSET);
    }

    /**
     * Add one tuple requires
     * 4bytes to store the tuple offset
     * 4bytes * |fields| to store the relative offset of each field
     * nbytes the actual data.
     * If the tupleLength includes the field slot, please set the fieldCount = 0
     */
    public static int calcRequiredSpace(int fieldCount, int tupleLength) {
        return 4 + fieldCount * 4 + tupleLength;
    }

    /**
     * A faster way of calculating the ceiling
     *
     * @param fieldCount   please set fieldCount to 0 if the tupleLength includes the fields' length
     * @param tupleLength
     * @param minFrameSize
     * @return
     */
    public static int calcAlignedFrameSizeToStore(int fieldCount, int tupleLength, int minFrameSize) {
        assert fieldCount >= 0 && tupleLength >= 0 && minFrameSize > 0;
        return (1 + (calcRequiredSpace(fieldCount, tupleLength) + FrameConstants.META_DATA_LEN - 1) / minFrameSize)
                * minFrameSize;
    }

    public static void clearRemainingFrame(ByteBuffer buffer, int position) {
        buffer.array()[position] = 0;
    }

    public static boolean hasBeenCleared(ByteBuffer buffer, int position) {
        return deserializeNumOfMinFrame(buffer, position) == 0;
    }
}
