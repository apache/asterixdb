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

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

public class FrameFixedFieldAppender extends AbstractFrameAppender implements IFrameFieldAppender {
    private final int fieldCount;
    private int lastFieldEndOffset;
    private int currentField;
    private int leftOverSize;
    private byte[] cachedLeftOverFields;

    public FrameFixedFieldAppender(int numberFields) {
        this.fieldCount = numberFields;
        this.lastFieldEndOffset = 0;
        this.currentField = 0;
        this.leftOverSize = 0;
    }

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        super.reset(frame, clear);
        lastFieldEndOffset = 0;
        currentField = 0;
        leftOverSize = 0;
    }

    /**
     * Reset frame states and copy the left over data into the new frame
     *
     * @param frame
     * @throws HyracksDataException
     */
    public void resetWithLeftOverData(IFrame frame) throws HyracksDataException {
        super.reset(frame, true);
        copyLeftOverDataFromeBufferToFrame();
    }

    @Override
    public void write(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        super.write(outWriter, clearFrame);
        if (clearFrame) {
            copyLeftOverDataFromeBufferToFrame();
        }
    }

    public boolean appendField(byte[] bytes, int offset, int length) throws HyracksDataException {
        if (canHoldNewTuple(fieldCount, lastFieldEndOffset + length)) {
            int currentFieldDataStart = tupleDataEndOffset + fieldCount * 4 + lastFieldEndOffset;
            System.arraycopy(bytes, offset, array, currentFieldDataStart, length);
            lastFieldEndOffset = lastFieldEndOffset + length;
            IntSerDeUtils.putInt(array, tupleDataEndOffset + currentField * 4, lastFieldEndOffset);
            if (++currentField == fieldCount) {
                tupleDataEndOffset += fieldCount * 4 + lastFieldEndOffset;
                IntSerDeUtils.putInt(array,
                        FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1),
                        tupleDataEndOffset);
                ++tupleCount;
                IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), tupleCount);

                //reset for the next tuple
                currentField = 0;
                lastFieldEndOffset = 0;
            }
            return true;
        } else {
            if (currentField > 0) {
                copyLeftOverDataFromFrameToBuffer();
            }
            return false;
        }
    }

    private void copyLeftOverDataFromFrameToBuffer() {
        leftOverSize = lastFieldEndOffset + fieldCount * 4;
        if (cachedLeftOverFields == null || cachedLeftOverFields.length < leftOverSize) {
            cachedLeftOverFields = new byte[leftOverSize];
        }
        System.arraycopy(array, tupleDataEndOffset, cachedLeftOverFields, 0, leftOverSize);
    }

    private void copyLeftOverDataFromeBufferToFrame() throws HyracksDataException {
        if (leftOverSize > 0) {
            if (!canHoldNewTuple(0, leftOverSize)) {
                throw new HyracksDataException(
                        "The given frame can not be extended to insert the leftover data from the last record");
            }
            System.arraycopy(cachedLeftOverFields, 0, array, tupleDataEndOffset, leftOverSize);
            leftOverSize = 0;
        }
    }

    public boolean appendField(IFrameTupleAccessor fta, int tIndex, int fIndex) throws HyracksDataException {
        int startOffset = fta.getTupleStartOffset(tIndex);
        int fStartOffset = fta.getFieldStartOffset(tIndex, fIndex);
        int fLen = fta.getFieldEndOffset(tIndex, fIndex) - fStartOffset;
        return appendField(fta.getBuffer().array(), startOffset + fta.getFieldSlotsLength() + fStartOffset, fLen);
    }

    public boolean hasLeftOverFields() {
        return currentField != 0;
    }
}
