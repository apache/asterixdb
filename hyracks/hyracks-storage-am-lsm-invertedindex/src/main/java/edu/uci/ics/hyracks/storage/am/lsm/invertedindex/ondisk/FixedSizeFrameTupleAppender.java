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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class FixedSizeFrameTupleAppender {

    private static final int TUPLE_COUNT_SIZE = 4;
    private final int frameSize;
    private final int tupleSize;
    private ByteBuffer buffer;
    private int tupleCount;
    private int tupleDataEndOffset;

    public FixedSizeFrameTupleAppender(int frameSize, ITypeTraits[] fields) {
        this.frameSize = frameSize;
        int tmp = 0;
        for (int i = 0; i < fields.length; i++) {
            tmp += fields[i].getFixedLength();
        }
        tupleSize = tmp;
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        this.buffer = buffer;
        if (clear) {
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            tupleCount = 0;
            tupleDataEndOffset = 0;
        }
    }

    public boolean append(byte[] bytes, int offset) {
        if (tupleDataEndOffset + tupleSize + TUPLE_COUNT_SIZE <= frameSize) {
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset, tupleSize);
            tupleDataEndOffset += tupleSize;
            tupleCount++;
            return true;
        }
        return false;
    }

    public boolean append(byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + length + TUPLE_COUNT_SIZE <= frameSize) {
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            return true;
        }
        return false;
    }

    public boolean append(int fieldValue) {
        if (tupleDataEndOffset + 4 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.putInt(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 4;
            tupleCount++;
            return true;
        }
        return false;
    }

    public boolean append(long fieldValue) {
        if (tupleDataEndOffset + 8 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.putLong(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 8;
            tupleCount++;
            return true;
        }
        return false;
    }

    public boolean append(char fieldValue) {
        if (tupleDataEndOffset + 2 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.putLong(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 2;
            tupleCount++;
            return true;
        }
        return false;
    }

    public boolean append(byte fieldValue) {
        if (tupleDataEndOffset + 1 + TUPLE_COUNT_SIZE <= frameSize) {
            buffer.put(tupleDataEndOffset, fieldValue);
            tupleDataEndOffset += 1;
            tupleCount++;
            return true;
        }
        return false;
    }

    // returns true if an entire tuple fits
    // returns false otherwise
    public boolean hasSpace() {
        return tupleDataEndOffset + tupleSize + TUPLE_COUNT_SIZE <= frameSize;
    }

    public void incrementTupleCount(int count) {
        buffer.putInt(FrameHelper.getTupleCountOffset(frameSize),
                buffer.getInt(FrameHelper.getTupleCountOffset(frameSize)) + count);
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
