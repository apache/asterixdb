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
package org.apache.hyracks.dataflow.common.comm.util;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FrameUtils {

    public static void copyWholeFrame(ByteBuffer srcFrame, ByteBuffer destFrame) {
        srcFrame.clear();
        destFrame.clear();
        destFrame.put(srcFrame);
    }

    public static void copyAndFlip(ByteBuffer srcFrame, ByteBuffer destFrame) {
        srcFrame.position(0);
        destFrame.clear();
        destFrame.put(srcFrame);
        destFrame.flip();
    }

    public static void flushFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        writer.nextFrame(buffer);
        buffer.clear();
    }

    /**
     * A util function to append the data to appender. If the appender buffer is full, it will directly flush
     * to the given writer, which saves the detecting logic in the caller.
     * It will return the bytes that have been flushed.
     *
     * @param writer
     * @param frameTupleAppender
     * @param fieldSlots
     * @param bytes
     * @param offset
     * @param length
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendSkipEmptyFieldToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * A util function to append the data to appender. If the appender buffer is full, it will directly flush
     * to the given writer, which saves the detecting logic in the caller.
     * It will return the bytes that have been flushed.
     *
     * @param writer
     * @param frameTupleAppender
     * @param bytes
     * @param offset
     * @param length
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender, byte[] bytes,
            int offset, int length) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(bytes, offset, length)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.append(bytes, offset, length)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param tupleAccessor
     * @param tStartOffset
     * @param tEndOffset
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param tupleAccessor
     * @param tIndex
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param tupleAppender
     * @param fieldEndOffsets
     * @param byteArray
     * @param start
     * @param size
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender tupleAppender,
            int[] fieldEndOffsets, byte[] byteArray, int start, int size) throws HyracksDataException {
        int flushedBytes = 0;
        if (!tupleAppender.append(fieldEndOffsets, byteArray, start, size)) {

            flushedBytes = tupleAppender.getBuffer().capacity();
            tupleAppender.write(writer, true);

            if (!tupleAppender.append(fieldEndOffsets, byteArray, start, size)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param accessor0
     * @param tIndex0
     * @param accessor1
     * @param tIndex1
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendConcatToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param accessor0
     * @param tIndex0
     * @param fieldSlots1
     * @param bytes1
     * @param offset1
     * @param dataLen1
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendConcatToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1,
            int dataLen1) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param frameTupleAppender
     * @param accessor
     * @param tIndex
     * @param fields
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendProjectionToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor accessor, int tIndex, int[] fields) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendProjection(accessor, tIndex, fields)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.appendProjection(accessor, tIndex, fields)) {
                throw new HyracksDataException("The output cannot be fit into a frame.");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param appender
     * @param array
     * @param start
     * @param length
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender, byte[] array,
            int start, int length) throws HyracksDataException {
        int flushedBytes = 0;
        if (!appender.appendField(array, start, length)) {
            flushedBytes = appender.getBuffer().capacity();
            appender.write(writer, true);
            if (!appender.appendField(array, start, length)) {
                throw new HyracksDataException("Could not write frame: the size of the tuple is too long");
            }
        }
        return flushedBytes;
    }

    /**
     * @param writer
     * @param appender
     * @param accessor
     * @param tid
     * @param fid
     * @return the number of bytes that have been flushed, 0 if not get flushed.
     * @throws HyracksDataException
     */
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender,
            IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        int flushedBytes = 0;
        if (!appender.appendField(accessor, tid, fid)) {
            flushedBytes = appender.getBuffer().capacity();
            appender.write(writer, true);
            if (!appender.appendField(accessor, tid, fid)) {
                throw new HyracksDataException("Could not write frame: the size of the tuple is too long");
            }
        }
        return flushedBytes;
    }

}
