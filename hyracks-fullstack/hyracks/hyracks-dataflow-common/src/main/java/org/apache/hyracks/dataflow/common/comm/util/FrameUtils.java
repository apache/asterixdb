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
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.trace.ITracer;

public class FrameUtils {

    private FrameUtils() {
    }

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
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, length);
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
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, length);
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
            IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset) throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.append(tupleAccessor, tStartOffset, tEndOffset)) {
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME,
                        tEndOffset - tStartOffset);
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
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME,
                        tupleAccessor.getTupleLength(tIndex));
            }
        }
        return flushedBytes;
    }

    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender frameTupleAppender,
            IFrameTupleAccessor tupleAccessor, int tIndex, ITracer tracer, String name, long cat, String args)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            long tid = tracer.durationB(name, cat, args);
            frameTupleAppender.write(writer, true);
            tracer.durationE(tid, cat, args);
            if (!frameTupleAppender.append(tupleAccessor, tIndex)) {
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME,
                        tupleAccessor.getTupleLength(tIndex));
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
    public static int appendToWriter(IFrameWriter writer, IFrameTupleAppender tupleAppender, int[] fieldEndOffsets,
            byte[] byteArray, int start, int size) throws HyracksDataException {
        int flushedBytes = 0;
        if (!tupleAppender.append(fieldEndOffsets, byteArray, start, size)) {

            flushedBytes = tupleAppender.getBuffer().capacity();
            tupleAppender.write(writer, true);

            if (!tupleAppender.append(fieldEndOffsets, byteArray, start, size)) {
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, size);
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
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME,
                        accessor0.getTupleLength(tIndex0) + accessor1.getTupleLength(tIndex1));
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
            IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1, int dataLen1)
            throws HyracksDataException {
        int flushedBytes = 0;
        if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
            flushedBytes = frameTupleAppender.getBuffer().capacity();
            frameTupleAppender.write(writer, true);
            if (!frameTupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1)) {
                int startOffset0 = accessor0.getTupleStartOffset(tIndex0);
                int endOffset0 = accessor0.getTupleEndOffset(tIndex0);
                int length0 = endOffset0 - startOffset0;
                int slotsLen1 = fieldSlots1.length * Integer.BYTES;
                int length1 = slotsLen1 + dataLen1;
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, length0 + length1);
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
                int fTargetSlotsLength = fields.length * Integer.BYTES;
                int length = fTargetSlotsLength;
                for (int i = 0; i < fields.length; ++i) {
                    length += (accessor.getFieldEndOffset(tIndex, fields[i])
                            - accessor.getFieldStartOffset(tIndex, fields[i]));
                }
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, length);
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
    public static int appendFieldToWriter(IFrameWriter writer, IFrameFieldAppender appender, byte[] array, int start,
            int length) throws HyracksDataException {
        int flushedBytes = 0;
        if (!appender.appendField(array, start, length)) {
            flushedBytes = appender.getBuffer().capacity();
            appender.write(writer, true);
            if (!appender.appendField(array, start, length)) {
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, length);
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
                int fStartOffset = accessor.getFieldStartOffset(tid, fid);
                int fLen = accessor.getFieldEndOffset(tid, fid) - fStartOffset;
                throw HyracksDataException.create(ErrorCode.TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME, fLen);
            }
        }
        return flushedBytes;
    }
}
