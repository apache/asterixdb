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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameAppender;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This appender can appendTuple and appendField but at the expense of additional checks.
 * Please use this Field/Tuple mixed appender only if you don't know the sequence of append functions.
 * Try using {@link FrameFixedFieldAppender} if you only want to appendFields.
 * and using {@link FrameTupleAppender} if you only want to appendTuples.
 */
public class FrameFixedFieldTupleAppender implements IFrameTupleAppender, IFrameFieldAppender {

    private FrameFixedFieldAppender fieldAppender;
    private FrameTupleAppender tupleAppender;
    private IFrame sharedFrame;
    private IFrameAppender lastAppender;

    public FrameFixedFieldTupleAppender(int numFields) {
        tupleAppender = new FrameTupleAppender();
        fieldAppender = new FrameFixedFieldAppender(numFields);
        lastAppender = tupleAppender;
    }

    private void resetAppenderIfNecessary(IFrameAppender appender) throws HyracksDataException {
        if (lastAppender != appender) {
            if (lastAppender == fieldAppender) {
                if (fieldAppender.hasLeftOverFields()) {
                    throw new HyracksDataException("The previous appended fields haven't been flushed yet.");
                }
            }
            appender.reset(sharedFrame, false);
            lastAppender = appender;
        }
    }

    @Override
    public boolean appendField(byte[] bytes, int offset, int length) throws HyracksDataException {
        resetAppenderIfNecessary(fieldAppender);
        return fieldAppender.appendField(bytes, offset, length);
    }

    @Override
    public boolean appendField(IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        resetAppenderIfNecessary(fieldAppender);
        return fieldAppender.appendField(accessor, tid, fid);
    }

    @Override
    public boolean append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(tupleAccessor, tIndex);
    }

    @Override
    public boolean append(int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(fieldSlots, bytes, offset, length);
    }

    @Override
    public boolean append(byte[] bytes, int offset, int length) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(bytes, offset, length);
    }

    @Override
    public boolean appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendSkipEmptyField(fieldSlots, bytes, offset, length);
    }

    @Override
    public boolean append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.append(tupleAccessor, tStartOffset, tEndOffset);
    }

    @Override
    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendConcat(accessor0, tIndex0, accessor1, tIndex1);
    }

    @Override
    public boolean appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1,
            int offset1, int dataLen1) throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendConcat(accessor0, tIndex0, fieldSlots1, bytes1, offset1, dataLen1);
    }

    @Override
    public boolean appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields)
            throws HyracksDataException {
        resetAppenderIfNecessary(tupleAppender);
        return tupleAppender.appendProjection(accessor, tIndex, fields);
    }

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        sharedFrame = frame;
        tupleAppender.reset(sharedFrame, clear);
        fieldAppender.reset(sharedFrame, clear);
    }

    @Override
    public int getTupleCount() {
        return lastAppender.getTupleCount();
    }

    @Override
    public ByteBuffer getBuffer() {
        return lastAppender.getBuffer();
    }

    @Override
    public void write(IFrameWriter outWriter, boolean clear) throws HyracksDataException {
        lastAppender.write(outWriter, clear);
    }
}
