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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * This class wraps the calls of FrameTupleAppender and
 * allows user to not worry about flushing full frames.
 * TODO(yingyib): cleanup existing usage of FrameTupleAppender.
 *
 * @author yingyib
 */
public class FrameTupleAppenderWrapper {
    private final IFrameTupleAppender frameTupleAppender;
    private final IFrameWriter outputWriter;

    public FrameTupleAppenderWrapper(IFrameTupleAppender frameTupleAppender, IFrameWriter outputWriter) {
        this.frameTupleAppender = frameTupleAppender;
        this.outputWriter = outputWriter;
    }

    public void open() throws HyracksDataException {
        outputWriter.open();
    }

    public void write() throws HyracksDataException {
        frameTupleAppender.write(outputWriter, true);
    }

    public void flush() throws HyracksDataException {
        frameTupleAppender.flush(outputWriter);
    }

    public void close() throws HyracksDataException {
        outputWriter.close();
    }

    public void fail() throws HyracksDataException {
        outputWriter.fail();
    }

    public void reset(IFrame buffer, boolean clear) throws HyracksDataException {
        frameTupleAppender.reset(buffer, clear);
    }

    public void appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length)
            throws HyracksDataException {
        FrameUtils.appendSkipEmptyFieldToWriter(outputWriter, frameTupleAppender, fieldSlots, bytes, offset, length);
    }

    public void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        FrameUtils.appendConcatToWriter(outputWriter, frameTupleAppender, accessor0, tIndex0, accessor1, tIndex1);
    }

    public void appendConcat(IFrameTupleAccessor accessor0, int tIndex0, int[] fieldSlots1, byte[] bytes1, int offset1,
            int dataLen1) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(outputWriter, frameTupleAppender, accessor0, tIndex0, fieldSlots1, bytes1,
                offset1, dataLen1);
    }

    public void appendProjection(IFrameTupleAccessor accessor, int tIndex, int[] fields) throws HyracksDataException {
        FrameUtils.appendProjectionToWriter(outputWriter, frameTupleAppender, accessor, tIndex, fields);
    }

}
