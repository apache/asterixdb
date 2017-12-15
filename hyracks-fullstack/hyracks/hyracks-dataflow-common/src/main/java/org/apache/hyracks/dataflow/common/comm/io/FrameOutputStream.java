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
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FrameOutputStream extends ByteArrayAccessibleOutputStream {
    private static final Logger LOGGER = LogManager.getLogger();

    private final FrameTupleAppender frameTupleAppender;

    public FrameOutputStream(int initialStreamCapaciy) {
        super(initialStreamCapaciy);
        this.frameTupleAppender = new FrameTupleAppender();
    }

    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        frameTupleAppender.reset(frame, clear);
    }

    public int getTupleCount() {
        int tupleCount = frameTupleAppender.getTupleCount();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("appendTuple(): tuple count: " + tupleCount);
        }
        return tupleCount;
    }

    public boolean appendTuple() throws HyracksDataException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("appendTuple(): tuple size: " + count);
        }
        boolean appended = frameTupleAppender.append(buf, 0, count);
        count = 0;
        return appended;
    }

    public void flush(IFrameWriter writer) throws HyracksDataException {
        frameTupleAppender.write(writer, true);
    }
}
