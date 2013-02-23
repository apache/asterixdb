/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.common.comm.util;

import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class FrameOutputStream extends OutputStream {
    private static final Logger LOGGER = Logger.getLogger(FrameOutputStream.class.getName());

    private final FrameTupleAppender frameTupleAppender;

    public FrameOutputStream(FrameTupleAppender frameTupleAppender) {
        this.frameTupleAppender = frameTupleAppender;
    }

    @Override
    public void write(int intByte) {
        byte[] b = { (byte) intByte };
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("write(int): byte: " + b);
        }
        if (!frameTupleAppender.append(b, 0, 1)) {
            throw new BufferOverflowException();
        }
    }

    @Override
    public void write(byte[] bytes) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("write(bytes[]): bytes: " + bytes);
        }
        if (!frameTupleAppender.append(bytes, 0, bytes.length)) {
            throw new BufferOverflowException();
        }
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("write(bytes[]): bytes: " + bytes);
        }
        if (!frameTupleAppender.append(bytes, offset, length)) {
            throw new BufferOverflowException();
        }
    }
}
