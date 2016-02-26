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

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IFrameAppender {

    /**
     * Reset to attach to a new frame.
     *
     * @param frame the new frame
     * @param clear indicate whether we need to clear this new frame
     * @throws HyracksDataException
     */
    void reset(IFrame frame, boolean clear) throws HyracksDataException;

    /**
     * Get how many tuples in current frame.
     *
     * @return
     */
    int getTupleCount();

    /**
     * Get the ByteBuffer which contains the frame data.
     *
     * @return
     */
    ByteBuffer getBuffer();

    /**
     * Write the frame content to the given writer.
     * Clear the inner buffer after write if {@code clear} is <code>true</code>.
     *
     * @param outWriter the output writer
     * @param clear     indicate whether to clear the inside frame after writing or not.
     * @throws HyracksDataException
     */
    void write(IFrameWriter outWriter, boolean clear) throws HyracksDataException;

    /**
     * Write currently buffered records to {@code writer} then flushes {@code writer}. The inside frame is always cleared
     * @param writer the FrameWriter to write to and flush
     * @throws HyracksDataException
     */
    public default void flush(IFrameWriter writer) throws HyracksDataException {
        write(writer, true);
        writer.flush();
    }
}
