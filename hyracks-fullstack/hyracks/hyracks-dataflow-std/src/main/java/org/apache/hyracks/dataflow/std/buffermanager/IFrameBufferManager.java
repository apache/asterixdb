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
package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Manages the buffer space in the unit of frame.
 */
public interface IFrameBufferManager {

    /**
     * Resets the counters and flags to initial status. This method should not release the pre-allocated resources
     *
     * @throws org.apache.hyracks.api.exceptions.HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * @param frameIndex index of frame requested
     * @param bufferInfo the given object need to be reset
     * @return the filled bufferInfo to facilitate the chain access
     */
    BufferInfo getFrame(int frameIndex, BufferInfo bufferInfo);

    /**
     * @return the number of frames in this buffer
     */
    int getNumFrames();

    /**
     * Writes the whole frame into the buffer.
     *
     * @param frame source frame
     * @return the id of the inserted frame. return -1 if it failed to insert
     */
    int insertFrame(ByteBuffer frame) throws HyracksDataException;

    /**
     * Releases the allocated resources.
     */
    void close();

}
