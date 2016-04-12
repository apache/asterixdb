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

public interface IFrame {

    ByteBuffer getBuffer();

    /**
     * Make sure the frameSize is bigger or equal to the given size
     *
     * @param frameSize
     * @throws HyracksDataException
     */
    void ensureFrameSize(int frameSize) throws HyracksDataException;

    /**
     *
     * Expand of shrink the inner buffer to make the size exactly equal to {@code frameSize}
     * @param frameSize
     */
    void resize(int frameSize) throws HyracksDataException;

    /**
     * Return the size of frame in bytes
     *
     * @return
     */
    int getFrameSize();

    /**
     * Return the minimum frame size which should read from the configuration file given by user
     *
     * @return
     */
    int getMinSize();

    /**
     * Reset the status of buffer, prepare to the next round of read/write
     */
    void reset() throws HyracksDataException;

}
