
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
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;

// This SingleFrameBufferManager is used for **scan** purpose.
// When scanning an inverted index, we will load the pages from disk to memory one by one,
// and this single frame buffer manager will allocate only one frame at the same time.
//
// Note: this buffer manager is NOT thread-safe
public class SingleFrameBufferManager implements ISimpleFrameBufferManager {
    boolean isAcquired = false;
    ByteBuffer buffer = null;

    @Override
    public ByteBuffer acquireFrame(int frameSize) throws HyracksDataException {
        if (buffer == null) {
            buffer = ByteBuffer.allocate(frameSize);
        }

        if (isAcquired) {
            return null;
        } else {
            if (buffer.capacity() >= frameSize) {
                isAcquired = true;
                buffer.clear();
                Arrays.fill(buffer.array(), (byte) 0);
                return buffer;
            } else {
                throw new HyracksDataException("Frame size changed");
            }
        }
    }

    @Override
    public void releaseFrame(ByteBuffer frame) {
        buffer.clear();
        isAcquired = false;
    }
}
