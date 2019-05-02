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

import java.util.Arrays;

class FrameFreeSlotLastFit implements IFrameFreeSlotPolicy {
    private static int INITIAL_CAPACITY = 10;

    private class FrameSpace {
        private int frameId;
        private int freeSpace;

        FrameSpace(int frameId, int freeSpace) {
            reset(frameId, freeSpace);
        }

        void reset(int frameId, int freeSpace) {
            this.frameId = frameId;
            this.freeSpace = freeSpace;
        }
    }

    private final int initialNumFrames;
    private FrameSpace[] frameSpaces;
    private int size;

    FrameFreeSlotLastFit(int initialFrameNumber) {
        initialNumFrames = initialFrameNumber;
        frameSpaces = new FrameSpace[initialNumFrames];
        size = 0;
    }

    FrameFreeSlotLastFit() {
        this(INITIAL_CAPACITY);
    }

    @Override
    public int popBestFit(int tobeInsertedSize) {
        for (int i = size - 1; i >= 0; i--) {
            if (frameSpaces[i].freeSpace >= tobeInsertedSize) {
                FrameSpace ret = frameSpaces[i];
                System.arraycopy(frameSpaces, i + 1, frameSpaces, i, size - i - 1);
                frameSpaces[--size] = ret;
                return ret.frameId;
            }
        }
        return -1;
    }

    @Override
    public void pushNewFrame(int frameID, int freeSpace) {
        if (frameSpaces == null) {
            frameSpaces = new FrameSpace[initialNumFrames];
        }
        if (size >= frameSpaces.length) {
            frameSpaces = Arrays.copyOf(frameSpaces, size * 2);
        }
        if (frameSpaces[size] == null) {
            frameSpaces[size++] = new FrameSpace(frameID, freeSpace);
        } else {
            frameSpaces[size++].reset(frameID, freeSpace);
        }
    }

    @Override
    public void reset() {
        size = 0;
    }

    @Override
    public void close() {
        size = 0;
        frameSpaces = null;
    }
}
