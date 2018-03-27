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
package org.apache.hyracks.api.test;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TestControlledFrameWriter extends TestFrameWriter {
    private boolean frozen = false;
    private boolean timed = false;
    private long duration = Long.MAX_VALUE;
    private final int initialFrameSize;
    private volatile int currentMultiplier = 0;
    private volatile int kicks = 0;

    public TestControlledFrameWriter(int initialFrameSize, boolean deepCopyInputFrames) {
        super(new CountAnswer(), new CountAnswer(), new CountAnswer(), new CountAnswer(), new CountAnswer(),
                deepCopyInputFrames);
        this.initialFrameSize = initialFrameSize;
    }

    public int getCurrentMultiplier() {
        return currentMultiplier;
    }

    public synchronized void freeze() {
        frozen = true;
    }

    public synchronized void time(long ms) {
        frozen = true;
        timed = true;
        duration = ms;
    }

    public synchronized void unfreeze() throws InterruptedException {
        wait(10);
        frozen = false;
        notify();
    }

    public synchronized void kick() {
        kicks++;
        notify();
    }

    @Override
    public synchronized void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        super.nextFrame(buffer);
        currentMultiplier = buffer.capacity() / initialFrameSize;
        if (frozen) {
            try {
                if (timed) {
                    wait(duration);
                } else {
                    while (frozen && kicks == 0) {
                        wait();
                    }
                    kicks--;
                }
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }
        currentMultiplier = 0;
    }
}
