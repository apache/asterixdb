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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TestFrameWriter implements IFrameWriter {
    private final CountAnswer openAnswer;
    private final CountAnswer nextAnswer;
    private final CountAnswer flushAnswer;
    private final CountAnswer failAnswer;
    private final CountAnswer closeAnswer;
    private static final int BYTES32KB = 32768;
    private long openDuration = 0L;
    private long nextDuration = 0L;
    private long flushDuration = 0L;
    private long failDuration = 0L;
    private long closeDuration = 0L;
    // If copyFrames was set, we take a copy of the frame, otherwise, we simply point lastFrame to it
    private final boolean deepCopyFrames;
    private ByteBuffer lastFrame;

    public TestFrameWriter(CountAnswer openAnswer, CountAnswer nextAnswer, CountAnswer flushAnswer,
            CountAnswer failAnswer, CountAnswer closeAnswer, boolean deepCopyFrames) {
        this.openAnswer = openAnswer;
        this.nextAnswer = nextAnswer;
        this.closeAnswer = closeAnswer;
        this.flushAnswer = flushAnswer;
        this.failAnswer = failAnswer;
        this.deepCopyFrames = deepCopyFrames;
        if (deepCopyFrames) {
            lastFrame = ByteBuffer.allocate(BYTES32KB);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        delay(openDuration);
        openAnswer.call();
    }

    public int openCount() {
        return openAnswer.getCallCount();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (deepCopyFrames) {
            if (lastFrame.capacity() != buffer.capacity()) {
                lastFrame = ByteBuffer.allocate(buffer.capacity());
            }
            lastFrame.clear();
            lastFrame.put(buffer.array());
        } else {
            lastFrame = buffer;
        }
        delay(nextDuration);
        nextAnswer.call();
    }

    public int nextFrameCount() {
        return nextAnswer.getCallCount();
    }

    @Override
    public void flush() throws HyracksDataException {
        delay(flushDuration);
        flushAnswer.call();
    }

    public int flushCount() {
        return flushAnswer.getCallCount();
    }

    @Override
    public void fail() throws HyracksDataException {
        delay(failDuration);
        failAnswer.call();
    }

    public int failCount() {
        return failAnswer.getCallCount();
    }

    @Override
    public void close() throws HyracksDataException {
        delay(closeDuration);
        closeAnswer.call();
    }

    public int closeCount() {
        return closeAnswer.getCallCount();
    }

    public synchronized boolean validate(boolean finished) {
        if (failAnswer.getCallCount() > 1 || closeAnswer.getCallCount() > 1 || openAnswer.getCallCount() > 1) {
            return false;
        }
        if (openAnswer.getCallCount() == 0
                && (nextAnswer.getCallCount() > 0 || failAnswer.getCallCount() > 0 || closeAnswer.getCallCount() > 0)) {
            return false;
        }
        if (finished) {
            if (closeAnswer.getCallCount() == 0 && (nextAnswer.getCallCount() > 0 || failAnswer.getCallCount() > 0
                    || openAnswer.getCallCount() > 0)) {
                return false;
            }
        }
        return true;
    }

    public void reset() {
        openAnswer.reset();
        nextAnswer.reset();
        flushAnswer.reset();
        failAnswer.reset();
        closeAnswer.reset();
    }

    public long getOpenDuration() {
        return openDuration;
    }

    public void setOpenDuration(long openDuration) {
        this.openDuration = openDuration;
    }

    public long getNextDuration() {
        return nextDuration;
    }

    public void setNextDuration(long nextDuration) {
        this.nextDuration = nextDuration;
    }

    public long getFlushDuration() {
        return flushDuration;
    }

    public void setFlushDuration(long flushDuration) {
        this.flushDuration = flushDuration;
    }

    public long getFailDuration() {
        return failDuration;
    }

    public void setFailDuration(long failDuration) {
        this.failDuration = failDuration;
    }

    public long getCloseDuration() {
        return closeDuration;
    }

    public void setCloseDuration(long closeDuration) {
        this.closeDuration = closeDuration;
    }

    private void delay(long duration) throws HyracksDataException {
        if (duration > 0) {
            try {
                synchronized (this) {
                    wait(duration);
                }
            } catch (InterruptedException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    public ByteBuffer getLastFrame() {
        return lastFrame;
    }
}
