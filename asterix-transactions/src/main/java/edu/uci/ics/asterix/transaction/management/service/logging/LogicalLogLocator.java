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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a location of a log record. The location has two parts to it. A
 * LogicalLogLocator extends PhyscialLogLocator and hence can also be used to
 * determine the physical location of the log record on the local filesystem. In
 * addition to the physical location, a LogicalLogLocator also contains a handle
 * to an in-memory buffer and an offset within the buffer where the log record
 * resides.
 */
public class LogicalLogLocator extends PhysicalLogLocator {

    private IBuffer buffer;
    private AtomicInteger memoryOffset;

    public LogicalLogLocator(long lsnValue, IBuffer buffer, int bufferOffset, ILogManager logManager) {
        super(lsnValue, logManager);
        this.buffer = buffer;
        this.memoryOffset = new AtomicInteger(bufferOffset);

    }

    public IBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(IBuffer buffer) {
        this.buffer = buffer;
    }

    public int getMemoryOffset() {
        return memoryOffset.get();
    }

    public void setMemoryOffset(int memoryOffset) {
        this.memoryOffset.set(memoryOffset);
    }

    @Override
    public String toString() {
        return super.toString() + " " + "memoryOffset:" + memoryOffset;
    }

    public boolean checkValidity() {
        return true;
    }

    public long increaseMemoryOffset(int delta) {
        return memoryOffset.addAndGet(delta);
    }
}
