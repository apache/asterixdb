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

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a wrapper over @see ByteBuffer supporting some custom APIs for
 * transaction support. This class is not "thread-safe". For performance
 * concerns, it is required for multiple writers to be able to write to the
 * buffer concurrently and that a writer is never blocked by another writer. The
 * users of this class must ensure that two concurrent writers get to write in
 * exclusive areas in the buffer. A reader and writer may or may not conflict
 * with each other. For example, reading of logs during roll back of a
 * transaction t1 does not conflict with writing of logs by another transaction
 * t2 as they are concerned with exclusive areas of the buffer. On the contrary,
 * a flushing the buffer to disk conflicts with a reader reading the buffer.
 * Appropriate locks are taken on the Buffer in the application logic and not
 * directly imposed by synchronized methods.
 */

public class Buffer implements IBuffer {

    ByteBuffer buffer;

    public Buffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    protected Buffer() {
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte getByte(int offset) {
        return buffer.get(offset);
    }

    @Override
    public byte getByte() {
        return buffer.get();
    }

    @Override
    public void getBytes(byte[] bytes, int offset, int size) {
        System.arraycopy(buffer.array(), offset, bytes, 0, size);
    }

    @Override
    public int getSize() {
        return buffer.capacity();
    }

    @Override
    public int readInt() {
        return buffer.getInt();
    }

    @Override
    public int readInt(int offset) {
        return buffer.getInt(offset);
    }

    @Override
    public long readLong(int offset) {
        return buffer.getLong(offset);
    }

    @Override
    public void put(byte b) {
        buffer.put(b);
    }

    @Override
    public void put(int offset, byte b) {
        buffer.put(offset, b);
    }

    @Override
    public void put(byte[] bytes, int start, int length) {
        buffer.put(bytes, start, length);

    }

    @Override
    public void put(byte[] bytes) {
        buffer.put(bytes);
    }

    @Override
    public void writeInt(int value) {
        buffer.putInt(value);
    }

    @Override
    public void writeInt(int offset, int value) {
        buffer.putInt(offset, value);

    }

    @Override
    public void writeLong(long value) {
        buffer.putLong(value);
    }

    @Override
    public void writeLong(int offset, long value) {
        buffer.putLong(offset, value);

    }

    @Override
    public byte[] getArray() {
        return buffer.array();
    }

    @Override
    public void erase() {
        Arrays.fill(buffer.array(), (byte) 0);
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return buffer;
    }

}
