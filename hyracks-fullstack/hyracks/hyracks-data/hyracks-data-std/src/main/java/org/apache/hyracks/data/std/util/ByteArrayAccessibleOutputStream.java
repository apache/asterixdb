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
package org.apache.hyracks.data.std.util;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

public class ByteArrayAccessibleOutputStream extends ByteArrayOutputStream {

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE;

    public ByteArrayAccessibleOutputStream() {
        super();
    }

    public ByteArrayAccessibleOutputStream(int size) {
        super(size);
    }

    public byte[] getByteArray() {
        return buf;
    }

    // Override to make it not synchronized.
    @Override
    public void reset() {
        count = 0;
    }

    @Override
    public void write(int b) {
        ensureCapacity(count + 1);
        buf[count] = (byte) b;
        count += 1;
    }

    /**
     * Rewind the current position by {@code delta} to a previous position.
     * This function is used to drop the already written delta bytes.
     * In some cases, we write some bytes, and afterward we found we've written more than expected.
     * Then we need to fix the position by rewind the current position to the expected one.
     * Currently, it is used by the {@link AbstractVarLenObjectBuilder} which may take more space than required
     * at beginning, and it will shift the data and fix the position whenever required.
     * It will throw {@link IndexOutOfBoundsException} if the {@code delta} is negative.
     * Evil function, use with caution.
     *
     * @param delta
     */
    public void rewindPositionBy(int delta) {
        if (delta < 0 || count < delta) {
            throw new IndexOutOfBoundsException();
        }
        count -= delta;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        ensureCapacity(count + len);
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    private void ensureCapacity(int minCapacity) {
        // overflow-conscious code
        if (minCapacity - buf.length > 0) {
            grow(minCapacity);
        }
    }

    /**
     * Increases the capacity to ensure that it can hold at least the
     * number of elements specified by the minimum capacity argument.
     *
     * @param minCapacity
     *            the desired minimum capacity
     */
    private void grow(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = buf.length;
        // increase by a factor of 1.5
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            newCapacity = hugeCapacity(minCapacity);
        }
        buf = Arrays.copyOf(buf, newCapacity);
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) { // overflow
            throw new RuntimeException("Memory allocation limit (" + MAX_ARRAY_SIZE + " bytes) exceeded");
        }
        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }

    /**
     * Return the current length of this stream (not capacity).
     */
    public int getLength() {
        return count;
    }

    public void setSize(int bytesRequired) {
        ensureCapacity(bytesRequired);
        count = bytesRequired;
    }
}
