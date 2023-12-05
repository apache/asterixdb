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
package org.apache.asterix.column.bytes.encoder;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Replaces {@link ValuesWriter}
 */
public abstract class AbstractParquetValuesWriter {

    public abstract BytesInput getBytes();

    /**
     * called after getBytes() to reset the current buffer and start writing the next page
     */
    public abstract void reset() throws HyracksDataException;

    /**
     * Called to close the values writer. Any output stream is closed and can no longer be used.
     * All resources are released.
     */
    public abstract void close();

    /**
     * @return the current (mostly) overestimated size needed to flush this writer
     */
    public abstract int getEstimatedSize();

    /**
     * @param length the length of value to be return
     * @return (probably) an overestimated size needed to write a value with the given length
     */
    public int calculateEstimatedSize(int length) {
        return length;
    }

    /**
     * @return the allocated size of the buffer
     */
    public abstract int getAllocatedSize();

    /**
     * @param v the value to encode
     */
    public void writeBoolean(boolean v) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @param v               the value to encode
     * @param skipLengthBytes whether to skip the length bytes of {@link UTF8StringPointable} or not
     */
    public void writeBytes(IValueReference v, boolean skipLengthBytes) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @param v the value to encode
     */
    public void writeInteger(int v) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @param v the value to encode
     */
    public void writeFloat(float v) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @param v the value to encode
     */
    public void writeLong(long v) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * @param v the value to encode
     */
    public void writeDouble(double v) {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
