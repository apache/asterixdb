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
package org.apache.asterix.column.bytes.stream.out;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.asterix.column.bytes.stream.out.pointer.IReservedPointer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Extends {@link OutputStream} to include methods needed by {@link ValuesWriter}
 */
public abstract class AbstractBytesOutputStream extends OutputStream {
    private final ParquetBytesInput bytesInput;

    protected AbstractBytesOutputStream() {
        bytesInput = new ParquetBytesInput(this);
    }

    @Override
    public abstract void write(int b) throws IOException;

    @Override
    public final void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public abstract void write(byte[] b, int off, int len) throws IOException;

    public final void write(IValueReference value) throws IOException {
        write(value.getByteArray(), value.getStartOffset(), value.getLength());
    }

    public final BytesInput asBytesInput() {
        return bytesInput;
    }

    public abstract void finish();

    /**
     * Reset output stream
     */
    public abstract void reset() throws HyracksDataException;

    /**
     * Reserve a byte at the current position of the stream
     *
     * @param pointer pointer that references the current position
     */
    public abstract void reserveByte(IReservedPointer pointer) throws IOException;

    /**
     * Reserve an integer at the current position of the stream
     *
     * @param pointer pointer that references the current position
     */
    public abstract void reserveInteger(IReservedPointer pointer) throws IOException;

    /**
     * @return a reusable instance of {@link IReservedPointer}
     */
    public abstract IReservedPointer createPointer();

    /**
     * @return Size of written value
     */
    public abstract int size();

    /**
     * @return Allocated buffer size
     */
    public abstract int capacity();

    /**
     * Write the content to another output stream
     *
     * @param outputStream output stream to write to
     */
    public abstract void writeTo(OutputStream outputStream) throws IOException;
}
