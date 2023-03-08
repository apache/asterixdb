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

import org.apache.asterix.column.bytes.stream.out.pointer.GrowableBytesPointer;
import org.apache.asterix.column.bytes.stream.out.pointer.IReservedPointer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class GrowableBytesOutputStream extends AbstractBytesOutputStream {
    private final ArrayBackedValueStorage storage;

    public GrowableBytesOutputStream() {
        storage = new ArrayBackedValueStorage(128);
    }

    @Override
    public void write(int b) throws IOException {
        storage.getDataOutput().write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        storage.getDataOutput().write(b, off, len);
    }

    @Override
    public void finish() {
        reset();
    }

    @Override
    public void reset() {
        storage.reset();
    }

    @Override
    public void reserveByte(IReservedPointer pointer) throws IOException {
        ((GrowableBytesPointer) pointer).setPointer(storage.getLength());
        storage.getDataOutput().write(0);
    }

    @Override
    public void reserveInteger(IReservedPointer pointer) throws IOException {
        ((GrowableBytesPointer) pointer).setPointer(storage.getLength());
        storage.getDataOutput().writeInt(0);
    }

    @Override
    public IReservedPointer createPointer() {
        return new GrowableBytesPointer(storage);
    }

    @Override
    public int size() {
        return storage.getLength();
    }

    @Override
    public int capacity() {
        return storage.getByteArray().length;
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        outputStream.write(storage.getByteArray(), storage.getStartOffset(), storage.getLength());
    }
}
