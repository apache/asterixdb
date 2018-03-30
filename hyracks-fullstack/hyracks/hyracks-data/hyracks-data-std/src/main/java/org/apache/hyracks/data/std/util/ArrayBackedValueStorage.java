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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;

public class ArrayBackedValueStorage implements IMutableValueStorage, IPointable {

    private final GrowableArray data;

    public ArrayBackedValueStorage(int size) {
        data = new GrowableArray(size);
    }

    public ArrayBackedValueStorage() {
        data = new GrowableArray();
    }

    @Override
    public void reset() {
        data.reset();
    }

    @Override
    public DataOutput getDataOutput() {
        return data.getDataOutput();
    }

    @Override
    public byte[] getByteArray() {
        return data.getByteArray();
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return data.getLength();
    }

    public void append(IValueReference value) throws HyracksDataException {
        try {
            data.append(value);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void assign(IValueReference value) throws HyracksDataException {
        reset();
        append(value);
    }

    public void setSize(int bytesRequired) {
        data.setSize(bytesRequired);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ArrayBackedValueStorage)) {
            return false;
        }
        ArrayBackedValueStorage other = (ArrayBackedValueStorage) obj;
        return Objects.equals(data, other.data);
    }

    @Override
    public void set(byte[] bytes, int start, int length) {
        reset();
        if (bytes != null) {
            try {
                data.append(bytes, start, length);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public void set(IValueReference pointer) {
        try {
            assign(pointer);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public byte[] toByteArray() {
        byte[] byteArray = new byte[getLength()];
        System.arraycopy(getByteArray(), getStartOffset(), byteArray, 0, getLength());
        return byteArray;
    }

}
