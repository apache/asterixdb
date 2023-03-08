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
package org.apache.asterix.column.bytes.stream.out.pointer;

import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class GrowableBytesPointer implements IReservedPointer {
    private final ArrayBackedValueStorage storage;
    private int offset;

    public GrowableBytesPointer(ArrayBackedValueStorage storage) {
        this.storage = storage;
    }

    public void setPointer(int offset) {
        this.offset = offset;
    }

    @Override
    public void setByte(byte value) {
        storage.getByteArray()[offset] = value;
    }

    @Override
    public void setInteger(int value) {
        IntegerPointable.setInteger(storage.getByteArray(), offset, value);
    }

    @Override
    public void reset() {
        offset = -1;
    }

    @Override
    public boolean isSet() {
        return offset >= 0;
    }
}
