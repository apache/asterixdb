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

import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;

public class ArrayBackedValueStorage implements IMutableValueStorage {
   
    private final GrowableArray data = new GrowableArray();

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

    public void append(IValueReference value) {
        try {
            data.append(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void assign(IValueReference value) {
        reset();
        append(value);
    }
}