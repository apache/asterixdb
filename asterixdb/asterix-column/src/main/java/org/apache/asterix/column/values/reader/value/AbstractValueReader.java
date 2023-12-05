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
package org.apache.asterix.column.values.reader.value;

import java.io.IOException;

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public abstract class AbstractValueReader implements Comparable<AbstractValueReader> {

    public abstract void init(AbstractBytesInputStream in, int tupleCount) throws IOException;

    public abstract void nextValue() throws HyracksDataException;

    public abstract ATypeTag getTypeTag();

    public boolean getBoolean() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public long getLong() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public float getFloat() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public double getDouble() {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public IValueReference getBytes() {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
