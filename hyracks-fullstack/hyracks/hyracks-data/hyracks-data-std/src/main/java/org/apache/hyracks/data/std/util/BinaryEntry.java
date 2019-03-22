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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A class that stores a meta-data (buf, offset, length) of the entry for BinaryHashMap and BinaryHashSet.
 */
public class BinaryEntry {
    private int off;
    private int len;
    private byte[] buf;

    public void set(int offset, int length) {
        this.buf = null;
        this.off = offset;
        this.len = length;
    }

    public void set(byte[] buf, int off, int len) {
        this.buf = buf;
        this.off = off;
        this.len = len;
    }

    public void setOffset(int off) {
        this.off = off;
    }

    public int getOffset() {
        return off;
    }

    public void setLength(int len) {
        this.len = len;
    }

    public int getLength() {
        return len;
    }

    public void setBuf(byte[] buf) {
        this.buf = buf;
    }

    public byte[] getBuf() {
        return buf;
    }

    // Inefficient. Just for debugging.
    @SuppressWarnings("rawtypes")
    public String print(ISerializerDeserializer serde) throws HyracksDataException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(buf, off, len);
        DataInput dataIn = new DataInputStream(inStream);
        return serde.deserialize(dataIn).toString();
    }
}
