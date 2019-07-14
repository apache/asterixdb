/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.dataflow.data.common;

import java.util.Objects;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;

public final class TaggedValueReference implements IValueReference {

    private byte[] bytes;
    private int valueStart;
    private int valueLength;
    private ATypeTag tag;

    public void set(byte[] bytes, int valueStart, int valueLength, ATypeTag tag) {
        Objects.requireNonNull(tag);
        this.bytes = bytes;
        this.valueStart = valueStart;
        this.valueLength = valueLength;
        this.tag = tag;
    }

    @Override
    public byte[] getByteArray() {
        return bytes;
    }

    @Override
    public int getStartOffset() {
        return valueStart;
    }

    @Override
    public int getLength() {
        return valueLength;
    }

    public ATypeTag getTag() {
        return tag;
    }
}
