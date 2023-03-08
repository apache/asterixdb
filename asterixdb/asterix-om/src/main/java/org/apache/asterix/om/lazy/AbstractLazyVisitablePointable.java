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
package org.apache.asterix.om.lazy;

import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * Similar to {@link IVisitablePointable}. The difference is, upon calling {@link #set(byte[], int, int)}, the nested
 * values (children) will not call {@link #set(byte[], int, int)} recursively. Instead, it will wait until the
 * child is accessed. Thus, when a processor (a class that implements {@link ILazyVisitablePointableVisitor}) wants
 * to traverse the object in a <b>DFS mode</b>, the traversal will be done in a single pass - compared to the two passes
 * when using the {@link IVisitablePointable}, where one pass is done when calling
 * {@link IVisitablePointable#set(byte[], int, int)} and another pass is done by the processor (e.g., the result
 * printer). Also, the lazy visitable-pointable requires less memory as we do not allocate any temporary buffers.
 */
public abstract class AbstractLazyVisitablePointable implements IPointable {
    private final boolean tagged;
    private byte[] data;
    private int offset;
    private int length;

    AbstractLazyVisitablePointable(boolean tagged) {
        this.tagged = tagged;
    }

    @Override
    public final void set(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        init(data, offset, length);
    }

    @Override
    public final void set(IValueReference pointer) {
        set(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public final byte[] getByteArray() {
        return data;
    }

    @Override
    public final int getStartOffset() {
        return offset;
    }

    @Override
    public final int getLength() {
        return length;
    }

    /**
     * @return The serialized type tag
     */
    public abstract byte getSerializedTypeTag();

    /**
     * @return The type tag
     */
    public abstract ATypeTag getTypeTag();

    public abstract <R, T> R accept(ILazyVisitablePointableVisitor<R, T> visitor, T arg) throws HyracksDataException;

    /**
     * @return true if the value contains tag, false otherwise
     */
    public final boolean isTagged() {
        return tagged;
    }

    /**
     * Called by {@link #set(byte[], int, int)} to initialize the visitable-pointable
     *
     * @param data   value's data
     * @param offset value's start offset
     * @param length value's length
     */
    abstract void init(byte[] data, int offset, int length);
}
