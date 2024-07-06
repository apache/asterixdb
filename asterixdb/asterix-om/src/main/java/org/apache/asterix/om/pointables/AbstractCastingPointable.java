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

package org.apache.asterix.om.pointables;

import static org.apache.asterix.om.pointables.AFlatValueCastingPointable.missingPointable;
import static org.apache.asterix.om.pointables.AFlatValueCastingPointable.nullPointable;

import org.apache.asterix.om.pointables.base.ICastingPointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public abstract class AbstractCastingPointable implements ICastingPointable {

    private final IObjectPool<AFlatValueCastingPointable, IAType> flatValuePool =
            new ListObjectPool<>(AFlatValueCastingPointable.FACTORY);
    private final IObjectPool<ARecordCastingPointable, IAType> recordValuePool =
            new ListObjectPool<>(ARecordCastingPointable.FACTORY);
    private final IObjectPool<AListCastingPointable, IAType> listValuePool =
            new ListObjectPool<>(AListCastingPointable.FACTORY);

    protected byte[] data;
    protected int start = -1;
    protected int len = -1;
    private boolean tagged;
    private ATypeTag tag;

    @Override
    public byte[] getByteArray() {
        return data;
    }

    @Override
    public int getLength() {
        return len;
    }

    @Override
    public int getStartOffset() {
        return start;
    }

    @Override
    public final void set(IValueReference ivf) {
        set(ivf.getByteArray(), ivf.getStartOffset(), ivf.getLength());
    }

    @Override
    public final void set(byte[] bytes, int start, int length) {
        setInfo(bytes, start, length, EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[start]), true);
    }

    @Override
    public final void setUntagged(byte[] bytes, int start, int length, ATypeTag tag) {
        setInfo(bytes, start, length, tag, false);
    }

    private void setInfo(byte[] bytes, int start, int length, ATypeTag tag, boolean tagged) {
        this.data = bytes;
        this.start = start;
        this.len = length;
        this.tag = tag;
        this.tagged = tagged;
        prepare(bytes, start);
    }

    @Override
    public final ATypeTag getTag() {
        return tag;
    }

    @Override
    public boolean isTagged() {
        return tagged;
    }

    protected void prepare(byte[] bytes, int start) {

    }

    protected ICastingPointable allocate(byte[] data, int valueStart, int valueLen, IAType valueType, ATypeTag valueTag,
            boolean openValue) throws HyracksDataException {
        if (valueTag == ATypeTag.NULL) {
            return nullPointable;
        }
        if (valueTag == ATypeTag.MISSING) {
            return missingPointable;
        }
        ICastingPointable pointable;
        switch (valueTag) {
            case OBJECT:
                pointable = recordValuePool.allocate(valueType);
                break;
            case ARRAY:
            case MULTISET:
                if (openValue) {
                    //TODO(ali): a bit weird where a closed (non-tagged) list has been written in open section
                    AbstractCollectionType listType;
                    int itemTagPosition = valueStart + 1;
                    if (valueTag == ATypeTag.ARRAY) {
                        listType = new AOrderedListType(getItemType(data, itemTagPosition), "");
                    } else {
                        listType = new AUnorderedListType(getItemType(data, itemTagPosition), "");
                    }
                    pointable = listValuePool.allocate(listType);
                } else {
                    pointable = listValuePool.allocate(valueType);
                }
                break;
            default:
                pointable = flatValuePool.allocate(null);
        }
        if (openValue) {
            pointable.set(data, valueStart, valueLen);
        } else {
            pointable.setUntagged(data, valueStart, valueLen, valueTag);
        }
        return pointable;
    }

    private IAType getItemType(byte[] data, int itemTagPosition) throws HyracksDataException {
        ATypeTag itemTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[itemTagPosition]);
        if (itemTag == ATypeTag.ARRAY) {
            return new AOrderedListType(getItemType(data, itemTagPosition + 1), "");
        } else if (itemTag == ATypeTag.MULTISET) {
            return new AUnorderedListType(getItemType(data, itemTagPosition + 1), "");
        } else {
            return TypeTagUtil.getBuiltinTypeByTag(itemTag);
        }
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\", \"data\" : "
                + (data == null ? "null" : ("\"" + System.identityHashCode(data) + ":" + data.length + "\""))
                + ", \"offset\" : " + start + ", \"length\" : " + len + ", \"tagged\" : " + tagged + ", \"tag\" : \""
                + tag + "\" }";
    }

    protected void reset() {
        flatValuePool.reset();
        recordValuePool.reset();
        listValuePool.reset();
    }

    protected void free(ICastingPointable castingPointable) {
        if (castingPointable == missingPointable || castingPointable == nullPointable) {
            return;
        }
        switch (castingPointable.getType()) {
            case FLAT:
                flatValuePool.free((AFlatValueCastingPointable) castingPointable);
                break;
            case LIST:
                listValuePool.free((AListCastingPointable) castingPointable);
                break;
            case RECORD:
                recordValuePool.free((ARecordCastingPointable) castingPointable);
                break;
        }
    }
}
