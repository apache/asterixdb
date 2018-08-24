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
package org.apache.asterix.om.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmBooleanNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmNullNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class AdmNodeUtils {

    private AdmNodeUtils() {
    }

    public static Map<String, IAdmNode> getOpenFields(ARecordPointable recPointable, ARecordType recordType)
            throws IOException {
        int openFieldCount = recPointable.getOpenFieldCount(recordType);
        Map<String, IAdmNode> map = (openFieldCount == 0) ? Collections.emptyMap() : new HashMap<>();
        for (int i = 0; i < openFieldCount; i++) {
            map.put(recPointable.getOpenFieldName(recordType, i), getOpenField(recPointable, recordType, i));
        }
        return map;
    }

    public static IAdmNode getAsAdmNode(IPointable pointable) throws IOException {
        byte[] bytes = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        int len = pointable.getLength();
        if (len == 0) {
            throw new IllegalArgumentException();
        }
        byte tagByte = bytes[offset];
        ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[tagByte];
        switch (tag) {
            case ARRAY:
                AListPointable listPointable = AListPointable.FACTORY.createPointable();
                listPointable.set(bytes, offset, len);
                return getAsAdmNode(listPointable);
            case BIGINT:
                return new AdmBigIntNode(LongPointable.getLong(bytes, offset + 1));
            case BOOLEAN:
                return AdmBooleanNode.get(BooleanPointable.getBoolean(bytes, offset + 1));
            case DOUBLE:
                return new AdmDoubleNode(DoublePointable.getDouble(bytes, offset + 1));
            case NULL:
                return AdmNullNode.INSTANCE;
            case OBJECT:
                ARecordPointable recPointable = ARecordPointable.FACTORY.createPointable();
                recPointable.set(bytes, offset, len);
                try {
                    return new AdmObjectNode(getOpenFields(recPointable, RecordUtil.FULLY_OPEN_RECORD_TYPE));
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            case STRING:
                UTF8StringPointable str = UTF8StringPointable.FACTORY.createPointable();
                str.set(bytes, offset + 1, len - 1);
                return new AdmStringNode(str.toString());
            default:
                throw new UnsupportedOperationException("Unsupported item type: " + tag);
        }
    }

    private static IAdmNode getOpenField(ARecordPointable recPointable, ARecordType type, int i) throws IOException {
        byte tagByte = recPointable.getOpenFieldTag(type, i);
        ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[tagByte];
        switch (tag) {
            case ARRAY:
                return getOpenFieldAsArray(recPointable, type, i);
            case BIGINT:
                return new AdmBigIntNode(LongPointable.getLong(recPointable.getByteArray(),
                        recPointable.getOpenFieldValueOffset(type, i) + 1));
            case BOOLEAN:
                return AdmBooleanNode.get(BooleanPointable.getBoolean(recPointable.getByteArray(),
                        recPointable.getOpenFieldValueOffset(type, i) + 1));
            case DOUBLE:
                return new AdmDoubleNode(DoublePointable.getDouble(recPointable.getByteArray(),
                        recPointable.getOpenFieldValueOffset(type, i) + 1));
            case NULL:
                return AdmNullNode.INSTANCE;
            case OBJECT:
                return getOpenFieldAsObject(recPointable, type, i);
            case STRING:
                UTF8StringPointable str = UTF8StringPointable.FACTORY.createPointable();
                str.set(recPointable.getByteArray(), recPointable.getOpenFieldValueOffset(type, i) + 1,
                        recPointable.getOpenFieldValueSize(type, i) - 1);
                return new AdmStringNode(str.toString());
            default:
                throw new UnsupportedOperationException("Unsupported item type: " + tag);
        }
    }

    private static AdmObjectNode getOpenFieldAsObject(ARecordPointable recPointable, ARecordType type, int i)
            throws IOException {
        ARecordPointable pointable = ARecordPointable.FACTORY.createPointable();
        int offset = recPointable.getOpenFieldValueOffset(type, i);
        int len = recPointable.getOpenFieldValueSize(type, i);
        pointable.set(recPointable.getByteArray(), offset, len);
        return new AdmObjectNode(getOpenFields(pointable, RecordUtil.FULLY_OPEN_RECORD_TYPE));
    }

    private static AdmArrayNode getOpenFieldAsArray(ARecordPointable recPointable, ARecordType type, int i)
            throws IOException {
        AListPointable pointable = AListPointable.FACTORY.createPointable();
        int offset = recPointable.getOpenFieldValueOffset(type, i);
        int len = recPointable.getOpenFieldValueSize(type, i);
        pointable.set(recPointable.getByteArray(), offset, len);
        return getAsAdmNode(pointable);
    }

    public static AdmArrayNode getAsAdmNode(AListPointable listPointable) throws IOException {
        int count = listPointable.getItemCount();
        AdmArrayNode node = new AdmArrayNode(count);
        for (int i = 0; i < count; i++) {
            byte tagByte = listPointable.getItemTag(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i);
            ATypeTag itemTag = ATypeTag.VALUE_TYPE_MAPPING[tagByte];
            switch (itemTag) {
                case ARRAY:
                    node.add(getOpenFieldAsArray(listPointable, i));
                    break;
                case BIGINT:
                    node.add(new AdmBigIntNode(LongPointable.getLong(listPointable.getByteArray(),
                            listPointable.getItemOffset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i) + 1)));
                    break;
                case BOOLEAN:
                    node.add(AdmBooleanNode.get(BooleanPointable.getBoolean(listPointable.getByteArray(),
                            listPointable.getItemOffset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i) + 1)));
                    break;
                case DOUBLE:
                    node.add(new AdmDoubleNode(DoublePointable.getDouble(listPointable.getByteArray(),
                            listPointable.getItemOffset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i) + 1)));
                    break;
                case NULL:
                    node.add(AdmNullNode.INSTANCE);
                    break;
                case OBJECT:
                    node.add(getOpenFieldAsObject(listPointable, i));
                    break;
                case STRING:
                    UTF8StringPointable str = UTF8StringPointable.FACTORY.createPointable();
                    str.set(listPointable.getByteArray(),
                            listPointable.getItemOffset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i) + 1,
                            listPointable.getItemSize(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i));
                    node.add(new AdmStringNode(str.toString()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported item type: " + itemTag);
            }
        }
        return node;
    }

    private static IAdmNode getOpenFieldAsObject(AListPointable listPointable, int i) throws IOException {
        ARecordPointable pointable = ARecordPointable.FACTORY.createPointable();
        int offset = listPointable.getItemOffset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i);
        int len = listPointable.getItemSize(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i);
        pointable.set(listPointable.getByteArray(), offset, len);
        return new AdmObjectNode(AdmNodeUtils.getOpenFields(pointable, RecordUtil.FULLY_OPEN_RECORD_TYPE));
    }

    private static AdmArrayNode getOpenFieldAsArray(AListPointable listPointable, int i) throws IOException {
        AListPointable pointable = AListPointable.FACTORY.createPointable();
        int offset = listPointable.getItemOffset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i);
        int len = listPointable.getItemSize(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, i);
        pointable.set(listPointable.getByteArray(), offset, len);
        return getAsAdmNode(pointable);
    }
}
