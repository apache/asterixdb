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
package org.apache.asterix.dataflow.data.nontagged.comparators;

import static org.apache.asterix.om.util.container.ObjectFactories.RECORD_FACTORY;
import static org.apache.asterix.om.util.container.ObjectFactories.VALUE_FACTORY;

import java.io.IOException;

import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.pointables.nonvisitor.RecordField;
import org.apache.asterix.om.pointables.nonvisitor.SortedRecord;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/**
 * This comparator is an ordering comparator. It deals with MISSING, NULL, and incompatible types different than the
 * logical comparison.
 */
abstract class AbstractAGenericBinaryComparator implements IBinaryComparator {

    protected final IAType leftType;
    protected final IAType rightType;
    private final ListObjectPool<SortedRecord, ARecordType> recordPool = new ListObjectPool<>(RECORD_FACTORY);
    private final ListObjectPool<TaggedValueReference, Void> taggedValueAllocator = new ListObjectPool<>(VALUE_FACTORY);

    AbstractAGenericBinaryComparator(IAType leftType, IAType rightType) {
        // factory should have already made sure to get the actual type (and no null types)
        this.leftType = leftType;
        this.rightType = rightType;
    }

    protected final int compare(IAType leftType, TaggedValueReference leftValue, IAType rightType,
            TaggedValueReference rightValue) throws HyracksDataException {
        ATypeTag tag1 = leftValue.getTag();
        ATypeTag tag2 = rightValue.getTag();
        if (tag1 == null || tag2 == null) {
            throw new IllegalStateException("Could not recognize the type of data.");
        }
        if (tag1 == ATypeTag.MISSING) {
            return tag2 == ATypeTag.MISSING ? 0 : -1;
        } else if (tag2 == ATypeTag.MISSING) {
            return 1;
        }
        if (tag1 == ATypeTag.NULL) {
            return tag2 == ATypeTag.NULL ? 0 : -1;
        } else if (tag2 == ATypeTag.NULL) {
            return 1;
        }
        byte[] b1 = leftValue.getByteArray();
        int s1 = leftValue.getStartOffset();
        int l1 = leftValue.getLength();
        byte[] b2 = rightValue.getByteArray();
        int s2 = rightValue.getStartOffset();
        int l2 = rightValue.getLength();

        if (ATypeHierarchy.isCompatible(tag1, tag2) && ATypeHierarchy.getTypeDomain(tag1) == Domain.NUMERIC) {
            return ComparatorUtil.compareNumbers(tag1, b1, s1, tag2, b2, s2);
        }
        // currently only numbers are compatible. if two tags are not compatible, we compare the tags to generate order
        if (tag1 != tag2) {
            return Byte.compare(tag1.serialize(), tag2.serialize());
        }

        switch (tag1) {
            case STRING:
                return UTF8StringPointable.compare(b1, s1, l1, b2, s2, l2);
            case UUID:
                return AUUIDPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case BOOLEAN:
                return BooleanPointable.compare(b1, s1, l1, b2, s2, l2);
            case TIME:
                return Integer.compare(ATimeSerializerDeserializer.getChronon(b1, s1),
                        ATimeSerializerDeserializer.getChronon(b2, s2));
            case DATE:
                return Integer.compare(ADateSerializerDeserializer.getChronon(b1, s1),
                        ADateSerializerDeserializer.getChronon(b2, s2));
            case YEARMONTHDURATION:
                return Integer.compare(AYearMonthDurationSerializerDeserializer.getYearMonth(b1, s1),
                        AYearMonthDurationSerializerDeserializer.getYearMonth(b2, s2));
            case DATETIME:
                return Long.compare(ADateTimeSerializerDeserializer.getChronon(b1, s1),
                        ADateTimeSerializerDeserializer.getChronon(b2, s2));
            case DAYTIMEDURATION:
                return Long.compare(ADayTimeDurationSerializerDeserializer.getDayTime(b1, s1),
                        ADayTimeDurationSerializerDeserializer.getDayTime(b2, s2));
            case RECTANGLE:
                return ARectanglePartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case CIRCLE:
                return ACirclePartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case POINT:
                return APointPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case POINT3D:
                return APoint3DPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case LINE:
                return ALinePartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case POLYGON:
                return APolygonPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case DURATION:
                return ADurationPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
            case INTERVAL:
                return compareInterval(b1, s1, l1, b2, s2, l2);
            case BINARY:
                return ByteArrayPointable.compare(b1, s1, l1, b2, s2, l2);
            case ARRAY:
                return compareArrays(leftType, leftValue, rightType, rightValue);
            case OBJECT:
                return compareRecords(leftType, b1, s1, rightType, b2, s2);
            default:
                return RawBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    protected int compareInterval(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return AIntervalAscPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
    }

    private int compareArrays(IAType leftArrayType, TaggedValueReference leftArray, IAType rightArrayType,
            TaggedValueReference rightArray) throws HyracksDataException {
        int leftNumItems = SerializerDeserializerUtil.getNumberOfItemsNonTagged(leftArray);
        int rightNumItems = SerializerDeserializerUtil.getNumberOfItemsNonTagged(rightArray);
        IAType leftItemType =
                ((AbstractCollectionType) TypeComputeUtils.getActualTypeOrOpen(leftArrayType, ATypeTag.ARRAY))
                        .getItemType();
        IAType rightItemType =
                ((AbstractCollectionType) TypeComputeUtils.getActualTypeOrOpen(rightArrayType, ATypeTag.ARRAY))
                        .getItemType();
        ATypeTag leftArrayItemTag = leftItemType.getTypeTag();
        ATypeTag rightArrayItemTag = rightItemType.getTypeTag();
        TaggedValueReference leftItem = taggedValueAllocator.allocate(null);
        TaggedValueReference rightItem = taggedValueAllocator.allocate(null);
        boolean leftItemHasTag = leftArrayItemTag == ATypeTag.ANY;
        boolean rightItemHasTag = rightArrayItemTag == ATypeTag.ANY;
        int result;
        try {
            for (int i = 0; i < leftNumItems && i < rightNumItems; i++) {
                ListAccessorUtil.getItemFromList(leftArray, i, leftItem, leftArrayItemTag, leftItemHasTag);
                ListAccessorUtil.getItemFromList(rightArray, i, rightItem, rightArrayItemTag, rightItemHasTag);
                result = compare(leftItemType, leftItem, rightItemType, rightItem);
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(leftNumItems, rightNumItems);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            taggedValueAllocator.free(rightItem);
            taggedValueAllocator.free(leftItem);
        }
    }

    private int compareRecords(IAType leftType, byte[] b1, int s1, IAType rightType, byte[] b2, int s2)
            throws HyracksDataException {
        ARecordType leftRecordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(leftType, ATypeTag.OBJECT);
        ARecordType rightRecordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(rightType, ATypeTag.OBJECT);
        SortedRecord leftRecord = recordPool.allocate(leftRecordType);
        SortedRecord rightRecord = recordPool.allocate(rightRecordType);
        TaggedValueReference leftFieldValue = taggedValueAllocator.allocate(null);
        TaggedValueReference rightFieldValue = taggedValueAllocator.allocate(null);
        try {
            leftRecord.resetNonTagged(b1, s1);
            rightRecord.resetNonTagged(b2, s2);
            int result;
            while (!leftRecord.isEmpty() && !rightRecord.isEmpty()) {
                RecordField leftField = leftRecord.poll();
                RecordField rightField = rightRecord.poll();
                // compare the names first
                result = RecordField.FIELD_NAME_COMP.compare(leftField, rightField);
                if (result != 0) {
                    return result;
                }
                // then compare the values if the names are equal
                leftRecord.getFieldValue(leftField, leftFieldValue);
                rightRecord.getFieldValue(rightField, rightFieldValue);
                result = compare(leftRecord.getFieldType(leftField), leftFieldValue,
                        rightRecord.getFieldType(rightField), rightFieldValue);
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(leftRecord.size(), rightRecord.size());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            recordPool.free(rightRecord);
            recordPool.free(leftRecord);
            taggedValueAllocator.free(rightFieldValue);
            taggedValueAllocator.free(leftFieldValue);
        }
    }
}
