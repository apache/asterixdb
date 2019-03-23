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
import static org.apache.asterix.om.util.container.ObjectFactories.STORAGE_FACTORY;
import static org.apache.asterix.om.util.container.ObjectFactories.VOID_FACTORY;

import java.io.IOException;

import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.om.pointables.nonvisitor.RecordField;
import org.apache.asterix.om.pointables.nonvisitor.SortedRecord;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * This comparator is an ordering comparator. It deals with MISSING, NULL, and incompatible types different than the
 * logical comparison.
 */
abstract class AbstractAGenericBinaryComparator implements IBinaryComparator {

    private final IBinaryComparator ascBoolComp = BooleanBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascStrComp =
            new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY).createBinaryComparator();
    private final IBinaryComparator ascByteArrayComp =
            new PointableBinaryComparatorFactory(ByteArrayPointable.FACTORY).createBinaryComparator();
    private final IBinaryComparator ascRectangleComp =
            ARectanglePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascCircleComp =
            ACirclePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascDurationComp =
            ADurationPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascIntervalComp =
            AIntervalAscPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascLineComp = ALinePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascPointComp =
            APointPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascPoint3DComp =
            APoint3DPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascPolygonComp =
            APolygonPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator ascUUIDComp = AUUIDPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    private final IBinaryComparator rawComp = RawBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    // the type fields can be null
    protected final IAType leftType;
    protected final IAType rightType;
    private final ListObjectPool<IMutableValueStorage, Void> storageAllocator = new ListObjectPool<>(STORAGE_FACTORY);
    private final ListObjectPool<IPointable, Void> voidPointableAllocator = new ListObjectPool<>(VOID_FACTORY);
    private final ListObjectPool<SortedRecord, ARecordType> recordPool = new ListObjectPool<>(RECORD_FACTORY);

    AbstractAGenericBinaryComparator(IAType leftType, IAType rightType) {
        // factory should have already made sure to get the actual type
        this.leftType = leftType;
        this.rightType = rightType;
    }

    protected final int compare(IAType leftType, byte[] b1, int s1, int l1, IAType rightType, byte[] b2, int s2, int l2)
            throws HyracksDataException {
        if (b1[s1] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
            return b2[s2] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG ? 0 : -1;
        } else if (b2[s2] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
            return 1;
        }
        if (b1[s1] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            return b2[s2] == ATypeTag.SERIALIZED_NULL_TYPE_TAG ? 0 : -1;
        } else if (b2[s2] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            return 1;
        }
        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b1[s1]);
        ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b2[s2]);
        // tag being null could mean several things among of which is that the passed args are not tagged
        if (tag1 == null || tag2 == null) {
            return rawComp.compare(b1, s1, l1, b2, s2, l2);
        }
        if (ATypeHierarchy.isCompatible(tag1, tag2) && ATypeHierarchy.getTypeDomain(tag1) == Domain.NUMERIC) {
            return ComparatorUtil.compareNumbers(tag1, b1, s1 + 1, tag2, b2, s2 + 1);
        }
        // currently only numbers are compatible. if two tags are not compatible, we compare the tags to generate order
        if (tag1 != tag2) {
            return Byte.compare(b1[s1], b2[s2]);
        }

        switch (tag1) {
            case STRING:
                return ascStrComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case UUID:
                return ascUUIDComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case BOOLEAN:
                return ascBoolComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case TIME:
                return Integer.compare(ATimeSerializerDeserializer.getChronon(b1, s1 + 1),
                        ATimeSerializerDeserializer.getChronon(b2, s2 + 1));
            case DATE:
                return Integer.compare(ADateSerializerDeserializer.getChronon(b1, s1 + 1),
                        ADateSerializerDeserializer.getChronon(b2, s2 + 1));
            case YEARMONTHDURATION:
                return Integer.compare(AYearMonthDurationSerializerDeserializer.getYearMonth(b1, s1 + 1),
                        AYearMonthDurationSerializerDeserializer.getYearMonth(b2, s2 + 1));
            case DATETIME:
                return Long.compare(ADateTimeSerializerDeserializer.getChronon(b1, s1 + 1),
                        ADateTimeSerializerDeserializer.getChronon(b2, s2 + 1));
            case DAYTIMEDURATION:
                return Long.compare(ADayTimeDurationSerializerDeserializer.getDayTime(b1, s1 + 1),
                        ADayTimeDurationSerializerDeserializer.getDayTime(b2, s2 + 1));
            case RECTANGLE:
                return ascRectangleComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case CIRCLE:
                return ascCircleComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case POINT:
                return ascPointComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case POINT3D:
                return ascPoint3DComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case LINE:
                return ascLineComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case POLYGON:
                return ascPolygonComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case DURATION:
                return ascDurationComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case INTERVAL:
                return compareInterval(b1, s1, l1, b2, s2, l2);
            case BINARY:
                return ascByteArrayComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case ARRAY:
                return compareArrays(leftType, b1, s1, l1, rightType, b2, s2, l2);
            case OBJECT:
                return compareRecords(leftType, b1, s1, l1, rightType, b2, s2, l2);
            default:
                return rawComp.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    protected int compareInterval(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
        return ascIntervalComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
    }

    private int compareArrays(IAType leftType, byte[] b1, int s1, int l1, IAType rightType, byte[] b2, int s2, int l2)
            throws HyracksDataException {
        if (leftType == null || rightType == null) {
            return rawComp.compare(b1, s1, l1, b2, s2, l2);
        }
        int leftNumItems = ListAccessorUtil.numberOfItems(b1, s1);
        int rightNumItems = ListAccessorUtil.numberOfItems(b2, s2);
        IAType leftArrayType = TypeComputeUtils.getActualTypeOrOpen(leftType, ATypeTag.ARRAY);
        IAType rightArrayType = TypeComputeUtils.getActualTypeOrOpen(rightType, ATypeTag.ARRAY);
        IAType leftItemType = ((AbstractCollectionType) leftArrayType).getItemType();
        IAType rightItemType = ((AbstractCollectionType) rightArrayType).getItemType();
        ATypeTag leftItemTag = leftItemType.getTypeTag();
        ATypeTag rightItemTag = rightItemType.getTypeTag();
        // TODO(ali): could be optimized to not need pointable when changing comparator to be non-tagged & no visitable
        IPointable leftItem = voidPointableAllocator.allocate(null);
        IPointable rightItem = voidPointableAllocator.allocate(null);
        // TODO(ali): optimize to not need this storage, will require optimizing records comparison to not use visitable
        ArrayBackedValueStorage leftStorage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        ArrayBackedValueStorage rightStorage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        int result;
        try {
            for (int i = 0; i < leftNumItems && i < rightNumItems; i++) {
                ListAccessorUtil.getItem(b1, s1, i, ATypeTag.ARRAY, leftItemTag, leftItem, leftStorage);
                ListAccessorUtil.getItem(b2, s2, i, ATypeTag.ARRAY, rightItemTag, rightItem, rightStorage);
                result = compare(leftItemType, leftItem.getByteArray(), leftItem.getStartOffset(), leftItem.getLength(),
                        rightItemType, rightItem.getByteArray(), rightItem.getStartOffset(), rightItem.getLength());
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(leftNumItems, rightNumItems);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            storageAllocator.free(rightStorage);
            storageAllocator.free(leftStorage);
            voidPointableAllocator.free(rightItem);
            voidPointableAllocator.free(leftItem);
        }
    }

    private int compareRecords(IAType leftType, byte[] b1, int s1, int l1, IAType rightType, byte[] b2, int s2, int l2)
            throws HyracksDataException {
        if (leftType == null || rightType == null) {
            return rawComp.compare(b1, s1, l1, b2, s2, l2);
        }
        ARecordType leftRecordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(leftType, ATypeTag.OBJECT);
        ARecordType rightRecordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(rightType, ATypeTag.OBJECT);
        SortedRecord leftRecord = recordPool.allocate(leftRecordType);
        SortedRecord rightRecord = recordPool.allocate(rightRecordType);
        IPointable leftFieldValue = voidPointableAllocator.allocate(null);
        IPointable rightFieldValue = voidPointableAllocator.allocate(null);
        // TODO(ali): this is not ideal. should be removed when tagged pointables are introduced
        ArrayBackedValueStorage leftStorage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        ArrayBackedValueStorage rightStorage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        try {
            leftRecord.reset(b1, s1);
            rightRecord.reset(b2, s2);
            IAType leftFieldType, rightFieldType;
            RecordField leftField, rightField;
            int result;
            while (!leftRecord.isEmpty() && !rightRecord.isEmpty()) {
                leftField = leftRecord.poll();
                rightField = rightRecord.poll();
                // compare the names first
                result = RecordField.FIELD_NAME_COMP.compare(leftField, rightField);
                if (result != 0) {
                    return result;
                }
                // then compare the values if the names are equal
                leftStorage.reset();
                rightStorage.reset();
                leftRecord.getFieldValue(leftField, leftFieldValue, leftStorage);
                rightRecord.getFieldValue(rightField, rightFieldValue, rightStorage);
                leftFieldType = leftRecord.getFieldType(leftField);
                rightFieldType = rightRecord.getFieldType(rightField);
                result = compare(leftFieldType, leftFieldValue.getByteArray(), leftFieldValue.getStartOffset(),
                        leftFieldValue.getLength(), rightFieldType, rightFieldValue.getByteArray(),
                        rightFieldValue.getStartOffset(), rightFieldValue.getLength());
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
            voidPointableAllocator.free(rightFieldValue);
            voidPointableAllocator.free(leftFieldValue);
            storageAllocator.free(rightStorage);
            storageAllocator.free(leftStorage);
        }
    }
}
