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

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.dataflow.data.nontagged.CompareHashUtil;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.util.container.ObjectFactories;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

abstract class AbstractAGenericBinaryComparator implements IBinaryComparator {

    // BOOLEAN
    private final IBinaryComparator ascBoolComp = BooleanBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // TINYINT
    private final IBinaryComparator ascByteComp =
            new PointableBinaryComparatorFactory(BytePointable.FACTORY).createBinaryComparator();
    // SMALLINT
    private final IBinaryComparator ascShortComp =
            new PointableBinaryComparatorFactory(ShortPointable.FACTORY).createBinaryComparator();
    // INTEGER
    private final IBinaryComparator ascIntComp =
            new PointableBinaryComparatorFactory(IntegerPointable.FACTORY).createBinaryComparator();
    // BIGINT
    private final IBinaryComparator ascLongComp = LongBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // STRING
    private final IBinaryComparator ascStrComp =
            new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY).createBinaryComparator();
    // BINARY
    private final IBinaryComparator ascByteArrayComp =
            new PointableBinaryComparatorFactory(ByteArrayPointable.FACTORY).createBinaryComparator();
    // FLOAT
    private final IBinaryComparator ascFloatComp =
            new PointableBinaryComparatorFactory(FloatPointable.FACTORY).createBinaryComparator();
    // DOUBLE
    private final IBinaryComparator ascDoubleComp =
            new PointableBinaryComparatorFactory(DoublePointable.FACTORY).createBinaryComparator();
    // RECTANGLE
    private final IBinaryComparator ascRectangleComp =
            ARectanglePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // CIRCLE
    private final IBinaryComparator ascCircleComp =
            ACirclePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // DURATION
    private final IBinaryComparator ascDurationComp =
            ADurationPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // INTERVAL
    private final IBinaryComparator ascIntervalComp =
            AIntervalAscPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // LINE
    private final IBinaryComparator ascLineComp = ALinePartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // POINT
    private final IBinaryComparator ascPointComp =
            APointPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // POINT3D
    private final IBinaryComparator ascPoint3DComp =
            APoint3DPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // POLYGON
    private final IBinaryComparator ascPolygonComp =
            APolygonPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // UUID
    private final IBinaryComparator ascUUIDComp = AUUIDPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    // RAW
    private final IBinaryComparator rawComp = RawBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    // these fields can be null
    protected final IAType leftType;
    protected final IAType rightType;
    // a storage to promote a value
    private final ArrayBackedValueStorage castBuffer;
    private final IObjectPool<IMutableValueStorage, Void> storageAllocator;
    private final IObjectPool<IPointable, Void> voidPointableAllocator;
    // used for record comparison, sorting field names
    private final PointableAllocator recordAllocator;
    private final IObjectPool<PriorityQueue<IVisitablePointable>, Void> heapAllocator;
    private final Comparator<IVisitablePointable> fieldNamesComparator;

    AbstractAGenericBinaryComparator(IAType leftType, IAType rightType) {
        // factory should have already made sure to get the actual type
        this.leftType = leftType;
        this.rightType = rightType;
        this.castBuffer = new ArrayBackedValueStorage();
        this.storageAllocator = new ListObjectPool<>(ObjectFactories.STORAGE_FACTORY);
        this.voidPointableAllocator = new ListObjectPool<>(ObjectFactories.VOID_FACTORY);
        this.recordAllocator = new PointableAllocator();
        this.fieldNamesComparator = CompareHashUtil.createFieldNamesComp(ascStrComp);
        this.heapAllocator = new ListObjectPool<>((type) -> new PriorityQueue<>(fieldNamesComparator));
    }

    protected int compare(IAType leftType, byte[] b1, int s1, int l1, IAType rightType, byte[] b2, int s2, int l2)
            throws HyracksDataException {
        // normally, comparing between MISSING and non-MISSING values should return MISSING as the result.
        // however, this comparator is used by order-by/group-by/distinct-by.
        // therefore, inside this method, we return an order between two values even if one value is MISSING.
        if (b1[s1] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
            return b2[s2] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG ? 0 : -1;
        } else {
            if (b2[s2] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                return 1;
            }
        }

        // normally, comparing between NULL and non-NULL/MISSING values should return NULL as the result.
        // however, this comparator is used by order-by/group-by/distinct-by.
        // therefore, inside this method, we return an order between two values even if one value is NULL.
        if (b1[s1] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            return b2[s2] == ATypeTag.SERIALIZED_NULL_TYPE_TAG ? 0 : -1;
        } else {
            if (b2[s2] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                return 1;
            }
        }

        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b1[s1]);
        ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b2[s2]);

        // if one of tag is null, that means we are dealing with an empty byte array in one side.
        // and, we don't need to continue. We just compare raw byte by byte.
        if (tag1 == null || tag2 == null) {
            return rawComp.compare(b1, s1, l1, b2, s2, l2);
        }

        // if two type does not match, we identify the source and the target and
        // promote the source to the target type if they are compatible.
        ATypeTag sourceTypeTag = null;
        ATypeTag targetTypeTag = null;
        boolean areTwoTagsEqual = false;
        boolean typePromotionApplied = false;
        boolean leftValueChanged = false;

        if (tag1 != tag2) {
            // tag1 can be promoted to tag2 (e.g. tag1: SMALLINT, tag2: INTEGER)
            if (ATypeHierarchy.canPromote(tag1, tag2)) {
                sourceTypeTag = tag1;
                targetTypeTag = tag2;
                typePromotionApplied = true;
                leftValueChanged = true;
                // or tag2 can be promoted to tag1 (e.g. tag2: INTEGER, tag1: DOUBLE)
            } else if (ATypeHierarchy.canPromote(tag2, tag1)) {
                sourceTypeTag = tag2;
                targetTypeTag = tag1;
                typePromotionApplied = true;
            }

            // we promote the source to the target by using a promoteComputer
            if (typePromotionApplied) {
                castBuffer.reset();
                ITypeConvertComputer promoter = ATypeHierarchy.getTypePromoteComputer(sourceTypeTag, targetTypeTag);
                if (promoter != null) {
                    try {
                        if (leftValueChanged) {
                            // left side is the source
                            promoter.convertType(b1, s1 + 1, l1 - 1, castBuffer.getDataOutput());
                        } else {
                            // right side is the source
                            promoter.convertType(b2, s2 + 1, l2 - 1, castBuffer.getDataOutput());
                        }
                    } catch (IOException e) {
                        throw new HyracksDataException("ComparatorFactory - failed to promote the type:" + sourceTypeTag
                                + " to the type:" + targetTypeTag);
                    }
                } else {
                    // No appropriate typePromoteComputer.
                    throw new HyracksDataException("No appropriate typePromoteComputer exists for " + sourceTypeTag
                            + " to the " + targetTypeTag + " type. Please check the code.");
                }
            }
        } else {
            // tag1 == tag2.
            sourceTypeTag = tag1;
            targetTypeTag = tag1;
            areTwoTagsEqual = true;
        }

        // if two tags are not compatible, then we compare raw byte by byte, including the type tag.
        // this is especially useful when we need to generate some order between any two types.
        if ((!areTwoTagsEqual && !typePromotionApplied)) {
            return rawComp.compare(b1, s1, l1, b2, s2, l2);
        }

        // conduct actual compare()
        switch (targetTypeTag) {
            case UUID:
                return ascUUIDComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case BOOLEAN:
                return ascBoolComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case TINYINT:
                // No type promotion from another type to the TINYINT can happen
                return ascByteComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
            case SMALLINT: {
                if (!typePromotionApplied) {
                    // No type promotion case
                    return ascShortComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                } else if (leftValueChanged) {
                    // Type promotion happened. Left side was the source
                    return ascShortComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                            castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                } else {
                    // Type promotion happened. Right side was the source
                    return ascShortComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                            castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                }
            }
            case TIME:
            case DATE:
            case YEARMONTHDURATION:
            case INTEGER: {
                if (!typePromotionApplied) {
                    // No type promotion case
                    return ascIntComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                } else if (leftValueChanged) {
                    // Type promotion happened. Left side was the source
                    return ascIntComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                            castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                } else {
                    // Type promotion happened. Right side was the source
                    return ascIntComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                            castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                }
            }
            case DATETIME:
            case DAYTIMEDURATION:
            case BIGINT: {
                if (!typePromotionApplied) {
                    // No type promotion case
                    return ascLongComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                } else if (leftValueChanged) {
                    // Type promotion happened. Left side was the source
                    return ascLongComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                            castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                } else {
                    // Type promotion happened. Right side was the source
                    return ascLongComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                            castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                }
            }
            case FLOAT: {
                if (!typePromotionApplied) {
                    // No type promotion case
                    return ascFloatComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                } else if (leftValueChanged) {
                    // Type promotion happened. Left side was the source
                    return ascFloatComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                            castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                } else {
                    // Type promotion happened. Right side was the source
                    return ascFloatComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                            castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                }
            }
            case DOUBLE: {
                if (!typePromotionApplied) {
                    // No type promotion case
                    return ascDoubleComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                } else if (leftValueChanged) {
                    // Type promotion happened. Left side was the source
                    return ascDoubleComp.compare(castBuffer.getByteArray(), castBuffer.getStartOffset() + 1,
                            castBuffer.getLength() - 1, b2, s2 + 1, l2 - 1);
                } else {
                    // Type promotion happened. Right side was the source
                    return ascDoubleComp.compare(b1, s1 + 1, l1 - 1, castBuffer.getByteArray(),
                            castBuffer.getStartOffset() + 1, castBuffer.getLength() - 1);
                }
            }
            case STRING:
                return ascStrComp.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
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
                // we include typeTag in comparison to compare between two type to enforce some ordering
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
        ARecordVisitablePointable leftRecord = recordAllocator.allocateRecordValue(leftRecordType);
        ARecordVisitablePointable rightRecord = recordAllocator.allocateRecordValue(rightRecordType);
        PriorityQueue<IVisitablePointable> leftNamesHeap = null, rightNamesHeap = null;
        try {
            leftRecord.set(b1, s1, l1);
            rightRecord.set(b2, s2, l2);
            List<IVisitablePointable> leftFieldsNames = leftRecord.getFieldNames();
            List<IVisitablePointable> rightFieldsNames = rightRecord.getFieldNames();
            List<IVisitablePointable> leftFieldsValues = leftRecord.getFieldValues();
            List<IVisitablePointable> rightFieldsValues = rightRecord.getFieldValues();
            leftNamesHeap = heapAllocator.allocate(null);
            rightNamesHeap = heapAllocator.allocate(null);
            leftNamesHeap.clear();
            rightNamesHeap.clear();
            int numLeftValuedFields = CompareHashUtil.addToHeap(leftFieldsNames, leftFieldsValues, leftNamesHeap);
            int numRightValuedFields = CompareHashUtil.addToHeap(rightFieldsNames, rightFieldsValues, rightNamesHeap);
            if (numLeftValuedFields == 0 && numRightValuedFields == 0) {
                return 0;
            } else if (numLeftValuedFields == 0) {
                return -1;
            } else if (numRightValuedFields == 0) {
                return 1;
            }
            int result;
            int leftFieldIdx, rightFieldIdx;
            IAType leftFieldType, rightFieldType;
            IVisitablePointable leftFieldName, leftFieldValue, rightFieldName, rightFieldValue;
            ATypeTag fieldTag;
            while (!leftNamesHeap.isEmpty() && !rightNamesHeap.isEmpty()) {
                leftFieldName = leftNamesHeap.poll();
                rightFieldName = rightNamesHeap.poll();
                // compare the names first
                result = ascStrComp.compare(leftFieldName.getByteArray(), leftFieldName.getStartOffset() + 1,
                        leftFieldName.getLength() - 1, rightFieldName.getByteArray(),
                        rightFieldName.getStartOffset() + 1, rightFieldName.getLength() - 1);
                if (result != 0) {
                    return result;
                }
                // then compare the values if the names are equal
                leftFieldIdx = CompareHashUtil.getIndex(leftFieldsNames, leftFieldName);
                rightFieldIdx = CompareHashUtil.getIndex(rightFieldsNames, rightFieldName);
                leftFieldValue = leftFieldsValues.get(leftFieldIdx);
                rightFieldValue = rightFieldsValues.get(rightFieldIdx);
                fieldTag = VALUE_TYPE_MAPPING[leftFieldValue.getByteArray()[leftFieldValue.getStartOffset()]];
                leftFieldType = CompareHashUtil.getType(leftRecordType, leftFieldIdx, fieldTag);
                fieldTag = VALUE_TYPE_MAPPING[rightFieldValue.getByteArray()[rightFieldValue.getStartOffset()]];
                rightFieldType = CompareHashUtil.getType(rightRecordType, rightFieldIdx, fieldTag);
                result = compare(leftFieldType, leftFieldValue.getByteArray(), leftFieldValue.getStartOffset(),
                        leftFieldValue.getLength(), rightFieldType, rightFieldValue.getByteArray(),
                        rightFieldValue.getStartOffset(), rightFieldValue.getLength());
                if (result != 0) {
                    return result;
                }
            }

            return Integer.compare(numLeftValuedFields, numRightValuedFields);
        } finally {
            recordAllocator.freeRecord(rightRecord);
            recordAllocator.freeRecord(leftRecord);
            if (rightNamesHeap != null) {
                heapAllocator.free(rightNamesHeap);
            }
            if (leftNamesHeap != null) {
                heapAllocator.free(leftNamesHeap);
            }
        }
    }
}
