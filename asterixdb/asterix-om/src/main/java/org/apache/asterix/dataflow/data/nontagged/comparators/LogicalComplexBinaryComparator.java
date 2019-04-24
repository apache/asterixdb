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

import static org.apache.asterix.om.types.ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;
import static org.apache.asterix.om.util.container.ObjectFactories.BIT_SET_FACTORY;
import static org.apache.asterix.om.util.container.ObjectFactories.STORAGE_FACTORY;
import static org.apache.asterix.om.util.container.ObjectFactories.VOID_FACTORY;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class LogicalComplexBinaryComparator implements ILogicalBinaryComparator {

    private final IAType leftType;
    private final IAType rightType;
    private final boolean isEquality;
    private final LogicalScalarBinaryComparator scalarComparator;
    private final IObjectPool<IMutableValueStorage, Void> storageAllocator;
    private final IObjectPool<IPointable, Void> voidPointableAllocator;
    private final IObjectPool<BitSet, Void> bitSetAllocator;
    private final PointableAllocator pointableAllocator;

    LogicalComplexBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.isEquality = isEquality;
        this.scalarComparator = new LogicalScalarBinaryComparator(isEquality);
        storageAllocator = new ListObjectPool<>(STORAGE_FACTORY);
        voidPointableAllocator = new ListObjectPool<>(VOID_FACTORY);
        bitSetAllocator = new ListObjectPool<>(BIT_SET_FACTORY);
        pointableAllocator = new PointableAllocator();
    }

    @Override
    public Result compare(IPointable left, IPointable right) throws HyracksDataException {
        ATypeTag leftRuntimeTag = VALUE_TYPE_MAPPING[left.getByteArray()[left.getStartOffset()]];
        ATypeTag rightRuntimeTag = VALUE_TYPE_MAPPING[right.getByteArray()[right.getStartOffset()]];
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftRuntimeTag, rightRuntimeTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // make sure both left and right are complex types
        if (!leftRuntimeTag.isDerivedType() || !rightRuntimeTag.isDerivedType()) {
            throw new IllegalStateException("Input data is not complex type");
        }
        return compareComplex(leftType, leftRuntimeTag, left, rightType, rightRuntimeTag, right);
    }

    @Override
    public Result compare(IPointable left, IAObject rightConstant) {
        // TODO(ali): not defined currently for constant complex types
        ATypeTag leftTag = VALUE_TYPE_MAPPING[left.getByteArray()[left.getStartOffset()]];
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // TODO(ali): deallocate when implemented
        return Result.NULL;
    }

    @Override
    public Result compare(IAObject leftConstant, IPointable right) {
        // TODO(ali): not defined currently for constant complex types
        Result result = compare(right, leftConstant);
        switch (result) {
            case LT:
                return Result.GT;
            case GT:
                return Result.LT;
            default:
                return result;
        }
    }

    @Override
    public Result compare(IAObject leftConstant, IAObject rightConstant) {
        // TODO(ali): not defined currently for constant complex types
        ATypeTag leftTag = leftConstant.getType().getTypeTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // TODO(ali): deallocate when implemented
        return Result.NULL;
    }

    private Result compareComplex(IAType leftType, ATypeTag leftTag, IPointable left, IAType rightType,
            ATypeTag rightTag, IPointable right) throws HyracksDataException {
        if (leftTag != rightTag) {
            return Result.INCOMPARABLE;
        }
        IAType leftCompileType = TypeComputeUtils.getActualTypeOrOpen(leftType, leftTag);
        IAType rightCompileType = TypeComputeUtils.getActualTypeOrOpen(rightType, rightTag);
        switch (leftTag) {
            case MULTISET:
                return compareMultisets(leftCompileType, leftTag, left, rightCompileType, rightTag, right);
            case ARRAY:
                return compareArrays(leftCompileType, leftTag, left, rightCompileType, rightTag, right);
            case OBJECT:
                return compareRecords(leftCompileType, left, rightCompileType, right);
            default:
                return Result.NULL;
        }
    }

    private Result compareArrays(IAType leftType, ATypeTag leftListTag, IPointable left, IAType rightType,
            ATypeTag rightListTag, IPointable right) throws HyracksDataException {
        // reaching here, both left and right have to be arrays (should be enforced)
        byte[] leftBytes = left.getByteArray();
        byte[] rightBytes = right.getByteArray();
        int leftStart = left.getStartOffset();
        int rightStart = right.getStartOffset();
        int leftNumItems = ListAccessorUtil.numberOfItems(leftBytes, leftStart);
        int rightNumItems = ListAccessorUtil.numberOfItems(rightBytes, rightStart);
        IAType leftItemCompileType = ((AbstractCollectionType) leftType).getItemType();
        IAType rightItemCompileType = ((AbstractCollectionType) rightType).getItemType();
        ATypeTag leftItemTag = leftItemCompileType.getTypeTag();
        ATypeTag rightItemTag = rightItemCompileType.getTypeTag();

        // TODO(ali): could be optimized to not need pointable when changing comparator to be non-tagged & no visitable
        IPointable leftItem = voidPointableAllocator.allocate(null);
        IPointable rightItem = voidPointableAllocator.allocate(null);
        // TODO(ali): optimize to not need this storage, will require optimizing records comparison to not use visitable
        ArrayBackedValueStorage leftStorage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        ArrayBackedValueStorage rightStorage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        Result determiningResult = null;
        Result tempResult;
        byte leftItemTagByte, rightItemTagByte;
        ATypeTag leftItemRuntimeTag, rightItemRuntimeTag;
        try {
            for (int i = 0; i < leftNumItems && i < rightNumItems; i++) {
                ListAccessorUtil.getItem(leftBytes, leftStart, i, leftListTag, leftItemTag, leftItem, leftStorage);
                ListAccessorUtil.getItem(rightBytes, rightStart, i, rightListTag, rightItemTag, rightItem,
                        rightStorage);
                leftItemTagByte = leftItem.getByteArray()[leftItem.getStartOffset()];
                rightItemTagByte = rightItem.getByteArray()[rightItem.getStartOffset()];

                // if both tags are derived, get item type or default to open item if array is open, then call complex
                leftItemRuntimeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftItemTagByte);
                rightItemRuntimeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(rightItemTagByte);
                if (leftItemRuntimeTag.isDerivedType() && rightItemRuntimeTag.isDerivedType()) {
                    tempResult = compareComplex(leftItemCompileType, leftItemRuntimeTag, leftItem, rightItemCompileType,
                            rightItemRuntimeTag, rightItem);
                } else {
                    tempResult = scalarComparator.compare(leftItem, rightItem);
                }

                if (tempResult == Result.INCOMPARABLE || tempResult == Result.MISSING || tempResult == Result.NULL) {
                    return Result.INCOMPARABLE;
                }

                // skip to next pair if current one is equal or the result of the comparison has already been decided
                if (determiningResult == null && tempResult != Result.EQ) {
                    determiningResult = tempResult;
                }
            }

            if (determiningResult != null) {
                return determiningResult;
            }
            return ILogicalBinaryComparator.asResult(Integer.compare(leftNumItems, rightNumItems));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            storageAllocator.free(rightStorage);
            storageAllocator.free(leftStorage);
            voidPointableAllocator.free(rightItem);
            voidPointableAllocator.free(leftItem);
        }
    }

    private Result compareMultisets(IAType leftType, ATypeTag leftListTag, IPointable left, IAType rightType,
            ATypeTag rightListTag, IPointable right) throws HyracksDataException {
        // TODO(ali): multiset comparison logic here
        // equality is the only operation defined for multiset
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        return Result.NULL;
    }

    private Result compareRecords(IAType leftType, IPointable left, IAType rightType, IPointable right)
            throws HyracksDataException {
        // equality is the only operation defined for records
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        ARecordType leftRecordType = (ARecordType) leftType;
        ARecordType rightRecordType = (ARecordType) rightType;
        ARecordVisitablePointable leftRecord = pointableAllocator.allocateRecordValue(leftRecordType);
        ARecordVisitablePointable rightRecord = pointableAllocator.allocateRecordValue(rightRecordType);
        // keeps track of the fields in the right record that have not been matched
        BitSet notMatched = bitSetAllocator.allocate(null);
        try {
            leftRecord.set(left);
            rightRecord.set(right);
            List<IVisitablePointable> leftFieldValues = leftRecord.getFieldValues();
            List<IVisitablePointable> leftFieldNames = leftRecord.getFieldNames();
            List<IVisitablePointable> rightFieldValues = rightRecord.getFieldValues();
            List<IVisitablePointable> rightFieldNames = rightRecord.getFieldNames();
            IVisitablePointable leftFieldValue, leftFieldName, rightFieldValue, rightFieldName;
            int leftNumFields = leftFieldNames.size();
            int rightNumFields = rightFieldNames.size();
            IAType leftFieldType, rightFieldType;
            ATypeTag leftFTag, rightFTag;
            Result tempCompResult;
            boolean foundFieldInRight;
            boolean notEqual = false;
            notMatched.set(0, rightNumFields);
            for (int i = 0; i < leftNumFields; i++) {
                leftFieldValue = leftFieldValues.get(i);
                leftFTag = VALUE_TYPE_MAPPING[leftFieldValue.getByteArray()[leftFieldValue.getStartOffset()]];

                // ignore if the field value is missing
                if (leftFTag != ATypeTag.MISSING) {
                    // start looking for the field in the right record
                    foundFieldInRight = false;
                    leftFieldName = leftFieldNames.get(i);
                    for (int k = 0; k < rightNumFields; k++) {
                        rightFieldName = rightFieldNames.get(k);
                        if (notMatched.get(k) && equalNames(leftFieldName, rightFieldName)) {
                            notMatched.clear(k);
                            rightFieldValue = rightFieldValues.get(k);
                            rightFTag = VALUE_TYPE_MAPPING[rightFieldValue.getByteArray()[rightFieldValue
                                    .getStartOffset()]];
                            // if right field has a missing value, ignore and flag the two records as not equal
                            if (rightFTag != ATypeTag.MISSING) {
                                foundFieldInRight = true;
                                if (leftFTag == ATypeTag.NULL || rightFTag == ATypeTag.NULL) {
                                    tempCompResult = Result.NULL;
                                } else if (leftFTag.isDerivedType() && rightFTag.isDerivedType()) {
                                    leftFieldType = RecordUtil.getType(leftRecordType, i, leftFTag);
                                    rightFieldType = RecordUtil.getType(rightRecordType, k, rightFTag);
                                    tempCompResult = compareComplex(leftFieldType, leftFTag, leftFieldValue,
                                            rightFieldType, rightFTag, rightFieldValue);
                                } else {
                                    tempCompResult = scalarComparator.compare(leftFieldValue, rightFieldValue);
                                }

                                if (tempCompResult == Result.INCOMPARABLE || tempCompResult == Result.MISSING
                                        || tempCompResult == Result.NULL) {
                                    return Result.INCOMPARABLE;
                                }
                                if (tempCompResult != Result.EQ) {
                                    notEqual = true;
                                }
                            }
                            break;
                        }
                    }
                    if (!foundFieldInRight) {
                        notEqual = true;
                    }
                }
            }

            if (notEqual) {
                // LT or GT does not make a difference since this is an answer to equality
                return Result.LT;
            }
            // check if there is a field in the right record that does not exist in left record
            byte rightFieldTag;
            for (int i = notMatched.nextSetBit(0); i >= 0 && i < rightNumFields; i = notMatched.nextSetBit(i + 1)) {
                rightFieldValue = rightFieldValues.get(i);
                rightFieldTag = rightFieldValue.getByteArray()[rightFieldValue.getStartOffset()];
                if (rightFieldTag != SERIALIZED_MISSING_TYPE_TAG) {
                    // LT or GT does not make a difference since this is an answer to equality
                    return Result.LT;
                }
            }

            // reaching here means every field in the left record exists in the right and vice versa
            return Result.EQ;
        } finally {
            pointableAllocator.freeRecord(rightRecord);
            pointableAllocator.freeRecord(leftRecord);
            bitSetAllocator.free(notMatched);
        }
    }

    private boolean equalNames(IValueReference fieldName1, IValueReference fieldName2) {
        return UTF8StringUtil.compareTo(fieldName1.getByteArray(), fieldName1.getStartOffset() + 1,
                fieldName2.getByteArray(), fieldName2.getStartOffset() + 1) == 0;
    }
}
