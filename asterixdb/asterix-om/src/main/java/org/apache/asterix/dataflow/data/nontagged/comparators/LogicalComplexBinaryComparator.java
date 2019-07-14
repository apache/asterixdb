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

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.pointables.nonvisitor.RecordField;
import org.apache.asterix.om.pointables.nonvisitor.SortedRecord;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;

public final class LogicalComplexBinaryComparator implements ILogicalBinaryComparator {

    private final IAType leftType;
    private final IAType rightType;
    private final boolean isEquality;
    private final LogicalScalarBinaryComparator scalarComparator;
    private final ListObjectPool<TaggedValueReference, Void> taggedValuePool = new ListObjectPool<>(VALUE_FACTORY);
    private final ListObjectPool<SortedRecord, ARecordType> recordPool = new ListObjectPool<>(RECORD_FACTORY);

    LogicalComplexBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.isEquality = isEquality;
        this.scalarComparator = LogicalScalarBinaryComparator.of(isEquality);
    }

    @Override
    public Result compare(TaggedValueReference left, TaggedValueReference right) throws HyracksDataException {
        ATypeTag leftRuntimeTag = left.getTag();
        ATypeTag rightRuntimeTag = right.getTag();
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
    public Result compare(TaggedValueReference left, IAObject rightConstant) {
        // TODO(ali): not defined currently for constant complex types
        ATypeTag leftTag = left.getTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // TODO(ali): deallocate when implemented
        return Result.NULL;
    }

    @Override
    public Result compare(IAObject leftConstant, TaggedValueReference right) {
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

    private Result compareComplex(IAType leftType, ATypeTag leftRuntimeTag, TaggedValueReference left, IAType rightType,
            ATypeTag rightRuntimeTag, TaggedValueReference right) throws HyracksDataException {
        if (leftRuntimeTag != rightRuntimeTag) {
            return Result.INCOMPARABLE;
        }
        IAType leftCompileType = TypeComputeUtils.getActualTypeOrOpen(leftType, leftRuntimeTag);
        IAType rightCompileType = TypeComputeUtils.getActualTypeOrOpen(rightType, rightRuntimeTag);
        switch (leftRuntimeTag) {
            case MULTISET:
                return compareMultisets(leftCompileType, leftRuntimeTag, left, rightCompileType, rightRuntimeTag,
                        right);
            case ARRAY:
                return compareArrays(leftCompileType, left, rightCompileType, right);
            case OBJECT:
                return compareRecords(leftCompileType, left, rightCompileType, right);
            default:
                return Result.NULL;
        }
    }

    private Result compareArrays(IAType leftType, TaggedValueReference leftArray, IAType rightType,
            TaggedValueReference rightArray) throws HyracksDataException {
        // reaching here, both leftArray and rightArray have to be arrays (should be enforced)
        int leftNumItems = SerializerDeserializerUtil.getNumberOfItemsNonTagged(leftArray);
        int rightNumItems = SerializerDeserializerUtil.getNumberOfItemsNonTagged(rightArray);
        IAType leftItemCompileType = ((AbstractCollectionType) leftType).getItemType();
        IAType rightItemCompileType = ((AbstractCollectionType) rightType).getItemType();
        ATypeTag leftArrayItemTag = leftItemCompileType.getTypeTag();
        ATypeTag rightArrayItemTag = rightItemCompileType.getTypeTag();
        boolean leftItemHasTag = leftArrayItemTag == ATypeTag.ANY;
        boolean rightItemHasTag = rightArrayItemTag == ATypeTag.ANY;
        TaggedValueReference leftItem = taggedValuePool.allocate(null);
        TaggedValueReference rightItem = taggedValuePool.allocate(null);
        Result determiningResult = null;
        Result tempResult;
        try {
            for (int i = 0; i < leftNumItems && i < rightNumItems; i++) {
                ListAccessorUtil.getItemFromList(leftArray, i, leftItem, leftArrayItemTag, leftItemHasTag);
                ListAccessorUtil.getItemFromList(rightArray, i, rightItem, rightArrayItemTag, rightItemHasTag);
                // if both tags are derived, get item type or default to open item if array is open, then call complex
                ATypeTag leftItemRuntimeTag = leftItem.getTag();
                ATypeTag rightItemRuntimeTag = rightItem.getTag();
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
            taggedValuePool.free(rightItem);
            taggedValuePool.free(leftItem);
        }
    }

    private Result compareMultisets(IAType leftType, ATypeTag leftListTag, TaggedValueReference left, IAType rightType,
            ATypeTag rightListTag, TaggedValueReference right) throws HyracksDataException {
        // TODO(ali): multiset comparison logic here
        // equality is the only operation defined for multiset
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        return ILogicalBinaryComparator
                .asResult(RawBinaryComparatorFactory.compare(left.getByteArray(), left.getStartOffset(),
                        left.getLength(), right.getByteArray(), right.getStartOffset(), right.getLength()));
    }

    private Result compareRecords(IAType leftType, TaggedValueReference left, IAType rightType,
            TaggedValueReference right) throws HyracksDataException {
        // equality is the only operation defined for records
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        ARecordType leftRecordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(leftType, ATypeTag.OBJECT);
        ARecordType rightRecordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(rightType, ATypeTag.OBJECT);
        SortedRecord leftRecord = recordPool.allocate(leftRecordType);
        SortedRecord rightRecord = recordPool.allocate(rightRecordType);
        TaggedValueReference leftFieldValue = taggedValuePool.allocate(null);
        TaggedValueReference rightFieldValue = taggedValuePool.allocate(null);
        try {
            leftRecord.resetNonTagged(left.getByteArray(), left.getStartOffset());
            rightRecord.resetNonTagged(right.getByteArray(), right.getStartOffset());
            boolean notEqual = false;
            RecordField leftField = null, rightField = null;
            int previousNamesComparisonResult = 0;
            while (!leftRecord.isEmpty() && !rightRecord.isEmpty()) {
                if (previousNamesComparisonResult == 0) {
                    // previous field names were equal or first time to enter the loop
                    leftField = leftRecord.poll();
                    rightField = rightRecord.poll();
                } else if (previousNamesComparisonResult > 0) {
                    // right field name was less than left field name. get next field from right
                    rightField = rightRecord.poll();
                } else {
                    leftField = leftRecord.poll();
                }
                Result tempCompResult;
                previousNamesComparisonResult = RecordField.FIELD_NAME_COMP.compare(leftField, rightField);
                if (previousNamesComparisonResult == 0) {
                    // filed names are equal
                    leftRecord.getFieldValue(leftField, leftFieldValue);
                    rightRecord.getFieldValue(rightField, rightFieldValue);
                    ATypeTag leftFTag = leftFieldValue.getTag();
                    ATypeTag rightFTag = rightFieldValue.getTag();
                    if (leftFTag == ATypeTag.NULL || rightFTag == ATypeTag.NULL) {
                        tempCompResult = Result.NULL;
                    } else if (leftFTag.isDerivedType() && rightFTag.isDerivedType()) {
                        IAType leftFieldType = RecordUtil.getType(leftRecordType, leftField.getIndex(), leftFTag);
                        IAType rightFieldType = RecordUtil.getType(rightRecordType, rightField.getIndex(), rightFTag);
                        tempCompResult = compareComplex(leftFieldType, leftFTag, leftFieldValue, rightFieldType,
                                rightFTag, rightFieldValue);
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
                } else {
                    notEqual = true;
                }
            }
            if (notEqual || leftRecord.size() != rightRecord.size()) {
                // LT or GT does not make a difference since this is an answer to equality
                return Result.LT;
            }
            // reaching here means every field in the left record exists in the right and vice versa
            return Result.EQ;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            recordPool.free(rightRecord);
            recordPool.free(leftRecord);
            taggedValuePool.free(rightFieldValue);
            taggedValuePool.free(leftFieldValue);
        }
    }
}
