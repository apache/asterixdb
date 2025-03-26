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
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrIncomparable(leftRuntimeTag, rightRuntimeTag);
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
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrIncomparable(leftTag, rightTag);
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
        Result comparisonResult = ComparatorUtil.returnMissingOrNullOrIncomparable(leftTag, rightTag);
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
        return switch (leftRuntimeTag) {
            case ARRAY -> compareArrays(leftCompileType, left, rightCompileType, right);
            case MULTISET -> compareMultisets(leftCompileType, left, rightCompileType, right);
            case OBJECT -> compareRecords(leftCompileType, left, rightCompileType, right);
            default -> Result.NULL;
        };
    }

    private Result compareArrays(IAType leftType, TaggedValueReference leftArray, IAType rightType,
            TaggedValueReference rightArray) throws HyracksDataException {
        // equality is the only operation defined for arrays
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        // reaching here, both leftArray and rightArray have to be arrays (should be enforced)
        int leftNumItems = SerializerDeserializerUtil.getNumberOfItemsNonTagged(leftArray);
        int rightNumItems = SerializerDeserializerUtil.getNumberOfItemsNonTagged(rightArray);
        int c = Integer.compare(leftNumItems, rightNumItems);
        if (c != 0) {
            return ILogicalBinaryComparator.asResult(c);
        }
        IAType leftItemCompileType = ((AbstractCollectionType) leftType).getItemType();
        IAType rightItemCompileType = ((AbstractCollectionType) rightType).getItemType();
        ATypeTag leftArrayItemTag = leftItemCompileType.getTypeTag();
        ATypeTag rightArrayItemTag = rightItemCompileType.getTypeTag();
        boolean leftItemHasTag = leftArrayItemTag == ATypeTag.ANY;
        boolean rightItemHasTag = rightArrayItemTag == ATypeTag.ANY;
        TaggedValueReference leftItem = taggedValuePool.allocate(null);
        TaggedValueReference rightItem = taggedValuePool.allocate(null);
        try {
            Result determiningResult = null;
            for (int i = 0; i < leftNumItems; i++) {
                ListAccessorUtil.getItemFromList(leftArray, i, leftItem, leftArrayItemTag, leftItemHasTag);
                ListAccessorUtil.getItemFromList(rightArray, i, rightItem, rightArrayItemTag, rightItemHasTag);
                // if both tags are derived, get item type or default to open item if array is open, then call complex
                ATypeTag leftItemRuntimeTag = leftItem.getTag();
                ATypeTag rightItemRuntimeTag = rightItem.getTag();
                Result tempResult;
                if (leftItemRuntimeTag.isDerivedType() && rightItemRuntimeTag.isDerivedType()) {
                    tempResult = compareComplex(leftItemCompileType, leftItemRuntimeTag, leftItem, rightItemCompileType,
                            rightItemRuntimeTag, rightItem);
                } else {
                    tempResult = scalarComparator.compare(leftItem, rightItem);
                }

                if (tempResult == Result.NULL || tempResult == Result.MISSING) {
                    if (determiningResult == null) {
                        determiningResult = tempResult;
                    }
                } else if (tempResult != Result.EQ) {
                    // break early if not equal
                    return tempResult;
                }
            }

            if (determiningResult != null) {
                return determiningResult;
            }
            return Result.EQ;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            taggedValuePool.free(rightItem);
            taggedValuePool.free(leftItem);
        }
    }

    private Result compareMultisets(IAType leftType, TaggedValueReference left, IAType rightType,
            TaggedValueReference right) throws HyracksDataException {
        // equality is the only operation defined for multiset
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        return compareArrays(leftType, left, rightType, right);
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
            int c = Integer.compare(leftRecord.size(), rightRecord.size());
            if (c != 0) {
                return ILogicalBinaryComparator.asResult(c);
            }

            RecordField leftField, rightField;
            Result determiningResult = null;
            while (!leftRecord.isEmpty()) {
                leftField = leftRecord.poll();
                rightField = rightRecord.poll();
                int comp = RecordField.FIELD_NAME_COMP.compare(leftField, rightField);
                if (comp != 0) {
                    return ILogicalBinaryComparator.asResult(comp);
                }
                Result tempCompResult;
                leftRecord.getFieldValue(leftField, leftFieldValue);
                rightRecord.getFieldValue(rightField, rightFieldValue);
                ATypeTag leftFTag = leftFieldValue.getTag();
                ATypeTag rightFTag = rightFieldValue.getTag();
                if (leftFTag == ATypeTag.NULL || rightFTag == ATypeTag.NULL) {
                    tempCompResult = Result.NULL;
                } else if (leftFTag.isDerivedType() && rightFTag.isDerivedType()) {
                    IAType leftFieldType = RecordUtil.getType(leftRecordType, leftField.getIndex(), leftFTag);
                    IAType rightFieldType = RecordUtil.getType(rightRecordType, rightField.getIndex(), rightFTag);
                    tempCompResult = compareComplex(leftFieldType, leftFTag, leftFieldValue, rightFieldType, rightFTag,
                            rightFieldValue);
                } else {
                    tempCompResult = scalarComparator.compare(leftFieldValue, rightFieldValue);
                }

                if (tempCompResult == Result.NULL || tempCompResult == Result.MISSING) {
                    if (determiningResult == null) {
                        determiningResult = tempCompResult;
                    }
                } else if (tempCompResult != Result.EQ) {
                    // break early if not equal
                    return tempCompResult;
                }
            }

            if (determiningResult != null) {
                return determiningResult;
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
