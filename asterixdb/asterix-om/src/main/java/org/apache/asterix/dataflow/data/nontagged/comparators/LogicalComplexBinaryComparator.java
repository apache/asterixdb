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

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class LogicalComplexBinaryComparator implements ILogicalBinaryComparator {

    private static final IObjectFactory<BitSet, Void> BIT_SET_FACTORY = (type) -> new BitSet();
    private static final IObjectFactory<IPointable, Void> VOID_FACTORY = (type) -> new VoidPointable();
    private final IAType leftType;
    private final IAType rightType;
    private final boolean isEquality;
    private final LogicalScalarBinaryComparator scalarComparator;
    private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
    private final IObjectPool<IPointable, Void> voidPointableAllocator;
    private final IObjectPool<BitSet, Void> bitSetAllocator;
    private final PointableAllocator pointableAllocator;
    private final IBinaryComparator utf8Comp;
    private final StringBuilder builder;

    public LogicalComplexBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.isEquality = isEquality;
        this.scalarComparator = new LogicalScalarBinaryComparator(isEquality);
        storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
        voidPointableAllocator = new ListObjectPool<>(VOID_FACTORY);
        bitSetAllocator = new ListObjectPool<>(BIT_SET_FACTORY);
        pointableAllocator = new PointableAllocator();
        utf8Comp = BinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
        builder = new StringBuilder();
    }

    @Override
    public Result compare(byte[] leftBytes, int leftStart, int leftLen, byte[] rightBytes, int rightStart, int rightLen)
            throws HyracksDataException {
        ATypeTag leftRuntimeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftBytes[leftStart]);
        ATypeTag rightRuntimeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(rightBytes[rightStart]);
        Result comparisonResult = LogicalComparatorUtil.returnMissingOrNullOrMismatch(leftRuntimeTag, rightRuntimeTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // make sure both left and right are complex types
        if (!leftRuntimeTag.isDerivedType() || !rightRuntimeTag.isDerivedType()) {
            throw new IllegalStateException("Input types are not complex type");
        }
        try {
            return compareComplex(leftType, leftRuntimeTag, leftBytes, leftStart, leftLen, rightType, rightRuntimeTag,
                    rightBytes, rightStart, rightLen);
        } finally {
            storageAllocator.reset();
            voidPointableAllocator.reset();
        }
    }

    @Override
    public Result compare(byte[] leftBytes, int leftStart, int leftLen, IAObject rightConstant) {
        // TODO(ali): not defined currently for constant complex types
        ATypeTag leftTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftBytes[leftStart]);
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        Result comparisonResult = LogicalComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // TODO(ali): deallocate when implemented
        return Result.NULL;
    }

    @Override
    public Result compare(IAObject leftConstant, byte[] rightBytes, int rightStart, int rightLen) {
        // TODO(ali): not defined currently for constant complex types
        Result result = compare(rightBytes, rightStart, rightLen, leftConstant);
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
        Result comparisonResult = LogicalComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        // TODO(ali): deallocate when implemented
        return Result.NULL;
    }

    private Result compareComplex(IAType leftType, ATypeTag leftRuntimeTag, byte[] leftBytes, int leftStart,
            int leftLen, IAType rightType, ATypeTag rightRuntimeTag, byte[] rightBytes, int rightStart, int rightLen)
            throws HyracksDataException {
        if (leftRuntimeTag != rightRuntimeTag) {
            return Result.INCOMPARABLE;
        }
        IAType leftCompileType = TypeComputeUtils.getActualType(leftType);
        if (leftCompileType.getTypeTag() == ATypeTag.ANY) {
            leftCompileType = DefaultOpenFieldType.getDefaultOpenFieldType(leftRuntimeTag);
        }
        IAType rightCompileType = TypeComputeUtils.getActualType(rightType);
        if (rightCompileType.getTypeTag() == ATypeTag.ANY) {
            rightCompileType = DefaultOpenFieldType.getDefaultOpenFieldType(rightRuntimeTag);
        }
        switch (leftRuntimeTag) {
            case MULTISET:
                return compareMultisets(leftCompileType, leftRuntimeTag, leftBytes, leftStart, rightCompileType,
                        rightRuntimeTag, rightBytes, rightStart);
            case ARRAY:
                return compareArrays(leftCompileType, leftRuntimeTag, leftBytes, leftStart, rightCompileType,
                        rightRuntimeTag, rightBytes, rightStart);
            case OBJECT:
                return compareRecords(leftCompileType, leftBytes, leftStart, leftLen, rightCompileType, rightBytes,
                        rightStart, rightLen);
            default:
                return Result.NULL;
        }
    }

    private Result compareArrays(IAType leftType, ATypeTag leftListTag, byte[] leftBytes, int leftStart,
            IAType rightType, ATypeTag rightListTag, byte[] rightBytes, int rightStart) throws HyracksDataException {
        // reaching here, both left and right have to be arrays (should be enforced)
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
        Result unknownResult = null;
        Result determiningResult = null;
        Result tempResult;
        byte leftItemTagByte;
        byte rightItemTagByte;
        ATypeTag leftItemRuntimeTag;
        ATypeTag rightItemRuntimeTag;
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
                    tempResult = compareComplex(leftItemCompileType, leftItemRuntimeTag, leftItem.getByteArray(),
                            leftItem.getStartOffset(), leftItem.getLength(), rightItemCompileType, rightItemRuntimeTag,
                            rightItem.getByteArray(), rightItem.getStartOffset(), rightItem.getLength());
                } else {
                    tempResult = scalarComparator.compare(leftItem.getByteArray(), leftItem.getStartOffset(),
                            leftItem.getLength(), rightItem.getByteArray(), rightItem.getStartOffset(),
                            rightItem.getLength());
                }

                if (tempResult == Result.INCOMPARABLE) {
                    return tempResult;
                }

                // skip to next pair if current one is equal or the result of the comparison has already been decided
                if (tempResult != Result.EQ && determiningResult == null) {
                    // tempResult = NULL, MISSING, LT, GT
                    if ((tempResult == Result.NULL || tempResult == Result.MISSING)) {
                        // keep unknown response if there is no yet a determining result switching to missing if found
                        if (unknownResult != Result.MISSING) {
                            unknownResult = tempResult;
                        }
                    } else {
                        // tempResult = LT, GT
                        determiningResult = tempResult;
                    }
                }
            }

            // reaching here means the two arrays are comparable
            if (isEquality && leftNumItems != rightNumItems) {
                return ILogicalBinaryComparator.asResult(Integer.compare(leftNumItems, rightNumItems));
            }
            // for >, < make unknownResult the determiningResult if unknownResult was encountered before finding one
            if (!isEquality && unknownResult != null) {
                determiningResult = unknownResult;
            }
            if (determiningResult != null) {
                return determiningResult;
            }
            if (unknownResult != null) {
                return unknownResult;
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

    private Result compareMultisets(IAType leftType, ATypeTag leftListTag, byte[] leftBytes, int leftStart,
            IAType rightType, ATypeTag rightListTag, byte[] rightBytes, int rightStart) throws HyracksDataException {
        // TODO(ali): multiset comparison logic here
        // equality is the only operation defined for multiset
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        return Result.NULL;
    }

    private Result compareRecords(IAType leftType, byte[] leftBytes, int leftStart, int leftLen, IAType rightType,
            byte[] rightBytes, int rightStart, int rightLen) throws HyracksDataException {
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
            leftRecord.set(leftBytes, leftStart, leftLen);
            rightRecord.set(rightBytes, rightStart, rightLen);
            List<IVisitablePointable> leftFieldValues = leftRecord.getFieldValues();
            List<IVisitablePointable> leftFieldNames = leftRecord.getFieldNames();
            List<IVisitablePointable> rightFieldValues = rightRecord.getFieldValues();
            List<IVisitablePointable> rightFieldNames = rightRecord.getFieldNames();
            IVisitablePointable leftFieldValue;
            IVisitablePointable leftFieldName;
            IVisitablePointable rightFieldValue;
            IVisitablePointable rightFieldName;
            int leftNumFields = leftFieldNames.size();
            int rightNumFields = rightFieldNames.size();
            IAType leftFieldType;
            IAType rightFieldType;
            ATypeTag leftFTag;
            ATypeTag rightFTag;
            Result tempCompResult;
            Result unknownResult = null;
            Result determiningResult = null;
            String complexFieldName;
            boolean foundFieldInRight;
            boolean notEqual = false;
            notMatched.set(0, rightNumFields);
            for (int i = 0; i < leftNumFields; i++) {
                leftFieldValue = leftFieldValues.get(i);
                leftFTag = VALUE_TYPE_MAPPING[leftFieldValue.getByteArray()[leftFieldValue.getStartOffset()]];

                // ignore if the field value is missing
                if (leftFTag != ATypeTag.MISSING) {
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
                                    complexFieldName = getComplexFieldName(leftFieldName);
                                    leftFieldType = getComplexFieldType(leftRecordType, complexFieldName, leftFTag);
                                    rightFieldType = getComplexFieldType(rightRecordType, complexFieldName, rightFTag);
                                    tempCompResult =
                                            compareComplex(leftFieldType, leftFTag, leftFieldValue.getByteArray(),
                                                    leftFieldValue.getStartOffset(), leftFieldValue.getLength(),
                                                    rightFieldType, rightFTag, rightFieldValue.getByteArray(),
                                                    rightFieldValue.getStartOffset(), rightFieldValue.getLength());
                                } else {
                                    tempCompResult = scalarComparator.compare(leftFieldValue.getByteArray(),
                                            leftFieldValue.getStartOffset(), leftFieldValue.getLength(),
                                            rightFieldValue.getByteArray(), rightFieldValue.getStartOffset(),
                                            rightFieldValue.getLength());
                                }

                                if (tempCompResult == Result.INCOMPARABLE) {
                                    return tempCompResult;
                                }
                                if (tempCompResult == Result.MISSING || tempCompResult == Result.NULL) {
                                    if (unknownResult != Result.MISSING) {
                                        unknownResult = tempCompResult;
                                    }
                                } else if (tempCompResult != Result.EQ && determiningResult == null) {
                                    determiningResult = tempCompResult;
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
            // two fields with the same name but having different values
            if (determiningResult != null) {
                return determiningResult;
            }
            // check if there is a field in the right record that does not exist in left record
            byte rightFieldTag;
            for (int i = 0; i < rightNumFields; i++) {
                rightFieldValue = rightFieldValues.get(i);
                rightFieldTag = rightFieldValue.getByteArray()[rightFieldValue.getStartOffset()];
                if (notMatched.get(i) && rightFieldTag != SERIALIZED_MISSING_TYPE_TAG) {
                    notEqual = true;
                    break;
                }
            }
            if (notEqual) {
                return Result.LT;
            }
            // reaching here means every field in the left record exists in the right and vice versa
            if (unknownResult != null) {
                return unknownResult;
            }
            return Result.EQ;
        } finally {
            pointableAllocator.freeRecord(rightRecord);
            pointableAllocator.freeRecord(leftRecord);
            bitSetAllocator.free(notMatched);
        }
    }

    private IAType getComplexFieldType(ARecordType recordType, String fieldName, ATypeTag fieldRuntimeTag) {
        IAType fieldType = recordType.getFieldType(fieldName);
        return fieldType == null ? DefaultOpenFieldType.getDefaultOpenFieldType(fieldRuntimeTag) : fieldType;
    }

    private String getComplexFieldName(IValueReference fieldName) {
        builder.setLength(0);
        return UTF8StringUtil.toString(builder, fieldName.getByteArray(), fieldName.getStartOffset() + 1).toString();
    }

    private boolean equalNames(IValueReference fieldName1, IValueReference fieldName2) throws HyracksDataException {
        // TODO(ali): refactor with PointableHelper and move it from runtime package
        return utf8Comp.compare(fieldName1.getByteArray(), fieldName1.getStartOffset() + 1, fieldName1.getLength() - 1,
                fieldName2.getByteArray(), fieldName2.getStartOffset() + 1, fieldName2.getLength() - 1) == 0;
    }
}
