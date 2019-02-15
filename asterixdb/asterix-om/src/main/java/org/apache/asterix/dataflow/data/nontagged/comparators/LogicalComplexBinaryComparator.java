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

import java.io.IOException;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class LogicalComplexBinaryComparator implements ILogicalBinaryComparator {

    private static final IObjectFactory<IPointable, Void> VOID_FACTORY = (type) -> new VoidPointable();
    private final IAType leftType;
    private final IAType rightType;
    private final boolean isEquality;
    private final LogicalScalarBinaryComparator scalarComparator;
    private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
    private final IObjectPool<IPointable, Void> voidPointableAllocator;

    public LogicalComplexBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.isEquality = isEquality;
        this.scalarComparator = new LogicalScalarBinaryComparator(isEquality);
        storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
        voidPointableAllocator = new ListObjectPool<>(VOID_FACTORY);
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
        switch (leftRuntimeTag) {
            case MULTISET:
                return compareMultisets(leftType, leftRuntimeTag, leftBytes, leftStart, rightType, rightRuntimeTag,
                        rightBytes, rightStart);
            case ARRAY:
                return compareArrays(leftType, leftRuntimeTag, leftBytes, leftStart, rightType, rightRuntimeTag,
                        rightBytes, rightStart);
            case OBJECT:
                return compareRecords(leftType, leftBytes, leftStart, leftLen, rightType, rightBytes, rightStart,
                        rightLen);
            default:
                return Result.NULL;
        }
    }

    private Result compareArrays(IAType leftType, ATypeTag leftListTag, byte[] leftBytes, int leftStart,
            IAType rightType, ATypeTag rightListTag, byte[] rightBytes, int rightStart) throws HyracksDataException {
        // reaching here, both left and right have to be arrays (should be enforced)
        int leftNumItems = ListAccessorUtil.numberOfItems(leftBytes, leftStart);
        int rightNumItems = ListAccessorUtil.numberOfItems(rightBytes, rightStart);
        IAType leftListCompileType = TypeComputeUtils.getActualType(leftType);
        if (leftListCompileType.getTypeTag() == ATypeTag.ANY) {
            leftListCompileType = DefaultOpenFieldType.getDefaultOpenFieldType(leftListTag);
        }
        IAType rightListCompileType = TypeComputeUtils.getActualType(rightType);
        if (rightListCompileType.getTypeTag() == ATypeTag.ANY) {
            rightListCompileType = DefaultOpenFieldType.getDefaultOpenFieldType(rightListTag);
        }
        IAType leftItemCompileType = ((AbstractCollectionType) leftListCompileType).getItemType();
        IAType rightItemCompileType = ((AbstractCollectionType) rightListCompileType).getItemType();
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
            byte[] rightBytes, int rightStart, int rightLen) {
        // TODO(ali): record comparison logic here
        // equality is the only operation defined for records
        if (!isEquality) {
            return Result.INCOMPARABLE;
        }
        return Result.NULL;
    }
}
