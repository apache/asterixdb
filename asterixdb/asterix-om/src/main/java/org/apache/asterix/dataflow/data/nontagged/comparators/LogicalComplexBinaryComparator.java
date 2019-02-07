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

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class LogicalComplexBinaryComparator implements ILogicalBinaryComparator {

    @SuppressWarnings("squid:S1068") // unused variable, remove once used
    private final IAType leftType;
    @SuppressWarnings("squid:S1068") // unused variable, remove once used
    private final IAType rightType;
    @SuppressWarnings("squid:S1068") // unused variable, remove once used
    private final boolean isEquality;
    @SuppressWarnings("squid:S1068") // unused variable, remove once used
    private final LogicalScalarBinaryComparator scalarComparator;

    public LogicalComplexBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.isEquality = isEquality;
        this.scalarComparator = new LogicalScalarBinaryComparator(isEquality);
    }

    @Override
    public Result compare(byte[] leftBytes, int leftStart, int leftLen, byte[] rightBytes, int rightStart, int rightLen)
            throws HyracksDataException {
        ATypeTag leftTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftBytes[leftStart]);
        ATypeTag rightTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(rightBytes[rightStart]);
        Result comparisonResult = LogicalComparatorUtil.returnMissingOrNullOrMismatch(leftTag, rightTag);
        if (comparisonResult != null) {
            return comparisonResult;
        }
        if (!leftTag.isDerivedType() || !rightTag.isDerivedType()) {
            return Result.NULL;
        }
        // TODO(ali): complex types(records, arrays, multisets) logic here
        return Result.NULL;
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
        return Result.NULL;
    }

    @Override
    public Result compare(IAObject leftConstant, byte[] rightBytes, int rightStart, int rightLen) {
        // TODO(ali): not defined currently for constant complex types
        Result result = compare(rightBytes, rightStart, rightLen, leftConstant);
        if (result == Result.LT) {
            return Result.GT;
        } else if (result == Result.GT) {
            return Result.LT;
        }
        return result;
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
        return Result.NULL;
    }
}
