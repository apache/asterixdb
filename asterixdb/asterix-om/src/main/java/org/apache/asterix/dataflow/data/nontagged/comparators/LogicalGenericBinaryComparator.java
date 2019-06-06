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

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;

public final class LogicalGenericBinaryComparator implements ILogicalBinaryComparator {

    private final LogicalComplexBinaryComparator complexComparator;
    private final LogicalScalarBinaryComparator scalarComparator;

    LogicalGenericBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        complexComparator = new LogicalComplexBinaryComparator(leftType, rightType, isEquality);
        scalarComparator = LogicalScalarBinaryComparator.of(isEquality);
    }

    @Override
    public Result compare(IPointable left, IPointable right) throws HyracksDataException {
        ATypeTag leftTag = VALUE_TYPE_MAPPING[left.getByteArray()[left.getStartOffset()]];
        ATypeTag rightTag = VALUE_TYPE_MAPPING[right.getByteArray()[right.getStartOffset()]];
        if (leftTag.isDerivedType() && rightTag.isDerivedType()) {
            return complexComparator.compare(left, right);
        }
        return scalarComparator.compare(left, right);
    }

    @Override
    public Result compare(IPointable left, IAObject rightConstant) {
        ATypeTag leftTag = VALUE_TYPE_MAPPING[left.getByteArray()[left.getStartOffset()]];
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag.isDerivedType() && rightTag.isDerivedType()) {
            return complexComparator.compare(left, rightConstant);
        }
        return scalarComparator.compare(left, rightConstant);
    }

    @Override
    public Result compare(IAObject leftConstant, IPointable right) {
        Result result = compare(right, leftConstant);
        if (result == Result.LT) {
            return Result.GT;
        } else if (result == Result.GT) {
            return Result.LT;
        }
        return result;
    }

    @Override
    public Result compare(IAObject leftConstant, IAObject rightConstant) {
        ATypeTag leftTag = leftConstant.getType().getTypeTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag.isDerivedType() && rightTag.isDerivedType()) {
            return complexComparator.compare(leftConstant, rightConstant);
        }
        return scalarComparator.compare(leftConstant, rightConstant);
    }
}
