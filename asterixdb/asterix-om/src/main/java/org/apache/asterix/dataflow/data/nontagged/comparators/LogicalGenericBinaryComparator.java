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

public class LogicalGenericBinaryComparator implements ILogicalBinaryComparator {

    private final LogicalComplexBinaryComparator complexComparator;
    private final LogicalScalarBinaryComparator scalarComparator;

    public LogicalGenericBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        complexComparator = new LogicalComplexBinaryComparator(leftType, rightType, isEquality);
        scalarComparator = new LogicalScalarBinaryComparator(isEquality);
    }

    @Override
    public Result compare(byte[] leftBytes, int leftStart, int leftLen, byte[] rightBytes, int rightStart, int rightLen)
            throws HyracksDataException {
        ATypeTag leftTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftBytes[leftStart]);
        ATypeTag rightTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(rightBytes[rightStart]);
        if (leftTag.isDerivedType() && rightTag.isDerivedType()) {
            return complexComparator.compare(leftBytes, leftStart, leftLen, rightBytes, rightStart, rightLen);
        }
        return scalarComparator.compare(leftBytes, leftStart, leftLen, rightBytes, rightStart, rightLen);
    }

    @Override
    public Result compare(byte[] leftBytes, int leftStart, int leftLen, IAObject rightConstant) {
        ATypeTag leftTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(leftBytes[leftStart]);
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag.isDerivedType() && rightTag.isDerivedType()) {
            return complexComparator.compare(leftBytes, leftStart, leftLen, rightConstant);
        }
        return scalarComparator.compare(leftBytes, leftStart, leftLen, rightConstant);
    }

    @Override
    public Result compare(IAObject leftConstant, byte[] rightBytes, int rightStart, int rightLen) {
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
        ATypeTag leftTag = leftConstant.getType().getTypeTag();
        ATypeTag rightTag = rightConstant.getType().getTypeTag();
        if (leftTag.isDerivedType() && rightTag.isDerivedType()) {
            return complexComparator.compare(leftConstant, rightConstant);
        }
        return scalarComparator.compare(leftConstant, rightConstant);
    }
}
