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
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public final class LogicalGenericBinaryComparator implements ILogicalBinaryComparator {

    private final LogicalComplexBinaryComparator complexComparator;
    private final LogicalScalarBinaryComparator scalarComparator;

    LogicalGenericBinaryComparator(IAType leftType, IAType rightType, boolean isEquality) {
        complexComparator = new LogicalComplexBinaryComparator(leftType, rightType, isEquality);
        scalarComparator = LogicalScalarBinaryComparator.of(isEquality);
    }

    @Override
    public Result compare(TaggedValueReference left, TaggedValueReference right) throws HyracksDataException {
        if (left.getTag().isDerivedType() && right.getTag().isDerivedType()) {
            return complexComparator.compare(left, right);
        }
        return scalarComparator.compare(left, right);
    }

    @Override
    public Result compare(TaggedValueReference left, IAObject rightConstant) {
        if (left.getTag().isDerivedType() && rightConstant.getType().getTypeTag().isDerivedType()) {
            return complexComparator.compare(left, rightConstant);
        }
        return scalarComparator.compare(left, rightConstant);
    }

    @Override
    public Result compare(IAObject leftConstant, TaggedValueReference right) {
        Result result = compare(right, leftConstant);
        if (result == Result.LT) {
            return Result.GT;
        } else if (result == Result.GT) {
            return Result.LT;
        }
        return result;
    }

    @Override
    public Result compare(IAObject leftConst, IAObject rightConst) {
        if (leftConst.getType().getTypeTag().isDerivedType() && rightConst.getType().getTypeTag().isDerivedType()) {
            return complexComparator.compare(leftConst, rightConst);
        }
        return scalarComparator.compare(leftConst, rightConst);
    }
}
