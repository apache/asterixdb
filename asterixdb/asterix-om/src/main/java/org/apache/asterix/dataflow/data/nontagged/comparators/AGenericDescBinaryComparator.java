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

import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

final class AGenericDescBinaryComparator extends AbstractAGenericBinaryComparator {

    private final TaggedValueReference leftValue = new TaggedValueReference();
    private final TaggedValueReference rightValue = new TaggedValueReference();

    // interval asc and desc comparators are not the inverse of each other.
    // thus, we need to specify the interval desc comparator factory for descending comparisons.
    AGenericDescBinaryComparator(IAType leftType, IAType rightType) {
        super(leftType, rightType);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
        leftValue.set(b1, s1 + 1, l1 - 1, VALUE_TYPE_MAPPING[b1[s1]]);
        rightValue.set(b2, s2 + 1, l2 - 1, VALUE_TYPE_MAPPING[b2[s2]]);
        return -compare(leftType, leftValue, rightType, rightValue);
    }

    @Override
    protected int compareInterval(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -AIntervalDescPartialBinaryComparatorFactory.compare(b1, s1, l1, b2, s2, l2);
    }
}
