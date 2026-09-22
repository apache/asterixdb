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
package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A usable vector is a list of the expected length whose every element is numeric.
 * <p>
 * An {@code ANY} item type declares nothing about any individual element, so those lists are read element
 * by element: {@code [0.2, 0.1, "s", 0.3]} is not a usable vector.
 * <p>
 * Never throws for any input. {@code isvector} is registered as a TOTAL function and CLUSTER BY's guard
 * depends on that to keep the optimizer from reordering it into something that can fail.
 * <p>
 * An instance reuses one {@link ListAccessor} across calls, so it is <b>not</b> thread-safe; hold one per
 * evaluator instance the way {@code IsVectorDescriptor} does.
 */
public final class VectorValidator {

    private final ListAccessor listAccessor = new ListAccessor();

    /**
     * Whether the value at {@code offset} is a usable vector.
     *
     * @param expectedDimension the declared dimension or a non-positive value to accept any non-zero
     *                          length. An empty list is rejected either way.
     */
    public boolean isVector(byte[] data, int offset, int length, int expectedDimension) {
        if (length == 0) {
            return false;
        }
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL || typeTag == ATypeTag.SYSTEM_NULL) {
            return false;
        }
        if (typeTag == null || !typeTag.isListType()) {
            return false;
        }
        try {
            listAccessor.reset(data, offset);
            int size = listAccessor.size();
            // An empty list is never a usable vector with or without a declared dimension: there is
            // nothing to index.
            if (size == 0 || (expectedDimension > 0 && size != expectedDimension)) {
                return false;
            }
            return elementsAreNumeric(size);
        } catch (HyracksDataException e) {
            // Tagged as a list but unreadable. Not usable. This class does not throw.
            return false;
        }
    }

    /**
     * Whether every element is numeric. A declared item type settles the whole list at once in O(1). An
     * {@code ANY} item type settles nothing about any individual element, so each one is read.
     */
    private boolean elementsAreNumeric(int size) throws HyracksDataException {
        ATypeTag itemTypeTag = listAccessor.getItemType();
        // A null item type constrains no element either, so it is read the same way.
        if (itemTypeTag != null && itemTypeTag != ATypeTag.ANY) {
            return isNumeric(itemTypeTag);
        }
        for (int i = 0; i < size; i++) {
            // getItemTypeAt reads the tag at the element's offset; the element's value is not needed.
            if (!isNumeric(listAccessor.getItemTypeAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNumeric(ATypeTag typeTag) {
        return typeTag != null && ATypeHierarchy.getTypeDomain(typeTag) == ATypeHierarchy.Domain.NUMERIC;
    }
}
