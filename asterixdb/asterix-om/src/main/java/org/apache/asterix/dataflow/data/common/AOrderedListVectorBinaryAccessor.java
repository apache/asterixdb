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
package org.apache.asterix.dataflow.data.common;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;

/**
 * Accessor for extracting vectors from AOrderedList<ADouble> format.
 * This class handles the AsterixDB-specific serialization format and provides
 * a clean interface for the Hyracks storage layer.
 *
 * Format: [ATypeTag.ORDEREDLIST][ATypeTag.DOUBLE][list metadata][items...]
 */
public class AOrderedListVectorBinaryAccessor implements IVTreeBinaryAccessor {

    protected byte[] data;
    protected int start;
    protected int length;
    protected int listLength;

    @Override
    public void reset(byte[] data, int start, int length) throws HyracksDataException {
        this.data = data;
        this.start = start;
        this.length = length;
        this.listLength = AOrderedListSerializerDeserializer.getNumberOfItems(data, start);
    }

    @Override
    public double[] getVector() throws HyracksDataException {
        if (data == null) {
            // TODO(vector-errors): uncoded IllegalStateException -> surfaces to the user as "Internal error".
            // Reachable only if a caller skips reset(); decide whether to keep it as an unchecked
            // invariant or convert to RuntimeDataException(ErrorCode.ILLEGAL_STATE, ...).
            throw new IllegalStateException("Accessor not initialized. Call reset() first.");
        }

        double[] vector = new double[listLength];

        // Get item type (should be ADouble for vectors)
        ATypeTag itemType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[start + 1]);

        for (int i = 0; i < listLength; i++) {
            int itemOffset = getItemOffset(i);
            vector[i] = extractNumericValue(itemOffset, itemType);
        }

        return vector;
    }

    @Override
    public int getDimension() throws HyracksDataException {
        if (data == null) {
            // TODO(vector-errors): uncoded IllegalStateException -> surfaces to the user as "Internal error".
            // Reachable only if a caller skips reset(); decide whether to keep it as an unchecked
            // invariant or convert to RuntimeDataException(ErrorCode.ILLEGAL_STATE, ...).
            throw new IllegalStateException("Accessor not initialized. Call reset() first.");
        }
        return listLength;
    }

    /**
     * Get the byte offset of an item in the list.
     */
    protected int getItemOffset(int itemIndex) throws HyracksDataException {
        return AOrderedListSerializerDeserializer.getItemOffset(data, start, itemIndex);
    }

    /**
     * Extract a numeric value from a serialized AsterixDB type.
     * Handles ADouble, AFloat, AInt32, AInt64.
     */
    protected double extractNumericValue(int itemOffset, ATypeTag itemType) throws HyracksDataException {
        // For homogeneous lists, itemType tells us the type
        // For heterogeneous lists (itemType == ANY), read type tag from each item
        ATypeTag actualType = itemType;
        int valueOffset = itemOffset;

        if (itemType == ATypeTag.ANY) {
            // Heterogeneous list - read type tag from item
            actualType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[itemOffset]);
            valueOffset = itemOffset + 1; // Skip type tag
        }

        return switch (actualType) {
            case DOUBLE -> DoublePointable.getDouble(data, valueOffset);
            case FLOAT -> FloatPointable.getFloat(data, valueOffset);
            case INTEGER -> IntegerPointable.getInteger(data, valueOffset);
            case BIGINT -> LongPointable.getLong(data, valueOffset);
            default -> throw new RuntimeDataException(ErrorCode.UNSUPPORTED_VECTOR_ELEMENT_TYPE, actualType);
        };
    }
}
