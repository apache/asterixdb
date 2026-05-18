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

package org.apache.hyracks.storage.am.vector.utils;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;

/**
 * Single authority for the VTree per-cluster directory ("metadata") tuple layout
 * {@code [max_distance : double, data_page_pointer : int]}, mirroring {@code VTreeDataTupleAccessor}
 * and {@code VTreeStaticTupleAccessor}. Owns the field indices, the {@link ITypeTraits} schema (so the
 * frame factory reads it from here), and the typed field reads, so no other class hardcodes a position.
 */
public final class VTreeMetadataTupleAccessor {

    /** Max distance-to-centroid of the data page this directory entry points at (field 0, raw double). */
    public static final int MAX_DISTANCE_FIELD = 0;

    /** Pointer to the data page (field 1, raw int). */
    public static final int DATA_PAGE_POINTER_FIELD = 1;

    /** Serializers for building a metadata tuple, in {@link #typeTraits()} field order. */
    @SuppressWarnings("rawtypes")
    public static final ISerializerDeserializer[] SERDES =
            { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

    private VTreeMetadataTupleAccessor() {
    }

    /**
     * Build a metadata entry tuple {@code <maxDistance:double, dataPageId:int>} for a directory frame,
     * in {@link #typeTraits()} field order.
     */
    public static ITupleReference createMetadataTuple(double maxDistance, int dataPageId) throws HyracksDataException {
        return TupleUtils.createTuple(SERDES, maxDistance, dataPageId);
    }

    /** Tuple schema {@code [max_distance : double, data_page_pointer : int]}. */
    public static ITypeTraits[] typeTraits() {
        ITypeTraits[] schema = new ITypeTraits[DATA_PAGE_POINTER_FIELD + 1];
        schema[MAX_DISTANCE_FIELD] = DoublePointable.TYPE_TRAITS;
        schema[DATA_PAGE_POINTER_FIELD] = IntegerPointable.TYPE_TRAITS;
        return schema;
    }

    /** Max distance in field 0 (raw big-endian double, no type tag). */
    public static double getMaxDistance(ITupleReference tuple) {
        return DoublePointable.getDouble(tuple.getFieldData(MAX_DISTANCE_FIELD),
                tuple.getFieldStart(MAX_DISTANCE_FIELD));
    }

    /** Data-page pointer in field 1 (raw big-endian int, no type tag). */
    public static int getDataPagePointer(ITupleReference tuple) {
        return IntegerPointable.getInteger(tuple.getFieldData(DATA_PAGE_POINTER_FIELD),
                tuple.getFieldStart(DATA_PAGE_POINTER_FIELD));
    }
}
