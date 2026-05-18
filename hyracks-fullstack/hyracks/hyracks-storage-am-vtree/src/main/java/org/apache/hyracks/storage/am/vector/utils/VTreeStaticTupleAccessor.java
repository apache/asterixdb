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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.storage.am.vector.api.VTreeStaticTupleConstants;

/**
 * Single authority for the VTree STATIC-structure tuple layout, mirroring {@code VTreeDataTupleAccessor}
 * for the data tuple. It builds the per-field {@link ITypeTraits} schemas <em>positionally from
 * {@link VTreeStaticTupleConstants}</em>, so the frame factories no longer hand-maintain (and hand-keep
 * in sync) their own {@code ITypeTraits[]} arrays.
 * <p>
 * The pointer field — a child-page pointer for interior tuples, a metadata-page pointer for leaf tuples —
 * is always the LAST field:
 * <pre>
 *   Interior / non-quantized leaf: [ cid, centroid, pointer ]
 *   Quantized leaf:                [ cid, centroid, quantizedBytes, neighborList, pointer ]
 * </pre>
 */
public final class VTreeStaticTupleAccessor {

    // Schemas are fixed per layout, so build them once. The returned arrays are treated as read-only by
    // callers (frame factories hand them to a tuple-writer factory and never mutate them).
    private static final ITypeTraits[] BASE_SCHEMA = buildBaseSchema();
    private static final ITypeTraits[] LEAF_QUANTIZED_SCHEMA = buildLeafQuantizedSchema();

    private VTreeStaticTupleAccessor() {
    }

    /** Interior tuple schema: {@code [cid, centroid, childPagePointer]}. */
    public static ITypeTraits[] interiorTypeTraits() {
        return BASE_SCHEMA;
    }

    /**
     * Leaf tuple schema. A quantized leaf inserts the {@code quantizedBytes} and {@code neighborList}
     * fields before the trailing pointer; a non-quantized leaf has the same shape as an interior tuple.
     */
    public static ITypeTraits[] leafTypeTraits(boolean quantized) {
        return quantized ? LEAF_QUANTIZED_SCHEMA : BASE_SCHEMA;
    }

    /** {@code [cid, centroid, pointer]} — shared by interior and non-quantized leaf tuples. */
    private static ITypeTraits[] buildBaseSchema() {
        ITypeTraits[] schema = new ITypeTraits[VTreeStaticTupleConstants.EMBEDDING_FIELD + 2];
        schema[VTreeStaticTupleConstants.CENTROID_ID_FIELD] = IntegerPointable.TYPE_TRAITS;
        schema[VTreeStaticTupleConstants.EMBEDDING_FIELD] = VarLengthTypeTrait.INSTANCE;
        schema[schema.length - 1] = IntegerPointable.TYPE_TRAITS;
        return schema;
    }

    private static ITypeTraits[] buildLeafQuantizedSchema() {
        // Length is (last non-pointer field index) + 1 for that field + 1 for the trailing pointer.
        ITypeTraits[] schema = new ITypeTraits[VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD + 2];
        schema[VTreeStaticTupleConstants.CENTROID_ID_FIELD] = IntegerPointable.TYPE_TRAITS;
        schema[VTreeStaticTupleConstants.EMBEDDING_FIELD] = VarLengthTypeTrait.INSTANCE;
        schema[VTreeStaticTupleConstants.LEAF_QUANTIZED_BYTES_FIELD] = VarLengthTypeTrait.INSTANCE;
        schema[VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD] = VarLengthTypeTrait.INSTANCE;
        schema[schema.length - 1] = IntegerPointable.TYPE_TRAITS;
        return schema;
    }
}
