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
package org.apache.hyracks.storage.am.vector.api;

/**
 * Field layout of VTree static-structure (interior and leaf) tuples.
 * <p>
 * The authoritative on-page binary layout is the {@code ITypeTraits[]} declared in
 * {@code VTreeInteriorFrameFactory} and {@code VTreeLeafFrameFactory}; the field
 * <em>content</em> is encoded by the producer
 * ({@code VTreeStaticStructureCreatorOperatorDescriptor}). This class documents that
 * shared schema in one place so readers and the builder agree on field positions
 * without restating per-field serializers at each call site.
 * <p>
 * A stored tuple is the input centroid tuple with a trailing pointer field appended by
 * {@code VTreeStaticStructureBuilder.createEntryTuple} — the navigation child pointer for
 * interior tuples, the metadata-page pointer for leaf tuples. The pointer is always LAST,
 * so {@code getMetadataPagePointer()} resolves it via {@code getFieldCount() - 1}
 * regardless of which optional leaf fields precede it.
 *
 * <pre>
 *   Interior:                         [ cid, embedding, childPagePtr ]
 *   Leaf, non-quantized (tests):      [ cid, embedding, metadataPtr ]
 *   Leaf, quantized:                  [ cid, embedding, quantizedBytes, metadataPtr ]
 *   Leaf, quantized + graph neighbors:[ cid, embedding, quantizedBytes, neighborList, metadataPtr ]
 * </pre>
 *
 * Interior vs leaf is decided by build level (not by field count), since an interior tuple
 * and a non-quantized leaf tuple are both 3 fields once the pointer is appended.
 */
public final class VTreeStaticTupleConstants {

    private VTreeStaticTupleConstants() {
    }

    /** Centroid id field index (shared by interior and all leaf layouts). */
    public static final int CENTROID_ID_FIELD = 0;

    /** Full-precision centroid embedding field index (shared by interior and all leaf layouts). */
    public static final int EMBEDDING_FIELD = 1;

    /** Quantized centroid bytes field index in quantized leaf tuples. */
    public static final int LEAF_QUANTIZED_BYTES_FIELD = 2;

    /** Graph-neighbor list field index in quantized-with-neighbors leaf tuples. */
    public static final int LEAF_NEIGHBOR_LIST_FIELD = 3;
}
