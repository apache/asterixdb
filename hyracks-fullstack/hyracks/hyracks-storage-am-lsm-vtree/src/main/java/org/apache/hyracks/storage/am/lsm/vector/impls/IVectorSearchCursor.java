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
package org.apache.hyracks.storage.am.lsm.vector.impls;

/**
 * Capability interface for vector ANN search cursors that expose the per-candidate distance
 * to the query vector (the {@code D(q,x)} value the cursor used during its top-K search).
 *
 * <p>Required by the index-only ANN plan, where the operator emits {@code [pk..., dist]} to its
 * output frame and the downstream {@code ORDER BY $$dist → LIMIT k} avoids the primary BTree
 * lookup and the rerank ASSIGN. The cursor's emitted tuple still carries only the index's
 * on-disk fields (where field 0 is the distance-to-centroid, not the query distance), so the
 * operator obtains the per-candidate {@code D(q,x)} via {@link #getCurrentDistance()} and
 * appends it as a separate ADOUBLE field.
 *
 * <p>{@link #getCurrentDistance()} returns the distance for the tuple currently held by the
 * cursor (the one that {@code cursor.getTuple()} would return). It must be called between
 * {@code cursor.next()} and the next {@code cursor.next()} or {@code cursor.close()}.
 *
 * <p>Implementing this interface <em>is</em> the capability signal: a cursor implements it iff it computes
 * a per-candidate query distance. Cursors that only order by distance-to-centroid (the streaming non-pruned
 * {@code LSMVTreeSearchCursor}, used for merges/full scans/tests) simply do not implement it, so the
 * index-only ANN plan detects the unsupported case with a plain {@code instanceof} check. A NaN from
 * {@link #getCurrentDistance()} is therefore never a "distance unavailable" signal; it is a genuine value
 * (e.g. the cosine distance of a zero-magnitude vector).
 */
public interface IVectorSearchCursor {

    /**
     * @return the distance from the query vector to the candidate vector for the current tuple. This may be
     *         {@link Double#NaN} as a genuine value (e.g. the cosine distance of a zero-magnitude vector).
     */
    double getCurrentDistance();
}
