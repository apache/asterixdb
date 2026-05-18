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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Interface for VTree data frames. Data frames contain vector records sorted by
 * {@code distance_to_centroid} ascending; the exact tuple shape depends on whether the index is
 * quantized:
 * <ul>
 *   <li>Non-quantized: {@code <distance_to_centroid, centroid_id, PK, include_fields>}</li>
 *   <li>Quantized:     {@code <distance_to_centroid, centroid_id, quantized_distance,
 *       quantized_embedding, PK, include_fields>} (the default in this build, since
 *       quantization is enforced at index creation; pkStartField=4 vs 2)</li>
 * </ul>
 * <p>
 * Not thread-safe: an instance wraps one pinned page and is confined to a single operation context.
 */
public interface IVTreeDataFrame extends IVTreeFrame {

    /** Sets this page's forward chain pointer; {@code -1} marks end-of-chain. */
    void setNextPage(int nextPage);

    /** Returns the forward chain pointer, or {@code -1} if this is the last page in the chain. */
    int getNextPage();

    /** Returns the {@code distance_to_centroid} stored in field 0 of the tuple at {@code tupleIndex}. */
    double getDistanceToCentroid(int tupleIndex) throws HyracksDataException;

    /**
     * Inserts {@code tuple} at slot {@code tupleIndex}, shifting existing tuples right. The caller
     * must supply an index that preserves the {@code distance_to_centroid}-ascending ordering (see
     * {@link #findInsertPosition(double)}) and must have ensured the page has room.
     */
    @Override
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Splits this (full) page, moving the upper half of its tuples into {@code rightFrame} and then
     * inserting {@code tuple} into whichever half keeps the ascending order. The insertion index is
     * recomputed from the tuple's distance in the chosen half. On return both halves remain sorted by
     * {@code distance_to_centroid}.
     */
    void split(IVTreeDataFrame rightFrame, ITupleReference tuple) throws HyracksDataException;

    /**
     * Returns the slot at which a tuple with the given {@code distance} would be inserted to keep
     * the page sorted by {@code distance_to_centroid} ascending: the index of the first tuple whose
     * distance is strictly {@code > distance} (so equal distances are inserted after existing ones),
     * or the tuple count if none.
     */

    int findInsertPosition(double distance) throws HyracksDataException;
}
