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
 * Interface for VTree data frames. Tuples are kept sorted by the ordering key, whose fields the caller
 * names when the frame is built; field 0 is the {@code distance_to_centroid} this frame reports. See
 * {@code VTreeDataTupleAccessor} for the tuple shape.
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
     * Inserts {@code tuple} at slot {@code tupleIndex}, shifting existing tuples right. The caller must
     * supply an index that preserves the key-ascending ordering (see {@link #findInsertPosition}) and
     * must have ensured the page has room.
     */
    @Override
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Splits this (full) page, moving the upper half of its tuples into {@code rightFrame} and then
     * inserting {@code tuple} into whichever half keeps the ascending order. The insertion index is
     * recomputed from the tuple's full key in the chosen half. On return both halves remain sorted.
     */
    void split(IVTreeDataFrame rightFrame, ITupleReference tuple) throws HyracksDataException;

    /**
     * Returns the slot at which {@code key} belongs, that is the index of the first stored tuple whose
     * key is {@code >=} it, or the tuple count if none. The ordering key extends past the distance so
     * that a component's tuple stream is ordered by the same key the LSM merge cursor reconciles on,
     * which keeps versions of one record adjacent when unrelated records tie on distance.
     *
     * @param key the search key, in key layout: the fields named by {@code comparatorFields}, in order
     */
    int findInsertPosition(ITupleReference key) throws HyracksDataException;

    /**
     * Projects a stored-layout tuple onto the ordering key, so a caller holding a data tuple can obtain
     * a search key without knowing which fields form it. The view is valid until the next call.
     */
    ITupleReference keyOf(ITupleReference storedTuple);

    /**
     * The ordering key of the tuple at {@code tupleIndex}. The view is valid until the next call on
     * this frame; a caller keeping it past that must copy it.
     */
    ITupleReference keyAt(int tupleIndex) throws HyracksDataException;
}
