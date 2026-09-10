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
 * Interface for VTree metadata/directory frames.
 * Metadata frames contain entries: <max_distance, pointer_to_data_page>, sorted by
 * the key ascending.
 * <p>
 * Not thread-safe: an instance wraps one pinned page and is confined to a single operation context.
 */
public interface IVTreeMetadataFrame extends IVTreeFrame {

    /** Sets this directory page's forward chain pointer; {@code -1} marks end-of-chain. */
    void setNextPage(int nextPage);

    /** Returns the forward chain pointer, or {@code -1} if this is the last directory page. */
    int getNextPage();

    /**
     * Compares the separator at {@code tupleIndex} against {@code key}: negative if the page it points
     * at sorts entirely before the key, positive if after, zero if the key is that page's maximum.
     */
    int compareSeparatorToKey(int tupleIndex, ITupleReference key) throws HyracksDataException;

    /** Leftmost index at which an entry carrying {@code key} keeps the page key-ascending. */
    int findInsertPosition(ITupleReference key) throws HyracksDataException;

    /**
     * Replaces the separator at {@code tupleIndex} with one carrying {@code key}, keeping its position,
     * and reports whether it fitted. A full-width separator is variable length, so a replacement can be
     * wider than what it replaces; on {@code false} the page is untouched and the caller must make room.
     * The caller must have established that the position still holds.
     */
    boolean replaceSeparator(int tupleIndex, ITupleReference key, int dataPageId) throws HyracksDataException;

    /** Removes the separator at {@code tupleIndex}, freeing its bytes. */
    void deleteSeparator(int tupleIndex) throws HyracksDataException;

    /** Returns the data-page pointer of the entry at {@code tupleIndex}. */
    int getDataPagePointer(int tupleIndex) throws HyracksDataException;

    /**
     * Inserts {@code tuple} at slot {@code tupleIndex}, shifting existing entries right. The caller
     * must supply an index that preserves the key-ascending ordering.
     */
    void insert(ITupleReference tuple, int tupleIndex);
}
