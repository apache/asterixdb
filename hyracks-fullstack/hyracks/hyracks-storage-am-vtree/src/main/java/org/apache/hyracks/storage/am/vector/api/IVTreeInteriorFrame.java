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

/**
 * Interface for VTree interior/root frames.
 * Interior frames contain cluster entries: <cid, full_precision_centroid, pointer_to_clustered_page>
 * <p>
 * Not thread-safe: an instance wraps one pinned page and is confined to a single operation context.
 */
public interface IVTreeInteriorFrame extends IVTreeFrame {

    /** Returns the child (next-level) page id pointed to by the entry at {@code tupleIndex}. */
    int getChildPageId(int tupleIndex) throws HyracksDataException;

    /** Sets the child page pointer for the entry at {@code tupleIndex}. */
    void setChildPageId(int tupleIndex, int childPageId) throws HyracksDataException;

    /** Sets the same-level overflow chain pointer; {@code -1} marks end-of-chain. */
    void setNextPage(int nextPageId);

    /** Returns the same-level overflow chain pointer, or {@code -1} if this is the last page. */
    int getNextPage();

    /** Sets the header bit indicating this page continues onto a {@link #getNextPage()} overflow page. */
    void setOverflowFlagBit(boolean overflowFlag);

    /** Returns whether this page has an overflow continuation (see {@link #getNextPage()}). */
    boolean getOverflowFlagBit();
}
