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
package org.apache.hyracks.storage.am.lsm.invertedindex.api;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexCursorStats;

public interface IInPlaceInvertedIndex extends IInvertedIndex {
    /**
     * Creates an inverted list cursor.
     *
     * @throws HyracksDataException
     */
    InvertedListCursor createInvertedListCursor(IHyracksTaskContext ctx) throws HyracksDataException;

    /**
     * Creates an inverted-list-range-search cursor. This cursor is mainly used to conduct
     * a full scan of an inverted list during a storage-component-merge operation.
     *
     * @throws HyracksDataException
     */
    InvertedListCursor createInvertedListRangeSearchCursor(IIndexCursorStats stats) throws HyracksDataException;

    /**
     * Opens an inverted list cursor
     *
     * @param listCursor
     *            the cursor to open
     * @param searchKey
     *            the search key to use for the operation
     * @param ictx
     *            the operation context under which the cursor is to be open
     * @throws HyracksDataException
     */
    void openInvertedListCursor(InvertedListCursor listCursor, ITupleReference searchKey, IIndexOperationContext ictx)
            throws HyracksDataException;
}
