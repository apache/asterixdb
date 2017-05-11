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

package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IIndexCursor {
    /**
     * Opens the cursor
     * if open succeeds, close must be called.
     *
     * @param initialState
     * @param searchPred
     * @throws HyracksDataException
     */
    void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException;

    /**
     * True if the cursor has a next value
     *
     * @return
     * @throws HyracksDataException
     */
    boolean hasNext() throws HyracksDataException;

    /**
     * Moves the cursor to the next value
     *
     * @throws HyracksDataException
     */
    void next() throws HyracksDataException;

    /**
     * Closes the cursor
     *
     * @throws HyracksDataException
     */
    void close() throws HyracksDataException;

    /**
     * Reset the cursor to be reused
     *
     * @throws HyracksDataException
     * @throws IndexException
     */
    void reset() throws HyracksDataException;

    /**
     * @return the tuple pointed to by the cursor
     */
    ITupleReference getTuple();

    /**
     * @return the min tuple of the current index's filter
     */
    ITupleReference getFilterMinTuple();

    /**
     *
     * @return the max tuple of the current index's filter
     */
    ITupleReference getFilterMaxTuple();
}
