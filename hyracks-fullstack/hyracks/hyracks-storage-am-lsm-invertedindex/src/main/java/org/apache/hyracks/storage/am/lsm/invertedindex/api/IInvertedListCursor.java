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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * A cursor that reads an inverted list.
 */
public interface IInvertedListCursor extends IIndexCursor, Comparable<IInvertedListCursor> {

    /**
     * Conducts any operation that is required before loading pages.
     */
    void prepareLoadPages() throws HyracksDataException;

    /**
     * Loads one or more pages to memory.
     */
    void loadPages() throws HyracksDataException;

    /**
     * Unloads currently loaded pages in the memory.
     */
    void unloadPages() throws HyracksDataException;

    /**
     * Gets the cardinality of elements in the cursor.
     */
    int size() throws HyracksDataException;

    /**
     * Checks whether the given tuple is contained in the cursor.
     *
     * Note that this method is used when merging two sorted list, that means we can move the internal cursor of a list
     * in one-direction: the cursor won't go back.
     * A better name of this method might be moveCursorForwardToCheckContainsKey()
     */
    boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) throws HyracksDataException;

    /**
     * Prints all elements in the cursor (debug method).
     */
    @SuppressWarnings("rawtypes")
    String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException;

    /**
     * Prints the current element in the cursor (debug method).
     */
    @SuppressWarnings("rawtypes")
    String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException;
}