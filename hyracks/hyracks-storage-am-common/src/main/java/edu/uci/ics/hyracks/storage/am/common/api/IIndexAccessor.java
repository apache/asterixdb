/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Client handle for performing operations (insert/delete/update/search) on an
 * IIndex. An IIndexAccessor is not thread safe, but different IIndexAccessors
 * can concurrently operate on the same IIndex (i.e., the IIndex must allow
 * concurrent operations).
 */
public interface IIndexAccessor {
    /**
     * Inserts the given tuple.
     * 
     * @param tuple
     *            Tuple to be inserted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If an index-specific constraint is violated, e.g., the key
     *             already exists.
     */
    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * Updates the tuple in the index matching the given tuple with the new
     * contents in the given tuple.
     * 
     * @param tuple
     *            Tuple whose match in the index is to be update with the given
     *            tuples contents.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If there is no matching tuple in the index.
     */
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * Deletes the tuple in the index matching the given tuple.
     * 
     * @param tuple
     *            Tuple to be deleted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If there is no matching tuple in the index.
     */
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException;

    /**
     * This operation is only supported by indexes with the notion of a unique key.
     * If tuple's key already exists, then this operation performs an update.
     * Otherwise, it performs an insert.
     * 
     * @param tuple
     *            Tuple to be deleted.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             If there is no matching tuple in the index.
     * 
     */
    public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException;
    
    /**
     * Creates a cursor appropriate for passing into search().
     * 
     */
    public IIndexCursor createSearchCursor();

    /**
     * Open the given cursor for an index search using the given predicate as
     * search condition.
     * 
     * @param icursor
     *            Cursor over the index entries satisfying searchPred.
     * @param searchPred
     *            Search condition.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     */
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException;
}
