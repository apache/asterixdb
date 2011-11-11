/*
 * Copyright 2009-2010 by The Regents of the University of California
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
 * Client handle for performing operations
 * (insert/delete/update/search/diskorderscan) on an ITreeIndex. An
 * ITreeIndexAccessor is not thread safe, but different ITreeIndexAccessors can
 * concurrently operate on the same ITreeIndex (i.e., the ITreeIndex must allow
 * concurrent operations).
 */
public interface ITreeIndexAccessor {
	/**
	 * Inserts the given tuple.
	 * 
	 * @param tuple
	 *            Tuple to be inserted.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If an index-specific constraint is violated, e.g., the key
	 *             already exists.
	 * @throws PageAllocationException
	 */
	public void insert(ITupleReference tuple) throws HyracksDataException,
			TreeIndexException, PageAllocationException;

	/**
	 * Updates the tuple in the index matching the given tuple with the new
	 * contents in the given tuple.
	 * 
	 * @param tuple
	 *            Tuple whose match in the index is to be update with the given
	 *            tuples contents.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If there is no matching tuple in the index.
	 * @throws PageAllocationException
	 */
	public void update(ITupleReference tuple) throws HyracksDataException,
			TreeIndexException, PageAllocationException;

	/**
	 * Deletes the tuple in the index matching the given tuple.
	 * 
	 * @param tuple
	 *            Tuple to be deleted.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If there is no matching tuple in the index.
	 * @throws PageAllocationException
	 */
	public void delete(ITupleReference tuple) throws HyracksDataException,
			TreeIndexException, PageAllocationException;

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
	 * @throws TreeIndexException
	 * @throws PageAllocationException
	 */
	public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred)
			throws HyracksDataException, TreeIndexException, PageAllocationException;

	/**
	 * Open the given cursor for a disk-order scan, positioning the cursor to
	 * the first leaf tuple.
	 * 
	 * @param icursor
	 *            Cursor to be opened for disk-order scanning.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void diskOrderScan(ITreeIndexCursor cursor)
			throws HyracksDataException;
}
