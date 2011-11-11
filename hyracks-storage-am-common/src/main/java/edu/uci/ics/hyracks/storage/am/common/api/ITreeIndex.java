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
 * Interface describing the operations of tree-based index structures. Indexes
 * implementing this interface can easily reuse the tree index operators for
 * dataflow. We assume that indexes store tuples with a fixed number of fields.
 * Users must perform operations on an ITreeIndex via an ITreeIndexAccessor.
 */
public interface ITreeIndex {

	/**
	 * Initializes the persistent state of a tree index, e.g., the root page,
	 * and metadata pages.
	 * 
	 * @param indexFileId
	 *            The file id to use for this index.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void create(int indexFileId) throws HyracksDataException;

	/**
	 * Opens the tree index backed by the given file id.
	 * 
	 * @param indexFileId
	 *            The file id backing this index.
	 */
	public void open(int indexFileId);

	/**
	 * Closes the tree index.
	 */
	public void close();

	/**
	 * Creates an index accessor for performing operations on this index.
	 * (insert/delete/update/search/diskorderscan). An ITreeIndexAccessor is not
	 * thread safe, but different ITreeIndexAccessors can concurrently operate
	 * on the same ITreeIndex
	 * 
	 * @returns ITreeIndexAccessor A tree index accessor for this tree.
	 */
	public ITreeIndexAccessor createAccessor();

	/**
	 * Prepares the index for bulk loading, returning a bulk load context. The
	 * index must be empty for bulk loading to be possible.
	 * 
	 * @param fillFactor
	 *            Desired fill factor in [0, 1.0].
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If the tree is not empty.
	 * @throws PageAllocationException
	 * @returns A new context for bulk loading, required for appending tuples.
	 */
	public IIndexBulkLoadContext beginBulkLoad(float fillFactor)
			throws TreeIndexException, HyracksDataException,
			PageAllocationException;

	/**
	 * Append a tuple to the index in the context of a bulk load.
	 * 
	 * @param tuple
	 *            Tuple to be inserted.
	 * @param ictx
	 *            Existing bulk load context.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws PageAllocationException
	 */
	public void bulkLoadAddTuple(ITupleReference tuple,
			IIndexBulkLoadContext ictx) throws HyracksDataException,
			PageAllocationException;

	/**
	 * Finalize the bulk loading operation in the given context.
	 * 
	 * @param ictx
	 *            Existing bulk load context to be finalized.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws PageAllocationException
	 */
	public void endBulkLoad(IIndexBulkLoadContext ictx)
			throws HyracksDataException, PageAllocationException;

	/**
	 * @return The index's leaf frame factory.
	 */
	public ITreeIndexFrameFactory getLeafFrameFactory();

	/**
	 * @return The index's interior frame factory.
	 */
	public ITreeIndexFrameFactory getInteriorFrameFactory();

	/**
	 * @return The index's free page manager.
	 */
	public IFreePageManager getFreePageManager();

	/**
	 * @return The number of fields tuples of this index have.
	 */
	public int getFieldCount();

	/**
	 * @return The current root page id of this index.
	 */
	public int getRootPageId();

	/**
	 * @return An enum of the concrete type of this index.
	 */
	public IndexType getIndexType();
}
