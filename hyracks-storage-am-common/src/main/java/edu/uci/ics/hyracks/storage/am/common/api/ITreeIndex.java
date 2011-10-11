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
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

/**
 * Interface describing the operations of tree-based index structures. Indexes
 * implementing this interface can easily reuse the tree index operators for
 * dataflow. We assume that indexes store tuples with a fixed number of fields.
 */
public interface ITreeIndex {

	/**
	 * Initializes the persistent state of a tree index, e.g., the root page,
	 * and metadata pages.
	 * 
	 * @param indexFileId
	 *            The file id to use for this index.
	 * @param leafFrame
	 *            Leaf frame to use for initializing the root.
	 * @param metaFrame
	 *            Metadata frame to use for initializing metadata information.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void create(int indexFileId, ITreeIndexFrame leafFrame,
			ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;

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
	 * Creates an operation context for a given index operation
	 * (insert/delete/update/search/diskorderscan). An operation context
	 * maintains a cache of objects used during the traversal of the tree index.
	 * The context is intended to be reused for multiple subsequent operations
	 * by the same user/thread.
	 * 
	 * @param indexOp
	 *            Intended index operation.
	 * @param leafFrame
	 *            Leaf frame for interpreting leaf pages.
	 * @param interiorFrame
	 *            Interior frame for interpreting interior pages.
	 * @param metaFrame
	 *            Metadata frame for interpreting metadata pages.
	 * @returns IITreeIndexOpContext Operation context for the desired index
	 *          operation.
	 */
	public IIndexOpContext createOpContext(IndexOp op,
			ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
			ITreeIndexMetaDataFrame metaFrame);
	
	/**
	 * Inserts the given tuple into the index using an existing operation
	 * context.
	 * 
	 * @param tuple
	 *            Tuple to be inserted.
	 * @param ictx
	 *            Existing operation context.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If an index-specific constraint is violated, e.g., the key
	 *             already exists.
	 */
	public void insert(ITupleReference tuple, IIndexOpContext ictx)
			throws HyracksDataException, TreeIndexException;

	/**
	 * Updates the tuple in the index matching the given tuple with the new
	 * contents in the given tuple.
	 * 
	 * @param tuple
	 *            Tuple whose match in the index is to be update with the given
	 *            tuples contents.
	 * @param ictx
	 *            Existing operation context.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If there is no matching tuple in the index.
	 */
	public void update(ITupleReference tuple, IIndexOpContext ictx)
			throws HyracksDataException, TreeIndexException;

	/**
	 * Deletes the tuple in the index matching the given tuple.
	 * 
	 * @param tuple
	 *            Tuple to be deleted.
	 * @param ictx
	 *            Existing operation context.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If there is no matching tuple in the index.
	 */
	public void delete(ITupleReference tuple, IIndexOpContext ictx)
			throws HyracksDataException, TreeIndexException;

	/**
	 * Prepares the index for bulk loading, returning a bulk load context. The
	 * index must be empty for bulk loading to be possible.
	 * 
	 * @param fillFactor
	 *            Desired fill factor in [0, 1.0].
	 * @param leafFrame
	 *            Leaf frame for filling leaf pages.
	 * @param interiorFrame
	 *            Interior frame for filling interior pages.
	 * @param metaFrame
	 *            Metadata frame for accessing metadata pages.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 * @throws TreeIndexException
	 *             If the tree is not empty.
	 * @returns A new context for bulk loading, required for appending tuples.
	 */
	public IIndexBulkLoadContext beginBulkLoad(float fillFactor,
			ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
			ITreeIndexMetaDataFrame metaFrame) throws TreeIndexException,
			HyracksDataException;

	/**
	 * Append a tuple to the index in the context of a bulk load.
	 * 
	 * @param tuple
	 *            Tuple to be inserted.
	 * @param ictx
	 *            Existing bulk load context.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void bulkLoadAddTuple(ITupleReference tuple,
			IIndexBulkLoadContext ictx) throws HyracksDataException;

	/**
	 * Finalize the bulk loading operation in the given context.
	 * 
	 * @param ictx
	 *            Existing bulk load context to be finalized.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void endBulkLoad(IIndexBulkLoadContext ictx)
			throws HyracksDataException;

	/**
	 * Open the given cursor for a disk-order scan, positioning the cursor to
	 * the first leaf tuple.
	 * 
	 * @param leafFrame
	 *            Leaf frame for interpreting leaf pages.
	 * @param metaFrame
	 *            Metadata frame for interpreting metadata pages.
	 * @param ictx
	 *            Existing operation context.
	 * @throws HyracksDataException
	 *             If the BufferCache throws while un/pinning or un/latching.
	 */
	public void diskOrderScan(ITreeIndexCursor icursor,
			ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame,
			IIndexOpContext ictx) throws HyracksDataException;
	
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
