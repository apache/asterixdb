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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Interface describing the operations common to all index structures. Indexes
 * implementing this interface can easily reuse existing index operators for
 * dataflow. Users must perform operations on an IIndex via an IIndexAccessor.
 */
public interface IIndex {
    /**
     * Initializes the persistent state of an index, e.g., the root page, and
     * metadata pages.
     * 
     * @param indexFileId
     *            The file id to use for this index.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void create(int indexFileId) throws HyracksDataException;

    /**
     * Opens the index backed by the given file id.
     * 
     * @param indexFileId
     *            The file id backing this index.
     */
    public void open(int indexFileId) throws HyracksDataException;

    /**
     * Closes the index.
     */
    public void close() throws HyracksDataException;

    /**
     * Creates an index accessor for performing operations on this index.
     * (insert/delete/update/search/diskorderscan). An IIndexAccessor is not
     * thread safe, but different IIndexAccessors can concurrently operate
     * on the same IIndex
     * 
     * @returns IIndexAccessor An accessor for this tree.
     * @param modificationCallback TODO
     * @param searchCallback TODO
     */
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback);

    /**
     * Prepares the index for bulk loading, returning a bulk load context. The
     * index may require to be empty for bulk loading.
     * 
     * @param fillFactor
     *            Desired fill factor in [0, 1.0].
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     * @throws IndexException
     *             For example, if the index was already loaded and only
     *             supports a single load.
     * @returns A new context for bulk loading, required for appending tuples.
     */
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws IndexException, HyracksDataException;

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
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException;

    /**
     * Finalize the bulk loading operation in the given context.
     * 
     * @param ictx
     *            Existing bulk load context to be finalized.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException;

    /**
     * @return BufferCache underlying this index.
     */
    public IBufferCache getBufferCache();

    /**
     * @return An enum of the concrete type of this index.
     */
    public IndexType getIndexType();
}
