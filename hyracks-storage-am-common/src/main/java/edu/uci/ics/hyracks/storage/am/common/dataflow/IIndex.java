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
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
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
     * @param fileReference
     *            The file id to use for this index.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void create(FileReference file) throws HyracksDataException;

    /**
     * Opens the index backed by the given file id.
     * 
     * @param fileReference
     *            The file id backing this index.
     */
    public void open(FileReference file) throws HyracksDataException;

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
     * @param modificationCallback
     *            TODO
     * @param searchCallback
     *            TODO
     */
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback);

    /**
     * @param fillFactor
     *            TODO
     * @throws IndexException
     *             TODO
     */
    public IIndexBulkLoader createBulkLoader(float fillFactor) throws IndexException;

    /**
     * @return BufferCache underlying this index.
     */
    public IBufferCache getBufferCache();

    /**
     * @return An enum of the concrete type of this index.
     */
    public IndexType getIndexType();
}
