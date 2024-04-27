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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;

/**
 * Extends {@link IBufferCacheReadContext} to provide columnar-specific {@link IBufferCache} operations
 */
public interface IColumnReadContext extends IBufferCacheReadContext {
    /**
     * Pin the next Mega-leaf node
     * Notes:
     * - This method is responsible for unpinning the previous pageZero of the leafFrame as well as any other pages
     * - This method may prefetch the next mega-leaf node of the newly pinned mega-leaf node
     *
     * @param leafFrame   leaf frame used
     * @param bufferCache buffer cache
     * @param fileId      file ID
     * @return the pageZero of the next mega-leaf nodes
     */
    ICachedPage pinNext(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException;

    /**
     * Prepare the columns' pages
     * Notes:
     * - Calling this method does not guarantee the columns' pages will be pinned. Thus, it is the
     * {@link IColumnTupleIterator} responsibility to pin the required pages of the requested columns
     * - Calling this method may result in reading pages from the cloud and also persisting them in
     * the local drive (only in the cloud deployment)
     *
     * @param leafFrame   leaf frame used
     * @param bufferCache buffer cache
     * @param fileId      file ID
     */
    void prepareColumns(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException;

    /**
     * Release all pinned pages
     *
     * @param bufferCache buffer cache
     */
    void release(IBufferCache bufferCache) throws HyracksDataException;

    /**
     * Closing this context will unpin all pinned (and prefetched pages if any)
     *
     * @param bufferCache buffer cache
     */
    void close(IBufferCache bufferCache) throws HyracksDataException;
}
