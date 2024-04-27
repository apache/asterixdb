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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeBulkloader;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.cloud.IIndexDiskCacheManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;

/**
 * Extends {@link IIndexDiskCacheManager} to provide columnar-specific local disk caching operations
 */
public interface IColumnIndexDiskCacheManager extends IIndexDiskCacheManager {
    /**
     * By activating, an initial statistics about the stored columns will be gathered
     *
     * @param numberOfColumns number of columns
     * @param diskComponents  current disk components
     * @param bufferCache     buffer cache
     */
    void activate(int numberOfColumns, List<ILSMDiskComponent> diskComponents, IBufferCache bufferCache)
            throws HyracksDataException;

    /**
     * Create {@link IBufferCacheWriteContext} context for {@link ColumnBTreeBulkloader}
     *
     * @param numberOfColumns a hint of the known current number of columns
     * @param operationType   operation type
     * @return writer context
     */
    IColumnWriteContext createWriteContext(int numberOfColumns, LSMIOOperationType operationType);

    /**
     * Create {@link IBufferCacheReadContext} for {@link ColumnBTreeRangeSearchCursor}
     *
     * @param projectionInfo projected columns information
     * @return reader context
     */
    IColumnReadContext createReadContext(IColumnProjectionInfo projectionInfo);
}
