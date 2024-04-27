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

import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;

/**
 * Extends {@link IBufferCacheWriteContext} to provide columnar-specific {@link IBufferCache} operations
 */
public interface IColumnWriteContext extends IBufferCacheWriteContext {
    /**
     * Signal a column will be written
     *
     * @param columnIndex column index that will be written
     * @param overlapping whether the first page is shared (overlapped) with the previous column
     */
    void startWritingColumn(int columnIndex, boolean overlapping);

    /**
     * Report the end of the writing operation of a column
     *
     * @param columnIndex of the column was written
     * @param size        the actual size of the column
     */
    void endWritingColumn(int columnIndex, int size);

    /**
     * Indicates that all columns were persisted
     */
    void columnsPersisted();

    /**
     * Closing the context and report any required statistics
     */
    void close();
}
