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
package org.apache.hyracks.storage.am.lsm.btree.column.api;

import java.nio.ByteBuffer;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * A proxy to call {@link IBufferCache} operations. Each column should have its own buffer provider
 */
public interface IColumnBufferProvider {
    /**
     * Calling this method would pin all the pages of the requested columns from the buffer cache
     *
     * @param frame the frame for Page0
     */
    void reset(ColumnBTreeReadLeafFrame frame) throws HyracksDataException;

    /**
     * Return all the pages for a column
     *
     * @param buffers queue for all pages of a column
     */
    void readAll(Queue<ByteBuffer> buffers) throws HyracksDataException;

    /**
     * Release all the column pages (i.e., unpin all column pages)
     */
    void releaseAll() throws HyracksDataException;

    /**
     * @return a buffer of a column (in case there is only a single page for a column)
     */
    ByteBuffer getBuffer();

    /**
     * @return the actual length (in bytes) for all the column's pages
     */
    int getLength();

    /**
     * @return the column index
     */
    int getColumnIndex();
}
