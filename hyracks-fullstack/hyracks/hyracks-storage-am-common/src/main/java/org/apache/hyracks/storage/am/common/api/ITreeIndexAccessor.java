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

package org.apache.hyracks.storage.am.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.IIndexAccessor;

/**
 * Client handle for performing operations
 * (insert/delete/update/search/diskorderscan) on an ITreeIndex. An
 * ITreeIndexAccessor is not thread safe, but different ITreeIndexAccessors can
 * concurrently operate on the same ITreeIndex (i.e., the ITreeIndex must allow
 * concurrent operations).
 */
public interface ITreeIndexAccessor extends IIndexAccessor {
    /**
     * Creates a cursor appropriate for passing into diskOrderScan().
     *
     */
    public ITreeIndexCursor createDiskOrderScanCursor();

    /**
     * Open the given cursor for a disk-order scan, positioning the cursor to
     * the first leaf tuple.
     * If this method returns successfully, the cursor is open.
     * Otherwise, it was not open
     *
     * @param icursor
     *            Cursor to be opened for disk-order scanning.
     * @throws HyracksDataException
     *             If the BufferCache throws while un/pinning or un/latching.
     */
    public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException;
}
