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
package org.apache.hyracks.storage.common.buffercache;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;

public interface IBufferCache {

    long INVALID_DPID = -1L;
    int INVALID_PAGEID = -1;
    int RESERVED_HEADER_BYTES = 8;

    /**
     * Create file on disk
     *
     * @param fileRef
     *            the file to create
     * @return the file id
     * @throws HyracksDataException
     *             if the file already exists or attempt to create the file failed
     */
    int createFile(FileReference fileRef) throws HyracksDataException;

    /**
     * Open the file and register it (if not registered) with the file map manager
     *
     * @param fileRef
     *            the file to open
     * @return the file id
     * @throws HyracksDataException
     *             if the file doesn't exist or buffer cache failed to open the file
     */
    int openFile(FileReference fileRef) throws HyracksDataException;

    /**
     * Open the mapped file with the passed file id
     *
     * @param fileId
     *            the file id
     * @throws HyracksDataException
     *             if the file doesn't exist or buffer cache fails to open the file
     */
    void openFile(int fileId) throws HyracksDataException;

    /**
     * close the file
     *
     * @param fileId
     *            the file id
     * @throws HyracksDataException
     *             if file doesn't exist or is not open
     */
    void closeFile(int fileId) throws HyracksDataException;

    /**
     * delete the file from memory and disk
     *
     * @param fileId
     *            the file id
     * @throws HyracksDataException
     *             if the file doesn't exist or if a failure to delete takes place
     */
    void deleteFile(int fileId) throws HyracksDataException;

    /**
     * Delete from memory if registered and from disk
     *
     * @param file
     * @throws HyracksDataException
     */
    void deleteFile(FileReference file) throws HyracksDataException;

    ICachedPage tryPin(long dpid) throws HyracksDataException;

    ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException;

    void unpin(ICachedPage page) throws HyracksDataException;

    void flushDirtyPage(ICachedPage page) throws HyracksDataException;

    void adviseWontNeed(ICachedPage page);

    ICachedPage confiscatePage(long dpid) throws HyracksDataException;

    ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId) throws HyracksDataException;

    void returnPage(ICachedPage page);

    void returnPage(ICachedPage page, boolean reinsert);

    void force(int fileId, boolean metadata) throws HyracksDataException;

    int getPageSize();

    int getPageSizeWithHeader();

    int getNumPages();

    int getNumPagesOfFile(int fileId) throws HyracksDataException;

    int getFileReferenceCount(int fileId);

    void close() throws HyracksDataException;

    IFIFOPageQueue createFIFOQueue();

    void finishQueue();

    void setPageDiskId(ICachedPage page, long dpid);

    boolean isReplicationEnabled();

    IIOReplicationManager getIOReplicationManager();

    void purgeHandle(int fileId) throws HyracksDataException;

    void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;
}
