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
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;

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

    /**
     * Pin the page so it can't be evicted from the buffer cache...
     *
     * @param dpid
     *            page id is a unique id that is a combination of file id and page id
     * @param newPage
     *            whether this page is expected to be new.
     *            NOTE: undefined:
     *            -- what if the flag is true but the page exists?
     *            -- what if the flag is false but the page doesn't exist
     * @return the pinned page
     * @throws HyracksDataException
     */
    ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException;

    /**
     * Unpin a pinned page so its buffer can be recycled
     *
     * @param page
     *            the page
     * @throws HyracksDataException
     */
    void unpin(ICachedPage page) throws HyracksDataException;

    /**
     * Flush the page if it is dirty
     *
     * @param page
     *            the page to flush
     * @throws HyracksDataException
     */
    void flush(ICachedPage page) throws HyracksDataException;

    /**
     * Force bits that have been already flushed to disk
     * This method doesn't flush all dirty pages to disk but simply calls the sync method on the filesystem api
     *
     * @param fileId
     *            the file id
     * @param metadata
     *            whether metadata should be synced as well
     * @throws HyracksDataException
     */
    void force(int fileId, boolean metadata) throws HyracksDataException;

    /**
     * Take a page such that no one else has access to it
     *
     * @param dpid
     *            the unique (fileId,pageId)
     * @return the confiscated page or null if no page is available
     * @throws HyracksDataException
     */
    ICachedPage confiscatePage(long dpid) throws HyracksDataException;

    /**
     *
     * @return the confiscated page or null if no page is available
     * @throws HyracksDataException
     */
    /**
     * Take a large page such that no one else has access to it
     *
     * @param dpid
     *            the unique (fileId,pageId)
     * @param multiplier
     *            how many multiples of the original page size
     * @param extraBlockPageId
     *            the page id where the large block comes from
     * @return
     *         the confiscated page or null if a large page couldn't be found
     * @throws HyracksDataException
     */
    ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId) throws HyracksDataException;

    /**
     * Return and re-insert a confiscated page
     *
     * @param page
     *            the confiscated page
     */
    void returnPage(ICachedPage page);

    /**
     * Return a confiscated page
     *
     * @param page
     *            the confiscated page
     * @param reinsert
     *            if true, return the page to the cache, otherwise, destroy
     */
    void returnPage(ICachedPage page, boolean reinsert);

    /**
     * Get the standard page size
     *
     * @return the size in bytes
     */
    int getPageSize();

    /**
     * Get the standard page size with header if any
     *
     * @return the sum of page size and header size in bytes
     */
    int getPageSizeWithHeader();

    /**
     * @return the maximum allowed pages in this buffer cahce
     */
    int getPageBudget();

    /**
     * Get the number of pages used for a file
     *
     * @param fileId
     *            the file id
     * @return the number of pages used for the file
     * @throws HyracksDataException
     */
    int getNumPagesOfFile(int fileId) throws HyracksDataException;

    /**
     * Get the reference count for a file (num of open - num of close)
     *
     * @param fileId
     *            the file
     * @return the reference count
     */
    int getFileReferenceCount(int fileId);

    /**
     * Close the buffer cache, all of its files, and release the memory taken by it
     * The buffer cache is open upon successful instantiation and can't be re-opened
     *
     * @throws HyracksDataException
     */
    void close() throws HyracksDataException;

    /**
     * @return an instance of {@link IFIFOPageWriter} that can be used to write pages to the file
     */
    IFIFOPageWriter createFIFOWriter(IPageWriteCallback callback, IPageWriteFailureCallback failureCallback);

    // TODO: remove the replication out of the buffer cache interface
    /**
     * @return true if replication is enabled, false otherwise
     */
    boolean isReplicationEnabled();

    /**
     * @return the io replication manager
     */
    IIOReplicationManager getIOReplicationManager();

    /**
     * Deletes the file and recycle all of its pages without flushing them.
     *
     * ONLY call this if you absolutely, positively know this file has no dirty pages in the cache!
     * Bypasses the normal lifecycle of a file handle and evicts all references to it immediately.
     */
    void purgeHandle(int fileId) throws HyracksDataException;

    /**
     * Resize the page
     *
     * @param page
     *            the page to resize
     * @param multiplier
     *            how many multiples of the original page size
     * @param extraPageBlockHelper
     *            helper to determine the location of the resize block
     * @throws HyracksDataException
     */
    void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;

    /**
     * Close the file if open.
     *
     * @param fileRef
     * @throws HyracksDataException
     */
    void closeFileIfOpen(FileReference fileRef);

    /**
     * @return compressed page writer
     */
    default ICompressedPageWriter getCompressedPageWriter(int fileId) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support compressed pages");
    }

}
