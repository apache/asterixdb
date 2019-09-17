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
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public interface IPageManager {

    /**
     * Open metadata of an index file
     * Called on:
     * 1. Index Create
     * 2. Index Activate
     *
     * @param fileId
     *            The index file id
     * @throws HyracksDataException
     */
    void open(int fileId) throws HyracksDataException;

    /**
     * Close metadata of an index file. After this call returns, it is expected that the index pages have been written
     * to disk. Called on:
     * 1. Index Create
     * 2. Index Deactivate
     * 3. When we need to have a persisted state.
     *
     * Note: This method will not force indexes to disk driver using fsync
     *
     * @throws HyracksDataException
     */
    void close(IPageWriteFailureCallback failureCallback) throws HyracksDataException;

    /**
     * Create a metadata frame to be used for reading and writing to metadata pages
     *
     * @return a new metadata frame
     */
    ITreeIndexMetadataFrame createMetadataFrame();

    /**
     * Determines where the main metadata page is located in an index file
     *
     * @return The locaiton of the metadata page, or -1 if the file appears to be corrupt
     * @throws HyracksDataException
     */
    int getMetadataPageId() throws HyracksDataException;

    /**
     * Initializes the free page manager on an open index file.
     * This method is only called once per an index file at the beginning of creating the index.
     * It is expected that calling this method will initialize the state of an empty new index.
     *
     * @param leafFrameFactory
     * @param interiorFrameFactory
     * @param metaFrame
     *            A metadata frame used to wrap the raw page
     * @throws HyracksDataException
     */
    void init(ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory)
            throws HyracksDataException;

    /**
     * Get the location of a free page to use for index operations
     *
     * @param frame
     *            A metadata frame to use to wrap metadata pages
     * @return A page location, or -1 if no free page could be found or allocated
     * @throws HyracksDataException
     */
    int takePage(ITreeIndexMetadataFrame frame) throws HyracksDataException;

    /**
     * Get the location of a block of free pages to use for index operations
     * This is used for records that are larger than a normal page
     *
     * @param frame
     *            A metadata frame to use to wrap metadata pages
     * @return The starting page location, or -1 if a block of free pages could be found or allocated
     * @throws HyracksDataException
     */
    int takeBlock(ITreeIndexMetadataFrame frame, int count) throws HyracksDataException;

    /**
     * Add a page back to the pool of free pages within an index file
     *
     * @param frame
     *            A metadata frame to use to wrap metadata pages
     * @param page
     *            The page to be returned to the set of free pages
     * @throws HyracksDataException
     */
    void releasePage(ITreeIndexMetadataFrame frame, int page) throws HyracksDataException;

    /**
     * Add a page back to the pool of free pages within an index file
     *
     * @param frame
     *            A metadata frame to use to wrap metadata pages
     * @param page
     *            The start of the free pages block
     * @param count
     *            the number of regular sized pages in the free pages block
     * @throws HyracksDataException
     */
    void releaseBlock(ITreeIndexMetadataFrame frame, int page, int count) throws HyracksDataException;

    /**
     * Gets the highest page offset according to the metadata
     *
     * @param frame
     *            A metadata frame to use to wrap metadata pages
     * @return The locaiton of the highest offset page
     * @throws HyracksDataException
     */
    int getMaxPageId(ITreeIndexMetadataFrame frame) throws HyracksDataException;

    /**
     * Check whether the index is empty or not.
     *
     * @param frame
     *            interior frame
     * @param rootPage
     *            the current root page
     * @return true if empty, false otherwise
     * @throws HyracksDataException
     */
    boolean isEmpty(ITreeIndexFrame frame, int rootPage) throws HyracksDataException;

    /**
     * Get the root page of the id
     *
     * @return the root page
     * @throws HyracksDataException
     */
    int getRootPageId() throws HyracksDataException;

    /**
     * Get the first page to start the bulk load
     *
     * @return
     * @throws HyracksDataException
     */
    int getBulkLoadLeaf() throws HyracksDataException;

    /**
     * Set the root page id and finalize the bulk load operation
     *
     * @param rootPage
     * @throws HyracksDataException
     */
    void setRootPageId(int rootPage) throws HyracksDataException;
}
