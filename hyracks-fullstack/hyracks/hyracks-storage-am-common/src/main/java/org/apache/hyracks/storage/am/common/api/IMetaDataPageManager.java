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
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public interface IMetaDataPageManager {
    public static final long INVALID_LSN_OFFSET = -1;

    /**
     * This is the class through which one interfaces with index metadata.
     * The index metadata contains information such as the LSN of the index, free page information,
     * and filter page locations.
     */
    /**
     * Open an index file's metadata
     *
     * @param fileId
     *            The file which to open the metadata of
     */
    public void open(int fileId);

    /**
     * Close an index file's metadata.
     *
     * @throws HyracksDataException
     */

    public void close() throws HyracksDataException;

    /**
     * Get the location of a free page to use for index operations
     *
     * @param metaFrame
     *            A metadata frame to use to wrap the raw page
     * @return A page location, or -1 if no free page could be found or allocated
     * @throws HyracksDataException
     */

    public int getFreePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;

    /**
     * Get the location of a block of free pages to use for index operations
     *
     * @param metaFrame
     *            A metadata frame to use to wrap the raw page
     * @return The starting page location, or -1 if a block of free pages could be found or allocated
     * @throws HyracksDataException
     */

    public int getFreePageBlock(ITreeIndexMetaDataFrame metaFrame, int count) throws HyracksDataException;

    /**
     * Add a page back to the pool of free pages within an index file
     *
     * @param metaFrame
     *            A metadata frame to use to wrap the raw page
     * @param freePage
     *            The page which to return to the free space
     * @throws HyracksDataException
     */

    public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException;

    public void addFreePageBlock(ITreeIndexMetaDataFrame metaFrame, int startingPage, int count)
            throws HyracksDataException;

    /**
     * Gets the highest page offset according to the metadata
     *
     * @param metaFrame
     *            A metadata frame to use to wrap the raw page
     * @return The locaiton of the highest offset page
     * @throws HyracksDataException
     */

    public int getMaxPage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;

    /**
     * Initializes the index metadata
     *
     * @param metaFrame
     *            A metadata farme to use to wrap the raw page
     * @param currentMaxPage
     *            The highest page offset to consider valid
     * @throws HyracksDataException
     */

    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException;

    public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory();

    // required to return negative values
    public byte getMetaPageLevelIndicator();

    public byte getFreePageLevelIndicator();

    // determined by examining level indicator

    public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame);

    public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame);

    /**
     * Determines where the metadata page is located in an index file
     *
     * @return The locaiton of the metadata page, or -1 if the file appears to be corrupt
     * @throws HyracksDataException
     */

    public int getFirstMetadataPage() throws HyracksDataException;

    /**
     * Initializes the metadata manager on an open index file
     *
     * @param metaFrame
     *            A metadata frame used to wrap the raw page
     * @throws HyracksDataException
     */

    void init(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;

    /**
     * Locate the filter page in an index file
     *
     * @return The offset of the filter page if it exists, or less than zero if no filter page exists yet
     * @throws HyracksDataException
     */

    int getFilterPageId() throws HyracksDataException;

    void setFilterPageId(int filterPageId) throws HyracksDataException;

    long getLSN() throws HyracksDataException;

    void setLSN(long lsn) throws HyracksDataException;

    /**
     * Set the cached page to manage for filter data
     *
     * @param page
     *            The page to manage
     */

    void setFilterPage(ICachedPage page);

    ICachedPage getFilterPage();

    boolean appendOnlyMode();

    /**
     * @return The LSN byte offset in the LSM disk component if the index is valid, otherwise {@link #INVALID_LSN_OFFSET}.
     * @throws HyracksDataException
     */
    long getLSNOffset() throws HyracksDataException;

    public long getLastMarkerLSN() throws HyracksDataException;

    void setRootPage(int rootPage) throws HyracksDataException;

    int getRootPage() throws HyracksDataException;
}
