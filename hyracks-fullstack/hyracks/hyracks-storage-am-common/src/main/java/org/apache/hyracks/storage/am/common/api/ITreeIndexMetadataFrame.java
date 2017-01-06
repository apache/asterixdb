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
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/**
 * A frame for reading and writing metadata pages of an index
 * It can also be used to read metadata of non metadata pages
 */
public interface ITreeIndexMetadataFrame {
    /**
     * initialize the metadata page
     */
    void init();

    /**
     * Set the page in the frame
     * @param page
     */
    void setPage(ICachedPage page);

    /**
     * @return the page set in this frame
     */
    ICachedPage getPage();

    /**
     * The page level.
     * This method can be run on any type of page
     */
    byte getLevel();

    /**
     * Set the page level
     * @param level
     */
    void setLevel(byte level);

    /**
     * Get the next metadata page if this page is linked to other metadata pages
     * Return a negative value otherwise
     * @return
     */
    int getNextMetadataPage();

    /**
     * Link this metadata page to another one
     * @param nextPage
     */
    void setNextMetadataPage(int nextPage);

    /**
     * @return the max index file page as indicated by the current metadata page
     */
    int getMaxPage();

    /**
     * Set the max page of the file
     * @param maxPage
     */
    void setMaxPage(int maxPage);

    /**
     * Get a free page from the page
     * @return
     */
    int getFreePage();

    /**
     * Get the remaining space in the metadata page
     * @return
     */
    int getSpace();

    /**
     * add a new free page to the metadata page
     * @param freePage
     */
    void addFreePage(int freePage);

    /**
     * get the value with the key = key
     * @param key
     * @param value
     */
    void get(IValueReference key, IPointable value);

    /**
     * set the value with the key = key
     * @param key
     * @param value
     * @throws HyracksDataException
     */
    void put(IValueReference key, IValueReference value) throws HyracksDataException;

    /**
     * @return true if the index is valid according to the metadata page, false otherwise
     */
    boolean isValid();

    /**
     * Sets the index to be valid in the metadata page
     * @param valid
     */
    void setValid(boolean valid);

    /**
     * Get the storage version associated with this index
     * @return
     */
    int getVersion();

    /**
     * Set the index root page id
     * @param rootPage
     */
    void setRootPageId(int rootPage);

    /**
     * @return the index root page id
     */
    int getRootPageId();

    /**
     * @return the number of key value pairs
     */
    int getTupleCount();

    /**
     * return the offset to the entry of the passed key, -1, otherwise
     * @param key
     */
    int getOffset(IValueReference key);

    /**
     * @return true if the inspected page is a metadata page, false otherwise
     */
    boolean isMetadataPage();

    /**
     * @return true if the inspected page is a free page, false otherwise
     */
    boolean isFreePage();
}
