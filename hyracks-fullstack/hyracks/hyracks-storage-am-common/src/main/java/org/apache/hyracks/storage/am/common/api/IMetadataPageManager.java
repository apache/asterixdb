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

/**
 * Used to read from and write to index metadata.
 * The index metadata contains information such as:
 * --The LSN of the index.
 * --Free page information {Set of free pages}
 * --Filter information.
 * TODO: This interface needs to change to have calls to request memory space and write to those bytes
 */
public interface IMetadataPageManager extends IPageManager {
    public static class Constants {
        public static final long INVALID_LSN_OFFSET = -1;
        private Constants() {
        }
    }

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
     * @throws HyracksDataException
     */
    void setFilterPage(ICachedPage page) throws HyracksDataException;

    /**
     * Get filter page if exists, create and return a new one if it doesn't
     * @return
     * @throws HyracksDataException
     */
    ICachedPage getFilterPage() throws HyracksDataException;

    /**
     * @return The LSN byte offset in the LSM disk component if the index is valid,
     *         otherwise {@link #INVALID_LSN_OFFSET}.
     * @throws HyracksDataException
     */
    long getLSNOffset() throws HyracksDataException;
    long getLastMarkerLSN() throws HyracksDataException;
}
