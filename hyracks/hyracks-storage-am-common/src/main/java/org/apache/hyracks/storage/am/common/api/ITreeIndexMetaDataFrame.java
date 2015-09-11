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

import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public interface ITreeIndexMetaDataFrame {
    public void initBuffer(byte level);

    public void setPage(ICachedPage page);

    public ICachedPage getPage();

    public byte getLevel();

    public void setLevel(byte level);

    public int getNextPage();

    public void setNextPage(int nextPage);

    public int getMaxPage();

    public void setMaxPage(int maxPage);

    public int getFreePage();

    public boolean hasSpace();

    public void addFreePage(int freePage);

    // Special flag for LSM-Components to mark whether they are valid or not. 
    public boolean isValid();

    // Set special validity flag.
    public void setValid(boolean isValid);

    // Return the lsm component filter page id.
    public int getLSMComponentFilterPageId();

    // Set the lsm component filter page id.
    public void setLSMComponentFilterPageId(int filterPage);

    // Special placeholder for LSN information. Used for transactional LSM indexes.
    public long getLSN();

    public void setLSN(long lsn);
}
