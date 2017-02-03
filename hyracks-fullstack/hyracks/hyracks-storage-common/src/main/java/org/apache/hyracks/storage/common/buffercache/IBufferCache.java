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

    void createFile(FileReference fileRef) throws HyracksDataException;

    void openFile(int fileId) throws HyracksDataException;

    void closeFile(int fileId) throws HyracksDataException;

    void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException;

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

    public boolean isReplicationEnabled();

    public IIOReplicationManager getIOReplicationManager();

    void purgeHandle(int fileId) throws HyracksDataException;

    void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;
}
