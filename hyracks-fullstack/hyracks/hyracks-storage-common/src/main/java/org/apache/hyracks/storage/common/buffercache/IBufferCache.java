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

    long INVALID_DPID = -1l;
    int INVALID_PAGEID = -1;
    int RESERVED_HEADER_BYTES = 8;

    public void createFile(FileReference fileRef) throws HyracksDataException;

    public int createMemFile() throws HyracksDataException;

    public void openFile(int fileId) throws HyracksDataException;

    public void closeFile(int fileId) throws HyracksDataException;

    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException;

    public void deleteMemFile(int fileId) throws HyracksDataException;

    public ICachedPage tryPin(long dpid) throws HyracksDataException;

    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException;

    public void unpin(ICachedPage page) throws HyracksDataException;

    public void flushDirtyPage(ICachedPage page) throws HyracksDataException;

    public void adviseWontNeed(ICachedPage page);

    public ICachedPage confiscatePage(long dpid) throws HyracksDataException;

    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId) throws HyracksDataException;

    public void returnPage(ICachedPage page);

    public void returnPage(ICachedPage page, boolean reinsert);

    public void force(int fileId, boolean metadata) throws HyracksDataException;

    int getPageSize();

    int getPageSizeWithHeader();

    public int getNumPages();

    public int getNumPagesOfFile(int fileId) throws HyracksDataException;

    public int getFileReferenceCount(int fileId);

    public void close() throws HyracksDataException;

    public IFIFOPageQueue createFIFOQueue();

    public void finishQueue();

    void copyPage(ICachedPage src, ICachedPage dst);

    void setPageDiskId(ICachedPage page, long dpid);

    public boolean isReplicationEnabled();

    public IIOReplicationManager getIOReplicationManager();

    void purgeHandle(int fileId) throws HyracksDataException;

    void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;
}
