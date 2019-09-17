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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.replication.IIOReplicationManager;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.util.JSONUtil;

public class MultitenantVirtualBufferCache implements IVirtualBufferCache {

    private final IVirtualBufferCache vbc;
    private int openCount;

    public MultitenantVirtualBufferCache(IVirtualBufferCache virtualBufferCache) {
        this.vbc = virtualBufferCache;
        openCount = 0;
    }

    @Override
    public int createFile(FileReference fileRef) throws HyracksDataException {
        return vbc.createFile(fileRef);
    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        vbc.openFile(fileId);
    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        vbc.closeFile(fileId);
    }

    @Override
    public void deleteFile(int fileId) throws HyracksDataException {
        vbc.deleteFile(fileId);
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        return vbc.pin(dpid, newPage);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        vbc.unpin(page);
    }

    @Override
    public void flush(ICachedPage page) throws HyracksDataException {
        vbc.flush(page);
    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        vbc.force(fileId, metadata);
    }

    @Override
    public int getPageSize() {
        return vbc.getPageSize();
    }

    @Override
    public int getPageSizeWithHeader() {
        return vbc.getPageSizeWithHeader();
    }

    @Override
    public int getPageBudget() {
        return vbc.getPageBudget();
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        --openCount;
        if (openCount == 0) {
            vbc.close();
        }
    }

    @Override
    public synchronized void open() throws HyracksDataException {
        ++openCount;
        if (openCount == 1) {
            boolean failed = true;
            try {
                vbc.open();
                failed = false;
            } finally {
                if (failed) {
                    openCount--;
                }
            }
        }
    }

    @Override
    public boolean isFull() {
        return vbc.isFull();
    }

    @Override
    public void reset() {
        vbc.reset();
    }

    @Override
    public IFileMapManager getFileMapProvider() {
        return vbc.getFileMapProvider();
    }

    @Override
    public int getNumPagesOfFile(int fileId) throws HyracksDataException {
        return vbc.getNumPagesOfFile(fileId);
    }

    @Override
    public ICachedPage confiscatePage(long dpid) throws HyracksDataException {
        return vbc.confiscatePage(dpid);
    }

    @Override
    public ICachedPage confiscateLargePage(long dpid, int multiplier, int extraBlockPageId)
            throws HyracksDataException {
        return vbc.confiscateLargePage(dpid, multiplier, extraBlockPageId);
    }

    @Override
    public void returnPage(ICachedPage page) {
        vbc.returnPage(page);
    }

    @Override
    public IFIFOPageWriter createFIFOWriter(IPageWriteCallback callback, IPageWriteFailureCallback failureCallback) {
        return vbc.createFIFOWriter(callback, failureCallback);
    }

    @Override
    public void returnPage(ICachedPage page, boolean reinsert) {

    }

    @Override
    public int getFileReferenceCount(int fileId) {
        return 0;
    }

    @Override
    public boolean isReplicationEnabled() {
        return false;
    }

    @Override
    public IIOReplicationManager getIOReplicationManager() {
        return null;
    }

    @Override
    public void purgeHandle(int fileId) throws HyracksDataException {

    }

    @Override
    public void resizePage(ICachedPage page, int multiplier, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException {
        vbc.resizePage(page, multiplier, extraPageBlockHelper);
    }

    @Override
    public String toString() {
        return JSONUtil.fromMap(toMap());
    }

    private Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("class", getClass().getSimpleName());
        map.put("vbc", vbc.toString());
        map.put("openCount", openCount);
        return map;
    }

    @Override
    public int openFile(FileReference fileRef) throws HyracksDataException {
        return vbc.openFile(fileRef);
    }

    @Override
    public void deleteFile(FileReference file) throws HyracksDataException {
        vbc.deleteFile(file);
    }

    @Override
    public void closeFileIfOpen(FileReference fileRef) {
        throw new UnsupportedOperationException();
    }
}
