/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class MultitenantVirtualBufferCache implements IVirtualBufferCache {

    private final IVirtualBufferCache vbc;
    private int openCount;

    public MultitenantVirtualBufferCache(IVirtualBufferCache virtualBufferCache) {
        this.vbc = virtualBufferCache;
        openCount = 0;
    }

    @Override
    public void createFile(FileReference fileRef) throws HyracksDataException {
        vbc.createFile(fileRef);
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
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        vbc.deleteFile(fileId, flushDirtyPages);
    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        return vbc.tryPin(dpid);
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
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
        vbc.flushDirtyPage(page);
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
    public int getNumPages() {
        return vbc.getNumPages();
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
            vbc.open();
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
}
