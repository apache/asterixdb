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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read;

import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.DEFAULT;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public final class DefaultColumnReadContext implements IColumnReadContext {
    public static final IColumnReadContext INSTANCE = new DefaultColumnReadContext();

    private DefaultColumnReadContext() {
    }

    @Override
    public void onPin(ICachedPage page) {
        // NoOp
    }

    @Override
    public void onUnpin(ICachedPage page) {
        // NoOp
    }

    @Override
    public boolean isNewPage() {
        return false;
    }

    @Override
    public boolean incrementStats() {
        return true;
    }

    @Override
    public ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle, BufferCacheHeaderHelper header,
            CachedPage cPage) throws HyracksDataException {
        return DEFAULT.processHeader(ioManager, fileHandle, header, cPage);
    }

    @Override
    public ICachedPage pinNext(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId)
            throws HyracksDataException {
        int nextLeaf = leafFrame.getNextLeaf();
        bufferCache.unpin(leafFrame.getPage());
        ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeaf));
        leafFrame.setPage(nextPage);
        return nextPage;
    }

    @Override
    public void prepareColumns(ColumnBTreeReadLeafFrame leafFrame, IBufferCache bufferCache, int fileId) {
        // NoOp
    }

    @Override
    public void release(IBufferCache bufferCache) throws HyracksDataException {
        // NoOp
    }

    @Override
    public void close(IBufferCache bufferCache) throws HyracksDataException {
        // NoOp
    }
}
