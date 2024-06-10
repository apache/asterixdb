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
package org.apache.hyracks.cloud.sweeper;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.buffercache.page.CloudCachedPage;
import org.apache.hyracks.cloud.cache.unit.SweepableIndexUnit;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.ISweepContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public final class SweepContext implements ISweepContext {
    private final ICloudIOManager cloudIOManager;
    private final BufferCache bufferCache;
    private final Map<Integer, BufferedFileHandle> fileInfoMap;
    private final AtomicBoolean stop;
    private SweepableIndexUnit indexUnit;
    private BufferedFileHandle handle;

    public SweepContext(ICloudIOManager cloudIOManager, BufferCache bufferCache,
            Map<Integer, BufferedFileHandle> fileInfoMap, AtomicBoolean stop) {
        this.cloudIOManager = cloudIOManager;
        this.bufferCache = bufferCache;
        this.fileInfoMap = fileInfoMap;
        this.stop = stop;
    }

    @Override
    public void open(int fileId) throws HyracksDataException {
        close();
        bufferCache.openFile(fileId);
        synchronized (fileInfoMap) {
            this.handle = fileInfoMap.get(fileId);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (handle != null) {
            bufferCache.closeFile(handle.getFileId());
            handle = null;
        }
    }

    @Override
    public CloudCachedPage pin(long dpid, IBufferCacheReadContext bcOpCtx) throws HyracksDataException {
        return (CloudCachedPage) bufferCache.pin(dpid, bcOpCtx);
    }

    @Override
    public void unpin(ICachedPage page, IBufferCacheReadContext bcOpCtx) throws HyracksDataException {
        bufferCache.unpin(page, bcOpCtx);
    }

    public void setIndexUnit(SweepableIndexUnit indexUnit) {
        this.indexUnit = indexUnit;
    }

    public SweepableIndexUnit getIndexUnit() {
        return indexUnit;
    }

    public int punchHole(int startPageId, int numberOfPage) throws HyracksDataException {
        long offset = handle.getStartPageOffset(startPageId);
        long length = handle.getPagesTotalSize(startPageId, numberOfPage);
        return cloudIOManager.punchHole(handle.getFileHandle(), offset, length);
    }

    /**
     * Whether the sweep operation should stop or proceed
     * Stopping condition:
     * 1- Either the sweeper thread is shutting down
     * 2- OR the index was dropped
     *
     * @return true if it should stop, false otherwise
     */
    public boolean stopSweeping() {
        return stop.get() || indexUnit.isDropped();
    }
}
