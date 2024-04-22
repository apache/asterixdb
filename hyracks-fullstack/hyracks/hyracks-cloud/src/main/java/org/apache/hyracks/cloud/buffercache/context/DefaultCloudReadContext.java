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
package org.apache.hyracks.cloud.buffercache.context;

import static org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper.EXTRA_BLOCK_PAGE_ID_OFF;
import static org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper.FRAME_MULTIPLIER_OFF;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.disk.IPhysicalDrive;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.ThreadSafe;

/**
 * Default context for {@link BufferCache#pin(long, IBufferCacheReadContext)}
 * and {@link BufferCache#unpin(ICachedPage, IBufferCacheReadContext)} in a cloud deployment.
 * <p>
 * The default behavior of this context is persisting the pages read from cloud if {@link IPhysicalDrive}
 * reports that the local drive(s) are not pressured.
 */
@ThreadSafe
public class DefaultCloudReadContext implements IBufferCacheReadContext {
    private final IPhysicalDrive drive;

    public DefaultCloudReadContext(IPhysicalDrive drive) {
        this.drive = drive;
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
        return readAndPersistPage(ioManager, fileHandle, header, cPage, drive.hasSpace());
    }

    public static ByteBuffer readAndPersistPage(IOManager ioManager, BufferedFileHandle fileHandle,
            BufferCacheHeaderHelper header, CachedPage cPage, boolean persist) throws HyracksDataException {
        ByteBuffer headerBuf = readAndPersistIfEmpty(ioManager, fileHandle.getFileHandle(), header, cPage, persist);

        cPage.setFrameSizeMultiplier(headerBuf.getInt(FRAME_MULTIPLIER_OFF));
        cPage.setExtraBlockPageId(headerBuf.getInt(EXTRA_BLOCK_PAGE_ID_OFF));
        headerBuf.position(BufferCache.RESERVED_HEADER_BYTES);
        return headerBuf;
    }

    /**
     * Note: {@link BufferCache} guarantees that no two reads on the same page can happen at the same time.
     * This means persisting the page on disk here is guaranteed to be done by a single thread.
     *
     * @param ioManager  ioManager (guaranteed to be {@link ICloudIOManager}
     * @param fileHandle file
     * @param header     header buffer provider
     * @param cPage      cached page
     * @return header buffer
     */
    private static ByteBuffer readAndPersistIfEmpty(IOManager ioManager, IFileHandle fileHandle,
            BufferCacheHeaderHelper header, CachedPage cPage, boolean persist) throws HyracksDataException {
        ByteBuffer headerBuf = header.getBuffer();
        if (BufferCacheCloudReadContextUtil.isEmpty(header)) {
            // header indicates the page is empty
            // reset the buffer position to 0. Limit should be already set before the call of processHeader
            headerBuf.position(0);
            long offset = cPage.getCompressedPageOffset();
            ICloudIOManager cloudIOManager = (ICloudIOManager) ioManager;
            // Read pageZero from the cloud
            cloudIOManager.cloudRead(fileHandle, offset, headerBuf);
            headerBuf.flip();

            if (persist) {
                BufferCacheCloudReadContextUtil.persist(cloudIOManager, fileHandle, headerBuf, offset);
            }
        }

        return headerBuf;
    }
}
