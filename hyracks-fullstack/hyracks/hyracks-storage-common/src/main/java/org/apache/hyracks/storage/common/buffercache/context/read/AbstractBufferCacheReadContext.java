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
package org.apache.hyracks.storage.common.buffercache.context.read;

import static org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper.EXTRA_BLOCK_PAGE_ID_OFF;
import static org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper.FRAME_MULTIPLIER_OFF;
import static org.apache.hyracks.storage.common.buffercache.IBufferCache.RESERVED_HEADER_BYTES;

import java.nio.ByteBuffer;

import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

abstract class AbstractBufferCacheReadContext implements IBufferCacheReadContext {
    @Override
    public final void onPin(ICachedPage page) {
        // NoOp
    }

    @Override
    public final void onUnpin(ICachedPage page) {
        // NoOp
    }

    @Override
    public final ByteBuffer processHeader(IOManager ioManager, BufferedFileHandle fileHandle,
            BufferCacheHeaderHelper header, CachedPage cPage) {
        ByteBuffer buf = header.getBuffer();
        cPage.setFrameSizeMultiplier(buf.getInt(FRAME_MULTIPLIER_OFF));
        cPage.setExtraBlockPageId(buf.getInt(EXTRA_BLOCK_PAGE_ID_OFF));
        buf.position(RESERVED_HEADER_BYTES);
        return buf;
    }
}
