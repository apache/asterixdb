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

import static org.apache.hyracks.storage.common.buffercache.IBufferCache.RESERVED_HEADER_BYTES;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.compression.ICompressorDecompressor;

public class BufferCacheHeaderHelper {
    private static final int FRAME_MULTIPLIER_OFF = 0;
    private static final int EXTRA_BLOCK_PAGE_ID_OFF = FRAME_MULTIPLIER_OFF + 4; // 4

    private final ByteBuffer[] array;
    private final int pageSizeWithHeader;
    private ByteBuffer buf;

    public BufferCacheHeaderHelper(int pageSize) {
        this.pageSizeWithHeader = RESERVED_HEADER_BYTES + pageSize;
        buf = ByteBuffer.allocate(pageSizeWithHeader);
        array = new ByteBuffer[] { buf, null };
    }

    public ByteBuffer[] prepareWrite(CachedPage cPage) {
        setPageInfo(cPage);
        buf.position(0);
        buf.limit(RESERVED_HEADER_BYTES);
        array[1] = cPage.buffer;
        return array;
    }

    public ByteBuffer prepareWrite(CachedPage cPage, int requiredSize) {
        ensureBufferCapacity(requiredSize);
        setPageInfo(cPage);
        buf.position(RESERVED_HEADER_BYTES);
        buf.limit(buf.capacity());
        return buf;
    }

    public ByteBuffer prepareRead(int size) {
        buf.position(0);
        buf.limit(size);
        return buf;
    }

    public ByteBuffer processHeader(CachedPage cPage) {
        cPage.setFrameSizeMultiplier(buf.getInt(FRAME_MULTIPLIER_OFF));
        cPage.setExtraBlockPageId(buf.getInt(EXTRA_BLOCK_PAGE_ID_OFF));
        buf.position(RESERVED_HEADER_BYTES);
        return buf;
    }

    private void setPageInfo(CachedPage cPage) {
        buf.putInt(FRAME_MULTIPLIER_OFF, cPage.getFrameSizeMultiplier());
        buf.putInt(EXTRA_BLOCK_PAGE_ID_OFF, cPage.getExtraBlockPageId());
    }

    /**
     * {@link ICompressorDecompressor#compress(byte[], int, int, byte[], int)} may require additional
     * space to do the compression. see {@link ICompressorDecompressor#computeCompressedBufferSize(int)}.
     *
     * @param compressor
     * @param size
     */
    private void ensureBufferCapacity(int size) {
        final int requiredSize = size + RESERVED_HEADER_BYTES;
        if (buf.capacity() < requiredSize) {
            buf = ByteBuffer.allocate(requiredSize);
            array[0] = buf;
        }
        buf.limit(buf.capacity());
    }
}
