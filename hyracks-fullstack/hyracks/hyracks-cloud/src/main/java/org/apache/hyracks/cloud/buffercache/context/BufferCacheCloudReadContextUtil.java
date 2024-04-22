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

import static org.apache.hyracks.storage.common.buffercache.IBufferCache.RESERVED_HEADER_BYTES;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.storage.common.buffercache.BufferCacheHeaderHelper;

public class BufferCacheCloudReadContextUtil {
    private BufferCacheCloudReadContextUtil() {
    }

    public static boolean isEmpty(BufferCacheHeaderHelper header) {
        ByteBuffer headerBuf = header.getBuffer();
        // THIS IS ONLY VALID FOR COLUMNAR
        return headerBuf.getInt(BufferCacheHeaderHelper.FRAME_MULTIPLIER_OFF) == 0;
    }

    public static void persist(ICloudIOManager cloudIOManager, IFileHandle fileHandle, ByteBuffer buffer, long offset)
            throws HyracksDataException {
        int originalLimit = buffer.limit();
        buffer.position(RESERVED_HEADER_BYTES);

        // First write the content of the page
        cloudIOManager.localWriter(fileHandle, offset + RESERVED_HEADER_BYTES, buffer);

        // If a failure happened before this position, we are sure the header still has 0 for
        // the multiplier (i.e., a hole)

        // Next, write the header. This is like a "commit" for the page
        buffer.position(0);
        buffer.limit(RESERVED_HEADER_BYTES);
        // TODO what if this failed to write fully? (e.g., it wrote the first 3 bytes of the multiplier)
        cloudIOManager.localWriter(fileHandle, offset, buffer);

        // After this point the header is written. We are sure the page is valid and has the correct multiplier

        // Restore the page's position and limit
        buffer.position(0);
        buffer.limit(originalLimit);
    }
}
