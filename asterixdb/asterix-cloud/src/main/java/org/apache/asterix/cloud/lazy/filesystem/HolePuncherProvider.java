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
package org.apache.asterix.cloud.lazy.filesystem;

import java.nio.ByteBuffer;

import org.apache.asterix.cloud.AbstractCloudIOManager;
import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.common.cloud.CloudCachePolicy;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;

public final class HolePuncherProvider {
    private static final IHolePuncher UNSUPPORTED = HolePuncherProvider::unsupported;

    private HolePuncherProvider() {
    }

    public static IHolePuncher get(AbstractCloudIOManager cloudIOManager, CloudProperties cloudProperties,
            IWriteBufferProvider bufferProvider) {
        if (cloudProperties.getCloudCachePolicy() != CloudCachePolicy.SELECTIVE) {
            return UNSUPPORTED;
        }

        return new DebugHolePuncher(cloudIOManager, bufferProvider);
    }

    private static int unsupported(IFileHandle fileHandle, long offset, long length) {
        throw new UnsupportedOperationException("punchHole is not supported");
    }

    private static final class DebugHolePuncher implements IHolePuncher {
        private final AbstractCloudIOManager cloudIOManager;
        private final IWriteBufferProvider bufferProvider;

        private DebugHolePuncher(AbstractCloudIOManager cloudIOManager, IWriteBufferProvider bufferProvider) {
            this.cloudIOManager = cloudIOManager;
            this.bufferProvider = bufferProvider;
        }

        @Override
        public int punchHole(IFileHandle fileHandle, long offset, long length) throws HyracksDataException {
            ByteBuffer buffer = acquireAndPrepareBuffer(length);
            int totalWritten = 0;
            try {
                long remaining = length;
                long position = offset;
                while (remaining > 0) {
                    int written = cloudIOManager.localWriter(fileHandle, position, buffer);
                    position += written;
                    remaining -= written;
                    totalWritten += written;
                    buffer.limit((int) Math.min(remaining, buffer.capacity()));
                }
            } finally {
                bufferProvider.recycle(buffer);
            }

            return totalWritten;
        }

        private ByteBuffer acquireAndPrepareBuffer(long length) {
            ByteBuffer buffer = bufferProvider.getBuffer();
            buffer.clear();
            if (buffer.capacity() >= length) {
                buffer.limit((int) length);
            }

            while (buffer.remaining() > Long.BYTES) {
                buffer.putLong(0L);
            }

            while (buffer.remaining() > 0) {
                buffer.put((byte) 0);
            }

            buffer.flip();
            return buffer;
        }
    }
}
