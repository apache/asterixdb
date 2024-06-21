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
package org.apache.hyracks.cloud.io.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.cloud.io.request.ICloudBeforeRetryRequest;
import org.apache.hyracks.cloud.io.request.ICloudRequest;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class CloudInputStream {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ICloudIOManager cloudIOManager;
    private final IFileHandle handle;
    private InputStream in;
    private long offset;
    private long remaining;

    public CloudInputStream(ICloudIOManager cloudIOManager, IFileHandle handle, InputStream in, long offset,
            long length) {
        this.cloudIOManager = cloudIOManager;
        this.handle = handle;
        this.in = in;
        this.offset = offset;
        this.remaining = length;
    }

    public String getPath() {
        return handle.getFileReference().getRelativePath();
    }

    public long getOffset() {
        return offset;
    }

    public long getRemaining() {
        return remaining;
    }

    public void read(ByteBuffer buffer) throws HyracksDataException {
        int position = buffer.position();
        ICloudRequest read = () -> {
            while (buffer.remaining() > 0) {
                int length = in.read(buffer.array(), buffer.position(), buffer.remaining());
                if (length < 0) {
                    throw new IllegalStateException("Stream should not be empty!");
                }
                buffer.position(buffer.position() + length);
            }
        };

        ICloudBeforeRetryRequest retry = () -> {
            buffer.position(position);
            cloudIOManager.restoreStream(this);
        };

        CloudRetryableRequestUtil.run(read, retry);

        offset += buffer.limit();
        remaining -= buffer.limit();
    }

    public void skipTo(long newOffset) throws HyracksDataException {
        if (newOffset > offset) {
            skip(newOffset - offset);
        }
    }

    public void close() {
        if (remaining != 0) {
            LOGGER.warn("Closed cloud stream with nonzero bytes = {}", remaining);
        }

        try {
            in.close();
        } catch (IOException e) {
            LOGGER.error("Failed to close stream", e);
        }
    }

    public void setInputStream(InputStream in) {
        this.in = in;
    }

    private void skip(long n) throws HyracksDataException {
        /*
         * Advance offset and reduce the remaining so that the streamRestore will start from where we want to skip
         * in case the stream has to be restored.
         */
        offset += n;
        remaining -= n;

        try {
            long remaining = n;
            while (remaining > 0) {
                remaining -= in.skip(remaining);
            }
        } catch (Throwable e) {
            if (remaining > 0) {
                // Only restore the stream if additional bytes are required
                CloudRetryableRequestUtil.run(() -> cloudIOManager.restoreStream(this));
            }
        }
    }

    @Override
    public String toString() {
        return "{file: " + handle.getFileReference() + ", streamOffset: " + offset + ", streamRemaining: " + remaining
                + "}";
    }
}
