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

import java.nio.ByteBuffer;

import org.apache.hyracks.util.StorageUtil;

public class FreeColumnBufferPool implements IColumnBufferPool {
    private static final int INITIAL_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.KILOBYTE);

    @Override
    public void reserve(int requestedBuffers) throws InterruptedException {

    }

    @Override
    public boolean tryReserve(int requestedBuffers) {
        return true;
    }

    @Override
    public void release(int buffers) {

    }

    @Override
    public ByteBuffer getBuffer() {
        return ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
    }

    @Override
    public int getMaxReservedBuffers() {
        return 0;
    }

    @Override
    public void recycle(ByteBuffer buffer) {

    }

    @Override
    public void close() {

    }
}
