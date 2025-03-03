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
package org.apache.asterix.cloud;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public final class WriteBufferProvider implements IWriteBufferProvider {
    private final int bufferSize;
    private final BlockingQueue<ByteBuffer> writeBuffers;

    public WriteBufferProvider(int numberOfBuffers, int bufferSize) {
        this.bufferSize = bufferSize;
        writeBuffers = new ArrayBlockingQueue<>(numberOfBuffers);
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        writeBuffers.offer(buffer);
    }

    @Override
    public ByteBuffer getBuffer() {
        ByteBuffer writeBuffer = writeBuffers.poll();
        if (writeBuffer == null) {
            return ByteBuffer.allocate(bufferSize);
        }
        return writeBuffer;
    }
}
