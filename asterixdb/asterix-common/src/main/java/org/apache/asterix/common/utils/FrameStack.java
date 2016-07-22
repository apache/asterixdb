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
package org.apache.asterix.common.utils;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Not thread safe stack that is used to store fixed size buffers in memory
 * Once memory is consumed, it uses disk to store buffers
 */
public class FrameStack implements Closeable {
    private static final AtomicInteger stackIdGenerator = new AtomicInteger(0);
    private static final String STACK_FILE_NAME = "stack";
    private final int stackId;
    private final int frameSize;
    private final int numOfMemoryFrames;
    private final ArrayDeque<ByteBuffer> fullBuffers;
    private final ArrayDeque<ByteBuffer> emptyBuffers;
    private int totalWriteCount = 0;
    private int totalReadCount = 0;
    private final File file;
    private final RandomAccessFile iostream;
    private final byte[] frame;

    /**
     * Create a hybrid of memory and disk stack of byte buffers
     *
     * @param dir
     * @param frameSize
     * @param numOfMemoryFrames
     * @throws HyracksDataException
     * @throws FileNotFoundException
     */
    public FrameStack(String dir, int frameSize, int numOfMemoryFrames)
            throws HyracksDataException, FileNotFoundException {
        this.stackId = stackIdGenerator.getAndIncrement();
        this.frameSize = frameSize;
        this.numOfMemoryFrames = numOfMemoryFrames;
        this.fullBuffers = numOfMemoryFrames <= 0 ? null : new ArrayDeque<>();
        this.emptyBuffers = numOfMemoryFrames <= 0 ? null : new ArrayDeque<>();
        this.file = StoragePathUtil.createFile(
                ((dir == null) ? "" : (dir.endsWith(File.separator) ? dir : (dir + File.separator))) + STACK_FILE_NAME,
                stackId);
        this.iostream = new RandomAccessFile(file, "rw");
        this.frame = new byte[frameSize];
    }

    /**
     * @return the number of remaining frames to be read in the stack
     */
    public int remaining() {
        return totalWriteCount - totalReadCount;
    }

    /**
     * copy content of buffer into the stack
     *
     * @param buffer
     * @throws IOException
     */
    public synchronized void push(ByteBuffer buffer) throws IOException {
        int diff = totalWriteCount - totalReadCount;
        if (diff < numOfMemoryFrames) {
            ByteBuffer aBuffer = allocate();
            aBuffer.put(buffer.array());
            aBuffer.flip();
            fullBuffers.push(aBuffer);
        } else {
            long position = (long) (diff - numOfMemoryFrames) * frameSize;
            if (position != iostream.getFilePointer()) {
                iostream.seek(position);
            }
            iostream.write(buffer.array());
        }
        totalWriteCount++;
    }

    private ByteBuffer allocate() {
        ByteBuffer aBuffer = emptyBuffers.poll();
        if (aBuffer == null) {
            aBuffer = ByteBuffer.allocate(frameSize);
        }
        aBuffer.clear();
        return aBuffer;
    }

    /**
     * Free a frame off of the stack and copy it into dest
     *
     * @param dest
     * @throws IOException
     */
    public synchronized void pop(ByteBuffer dest) throws IOException {
        dest.clear();
        int diff = totalWriteCount - totalReadCount - 1;
        if (diff >= 0) {
            if (diff < numOfMemoryFrames) {
                totalReadCount++;
                ByteBuffer aBuffer = fullBuffers.pop();
                emptyBuffers.push(aBuffer);
                dest.put(aBuffer.array());
            } else {
                long position = (long) (diff - numOfMemoryFrames) * frameSize;
                iostream.seek(position);
                iostream.readFully(frame);
                dest.put(frame);
            }
        }
        dest.flip();
    }

    /**
     * Closing this stack will result in the data being deleted
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        iostream.close();
        Files.delete(file.toPath());
    }

}
