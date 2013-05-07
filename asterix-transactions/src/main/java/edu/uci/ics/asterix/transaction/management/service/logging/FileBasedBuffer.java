/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Represent a buffer that is backed by a physical file. Provider custom APIs
 * for accessing a chunk of the underlying file.
 */
public class FileBasedBuffer extends Buffer implements IFileBasedBuffer {

    private String filePath;
    private FileChannel fileChannel;
    private RandomAccessFile raf;
    private int bufferSize;

    private int bufferLastFlushOffset;
    private int bufferNextWriteOffset;
    private final int diskSectorSize;

    public FileBasedBuffer(String filePath, long offset, int bufferSize, int diskSectorSize) throws IOException {
        this.filePath = filePath;
        buffer = ByteBuffer.allocate(bufferSize);
        raf = new RandomAccessFile(new File(filePath), "rw");
        fileChannel = raf.getChannel();
        fileChannel.position(offset);
        fileChannel.read(buffer);
        buffer.position(0);
        this.bufferSize = bufferSize;
        buffer.limit(bufferSize);
        bufferLastFlushOffset = 0;
        bufferNextWriteOffset = 0;
        this.diskSectorSize = diskSectorSize;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public int getSize() {
        return bufferSize;
    }

    public void clear() {
        buffer.clear();
    }

    @Override
    public void flush() throws IOException {
        //flush
        int pos = bufferLastFlushOffset;
        int limit = (((bufferNextWriteOffset - 1) / diskSectorSize) + 1) * diskSectorSize;
        buffer.position(pos);
        buffer.limit(limit);
        fileChannel.write(buffer);
        fileChannel.force(true);

        //update variables
        bufferLastFlushOffset = limit;
        bufferNextWriteOffset = limit;
        buffer.limit(bufferSize);
    }

    @Override
    public void put(byte b) {
        buffer.put(b);
    }

    @Override
    public void put(int offset, byte b) {
        buffer.put(offset, b);
    }

    @Override
    public void put(byte[] bytes, int start, int length) {
        buffer.put(bytes, start, length);
    }

    @Override
    public void put(byte[] bytes) {
        buffer.put(bytes);
    }

    @Override
    public void writeInt(int value) {
        buffer.putInt(value);
    }

    @Override
    public void writeInt(int index, int value) {
        buffer.putInt(index, value);
    }

    @Override
    public void writeLong(long value) {
        buffer.putLong(value);
    }

    @Override
    public void writeLong(int index, long value) {
        buffer.putLong(index, value);
    }

    /**
     * Resets the buffer with content (size as specified) from a given file
     * starting at offset.
     */
    @Override
    public void reset(String filePath, long diskNextWriteOffset, int bufferSize) throws IOException {
        if (!filePath.equals(this.filePath)) {
            raf.close();//required?
            fileChannel.close();
            raf = new RandomAccessFile(filePath, "rw");
            this.filePath = filePath;
        }
        fileChannel = raf.getChannel();
        fileChannel.position(diskNextWriteOffset);
        erase();
        buffer.position(0);
        buffer.limit(bufferSize);
        this.bufferSize = bufferSize;

        bufferLastFlushOffset = 0;
        bufferNextWriteOffset = 0;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    @Override
    public void open(String filePath, long offset, int bufferSize) throws IOException {
        raf = new RandomAccessFile(filePath, "rw");
        fileChannel = raf.getChannel();
        fileChannel.position(offset);
        erase();
        buffer.position(0);
        buffer.limit(bufferSize);
        this.bufferSize = bufferSize;
        bufferLastFlushOffset = 0;
        bufferNextWriteOffset = 0;
    }

    @Override
    public long getDiskNextWriteOffset() throws IOException {
        return fileChannel.position();
    }

    @Override
    public void setDiskNextWriteOffset(long offset) throws IOException {
        fileChannel.position(offset);
    }

    @Override
    public int getBufferLastFlushOffset() {
        return bufferLastFlushOffset;
    }

    @Override
    public void setBufferLastFlushOffset(int offset) {
        this.bufferLastFlushOffset = offset;
    }

    @Override
    public int getBufferNextWriteOffset() {
        synchronized (fileChannel) {
            return bufferNextWriteOffset;
        }
    }

    @Override
    public void setBufferNextWriteOffset(int offset) {
        synchronized (fileChannel) {
            if (bufferNextWriteOffset < offset) {
                bufferNextWriteOffset = offset;
            }
        }
    }

}
