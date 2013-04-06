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
    private long nextWritePosition;
    private FileChannel fileChannel;
    private RandomAccessFile raf;
    private int size;

    public FileBasedBuffer(String filePath, long offset, int size) throws IOException {
        this.filePath = filePath;
        this.nextWritePosition = offset;
        buffer = ByteBuffer.allocate(size);
        raf = new RandomAccessFile(new File(filePath), "rw");
        raf.seek(offset);
        fileChannel = raf.getChannel();
        fileChannel.read(buffer);
        buffer.position(0);
        this.size = size;
        buffer.limit(size);
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getOffset() {
        return nextWritePosition;
    }

    public void setOffset(long offset) {
        this.nextWritePosition = offset;
    }

    @Override
    public int getSize() {
        return buffer.limit();
    }

    public void clear() {
        buffer.clear();
    }

    @Override
    public void flush() throws IOException {
        buffer.position(0);
        buffer.limit(size);
        fileChannel.write(buffer);
        fileChannel.force(true);
        erase();
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
    public void reset(String filePath, long nextWritePosition, int size) throws IOException {
        if (!filePath.equals(this.filePath)) {
            raf.close();//required?
            fileChannel.close();
            raf = new RandomAccessFile(filePath, "rw");
            this.filePath = filePath;
        }
        this.nextWritePosition = nextWritePosition;
        raf.seek(nextWritePosition);
        fileChannel = raf.getChannel();
        erase();
        buffer.position(0);
        buffer.limit(size);
        this.size = size;
    }
    
    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
    
    @Override
    public void open(String filePath, long offset, int size) throws IOException {
        raf = new RandomAccessFile(filePath, "rw");
        this.nextWritePosition = offset;
        fileChannel = raf.getChannel();
        fileChannel.position(offset);
        erase();
        buffer.position(0);
        buffer.limit(size);
        this.size = size;
    }

    public long getNextWritePosition() {
        return nextWritePosition;
    }

    public void setNextWritePosition(long nextWritePosition) {
        this.nextWritePosition = nextWritePosition;
    }

}
