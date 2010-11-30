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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RunFileWriter implements IFrameWriter {
    private final File file;
    private FileChannel channel;

    public RunFileWriter(File file) {
        this.file = file;
    }

    @Override
    public void open() throws HyracksDataException {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            throw new HyracksDataException(e);
        }
        channel = raf.getChannel();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        int remain = buffer.capacity();
        while (remain > 0) {
            int len;
            try {
                len = channel.write(buffer);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            if (len < 0) {
                throw new HyracksDataException("Error writing data");
            }
            remain -= len;
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
    }
}