/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.common.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IFileHandle;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class RunFileReader implements IFrameReader {
    private final FileReference file;
    private final IIOManager ioManager;
    private final long size;

    private IFileHandle handle;
    private long readPtr;

    public RunFileReader(FileReference file, IIOManager ioManager, long size) {
        this.file = file;
        this.ioManager = ioManager;
        this.size = size;
    }

    @Override
    public void open() throws HyracksDataException {
        handle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY, null);
        readPtr = 0;
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        buffer.clear();
        if (readPtr >= size) {
            return false;
        }
        readPtr += ioManager.syncRead(handle, readPtr, buffer);
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
        ioManager.close(handle);
    }

    public long getFileSize() {
        return size;
    }
}