package edu.uci.ics.hyracks.dataflow.common.io;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileHandle;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class RunFileReader implements IFrameReader {
    private final FileReference file;
    private final IIOManager ioManager;
    private final long size;

    private FileHandle handle;
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
}