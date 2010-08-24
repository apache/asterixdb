package edu.uci.ics.hyracks.storage.common.storage.buffercache;

import java.nio.ByteBuffer;

public class DirectBufferAllocator implements ICacheMemoryAllocator {
    @Override
    public ByteBuffer[] allocate(int pageSize, int numPages) {
        ByteBuffer[] buffers = new ByteBuffer[numPages];
        for (int i = 0; i < numPages; ++i) {
            buffers[i] = ByteBuffer.allocateDirect(pageSize);
        }
        return buffers;
    }
}