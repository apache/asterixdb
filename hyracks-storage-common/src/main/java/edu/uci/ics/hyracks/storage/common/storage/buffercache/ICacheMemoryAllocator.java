package edu.uci.ics.hyracks.storage.common.storage.buffercache;

import java.nio.ByteBuffer;

public interface ICacheMemoryAllocator {
    public ByteBuffer[] allocate(int pageSize, int numPages);
}