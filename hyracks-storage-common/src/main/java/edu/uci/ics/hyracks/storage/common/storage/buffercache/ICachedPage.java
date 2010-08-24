package edu.uci.ics.hyracks.storage.common.storage.buffercache;

import java.nio.ByteBuffer;

public interface ICachedPage {
    public ByteBuffer getBuffer();

    public void acquireReadLatch();

    public void releaseReadLatch();

    public void acquireWriteLatch();

    public void releaseWriteLatch();
}