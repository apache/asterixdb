package edu.uci.ics.hyracks.storage.common.storage.buffercache;

public interface IBufferCacheInternal extends IBufferCache {
    public ICachedPageInternal getPage(int cpid);
}