package edu.uci.ics.hyracks.storage.common.storage.buffercache;

public interface IPageReplacementStrategy {
    public Object createPerPageStrategyObject(int cpid);

    public void setBufferCache(IBufferCacheInternal bufferCache);

    public void notifyCachePageReset(ICachedPageInternal cPage);

    public void notifyCachePageAccess(ICachedPageInternal cPage);

    public ICachedPageInternal findVictim();
}