package edu.uci.ics.hyracks.storage.common.storage.buffercache;

public interface ICachedPageInternal extends ICachedPage {
    public int getCachedPageId();

    public Object getReplacementStrategyObject();

    public boolean pinIfGoodVictim();
}