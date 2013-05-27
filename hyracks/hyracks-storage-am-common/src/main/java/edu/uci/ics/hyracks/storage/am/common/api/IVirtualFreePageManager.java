package edu.uci.ics.hyracks.storage.am.common.api;

public interface IVirtualFreePageManager extends IFreePageManager {
    public int getCapacity();

    public void reset();
}
