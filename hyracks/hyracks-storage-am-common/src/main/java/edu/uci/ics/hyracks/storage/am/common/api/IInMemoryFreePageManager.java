package edu.uci.ics.hyracks.storage.am.common.api;

public interface IInMemoryFreePageManager extends IFreePageManager {
    public int getCapacity();

    public void reset();

    public boolean isFull();
}
