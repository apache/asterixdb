package edu.uci.ics.hyracks.storage.am.common.api;

public interface ICloseableResourceManager {
    public void addCloseableResource(long contextID, ICloseableResource closeableResource);

    public void closeAll(long contextID);
}