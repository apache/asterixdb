package edu.uci.ics.hyracks.test.support;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ICloseableResource;
import edu.uci.ics.hyracks.storage.am.common.api.ICloseableResourceManager;

public enum TestCloseableResourceManager implements ICloseableResourceManager {
    INSTANCE;

    @Override
    public void addCloseableResource(long contextID, ICloseableResource closeableResource) {
        // close the resource immediately
        try {
            closeableResource.close();
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void closeAll(long contextID) {
        // Do nothing.
    }

}
