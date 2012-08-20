package edu.uci.ics.hyracks.test.support;

import edu.uci.ics.hyracks.storage.am.common.api.ICloseableResourceManager;
import edu.uci.ics.hyracks.storage.am.common.api.ICloseableResourceManagerProvider;

public enum TestCloseableResourceManagerProvider implements ICloseableResourceManagerProvider {
    INSTANCE;

    @Override
    public ICloseableResourceManager getCloseableResourceManager() {
        return TestCloseableResourceManager.INSTANCE;
    }

}
