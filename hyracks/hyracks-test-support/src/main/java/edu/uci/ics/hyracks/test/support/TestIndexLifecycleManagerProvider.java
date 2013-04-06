package edu.uci.ics.hyracks.test.support;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;

public class TestIndexLifecycleManagerProvider implements IIndexLifecycleManagerProvider {

    private static final long serialVersionUID = 1L;

    @Override
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return TestStorageManagerComponentHolder.getIndexLifecycleManager(ctx);
    }

}
