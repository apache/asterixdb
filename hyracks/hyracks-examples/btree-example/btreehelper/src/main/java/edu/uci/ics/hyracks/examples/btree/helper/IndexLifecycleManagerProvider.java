package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;

public enum IndexLifecycleManagerProvider implements IIndexLifecycleManagerProvider {
    INSTANCE;

    @Override
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getIndexLifecycleManager();
    }

}
