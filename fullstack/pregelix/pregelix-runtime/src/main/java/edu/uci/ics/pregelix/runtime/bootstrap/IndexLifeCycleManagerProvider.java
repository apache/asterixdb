package edu.uci.ics.pregelix.runtime.bootstrap;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;

public class IndexLifeCycleManagerProvider implements IIndexLifecycleManagerProvider {

    private static final long serialVersionUID = 1L;

    public static final IIndexLifecycleManagerProvider INSTANCE = new IndexLifeCycleManagerProvider();

    private IndexLifeCycleManagerProvider() {
    }

    @Override
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getIndexLifecycleManager();
    }

}
