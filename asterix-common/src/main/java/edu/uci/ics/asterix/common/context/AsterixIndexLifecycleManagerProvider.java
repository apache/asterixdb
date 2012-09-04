package edu.uci.ics.asterix.common.context;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;

public enum AsterixIndexLifecycleManagerProvider implements IIndexLifecycleManagerProvider {
    INSTANCE;

    @Override
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getIndexLifecycleManager();
    }

}
