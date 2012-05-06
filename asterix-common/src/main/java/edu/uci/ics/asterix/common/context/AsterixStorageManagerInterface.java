package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.common.api.INodeApplicationState;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class AsterixStorageManagerInterface implements IStorageManagerInterface {
    private static final long serialVersionUID = 1L;

    public static AsterixStorageManagerInterface INSTANCE = new AsterixStorageManagerInterface();

    @Override
    public IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        INodeApplicationState applicationState = (INodeApplicationState) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
        return applicationState.getApplicationRuntimeContext().getBufferCache();
    }

    @Override
    public IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        INodeApplicationState applicationState = (INodeApplicationState) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
        return applicationState.getApplicationRuntimeContext().getFileMapManager();
    }
}
