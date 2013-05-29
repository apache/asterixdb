package edu.uci.ics.asterix.common.context;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;

public class AsterixVirtualBufferCacheProvider implements IVirtualBufferCacheProvider {

    private static final long serialVersionUID = 1L;
    private final int datasetID;

    public AsterixVirtualBufferCacheProvider(int datasetID) {
        this.datasetID = datasetID;
    }

    @Override
    public IVirtualBufferCache getVirtualBufferCache(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getVirtualBufferCache(datasetID);
    }

}
