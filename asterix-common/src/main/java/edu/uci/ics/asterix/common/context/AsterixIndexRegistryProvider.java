package edu.uci.ics.asterix.common.context;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;

public class AsterixIndexRegistryProvider implements IIndexRegistryProvider<IIndex> {

    private static final long serialVersionUID = 1L;

    public static final AsterixIndexRegistryProvider INSTANCE = new AsterixIndexRegistryProvider();

    private AsterixIndexRegistryProvider() {
    }

    @Override
    public IndexRegistry<IIndex> getRegistry(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getIndexRegistry();
    }

}
