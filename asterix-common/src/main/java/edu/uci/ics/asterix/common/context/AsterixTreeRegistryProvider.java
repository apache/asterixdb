package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.common.api.INodeApplicationState;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;

public class AsterixTreeRegistryProvider implements IIndexRegistryProvider<IIndex> {

    private static final long serialVersionUID = 1L;

    public static final AsterixTreeRegistryProvider INSTANCE = new AsterixTreeRegistryProvider();

    private AsterixTreeRegistryProvider() {
    }

    @Override
    public IndexRegistry<IIndex> getRegistry(IHyracksTaskContext ctx) {
        INodeApplicationState applicationState = (INodeApplicationState) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
        return applicationState.getApplicationRuntimeContext().getIndexRegistry();
    }

}
