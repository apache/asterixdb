package edu.uci.ics.hyracks.examples.btree.helper;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexArtifactMap;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexArtifactMapProvider;

public class IndexArtifactMapProvider implements IIndexArtifactMapProvider {
    private static final long serialVersionUID = 1L;

    public static final IndexArtifactMapProvider INSTANCE = new IndexArtifactMapProvider();

    @Override
    public IIndexArtifactMap getIndexArtifactMap(IHyracksTaskContext ctx) {
        return RuntimeContext.get(ctx).getIndexArtifactMap();
    }
}
