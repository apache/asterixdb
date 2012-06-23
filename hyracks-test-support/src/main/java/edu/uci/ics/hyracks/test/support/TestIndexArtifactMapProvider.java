package edu.uci.ics.hyracks.test.support;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexArtifactMap;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexArtifactMapProvider;

public class TestIndexArtifactMapProvider implements IIndexArtifactMapProvider {

    private static final long serialVersionUID = 1L;

    public static final TestIndexArtifactMapProvider INSTANCE = new TestIndexArtifactMapProvider();

    @Override
    public IIndexArtifactMap getIndexArtifactMap(IHyracksTaskContext ctx) {
        return TestStorageManagerComponentHolder.getIndexArtifactMap(ctx);
    }
}
