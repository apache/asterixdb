package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDataflowHelper;

public class InvertedIndexDataflowHelperFactory implements IIndexDataflowHelperFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public IndexDataflowHelper createIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition) {
        return new InvertedIndexDataflowHelper(opDesc, ctx, partition);
    }

}
