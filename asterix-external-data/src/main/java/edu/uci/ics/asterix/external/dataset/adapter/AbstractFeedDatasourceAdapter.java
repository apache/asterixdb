package edu.uci.ics.asterix.external.dataset.adapter;

import edu.uci.ics.asterix.om.types.ARecordType;

public abstract class AbstractFeedDatasourceAdapter extends AbstractDatasourceAdapter implements IFeedDatasourceAdapter {

    protected AdapterDataFlowType adapterDataFlowType;
    protected ARecordType adapterOutputType;

    public AdapterDataFlowType getAdapterDataFlowType() {
        return adapterDataFlowType;
    }

    public ARecordType getAdapterOutputType() {
        return adapterOutputType;
    }

}
