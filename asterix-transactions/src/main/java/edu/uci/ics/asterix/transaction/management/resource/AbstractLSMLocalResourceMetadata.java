package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;

public abstract class AbstractLSMLocalResourceMetadata implements ILocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    protected final int datasetID;

    public AbstractLSMLocalResourceMetadata(int datasetID) {
        this.datasetID = datasetID;
    }

    public int getDatasetID() {
        return datasetID;
    }
}
