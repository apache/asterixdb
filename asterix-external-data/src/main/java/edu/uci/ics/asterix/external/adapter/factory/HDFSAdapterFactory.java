package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;

public class HDFSAdapterFactory implements IExternalDatasetAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType atype) throws Exception {
        HDFSAdapter hdfsAdapter = new HDFSAdapter(atype);
        hdfsAdapter.configure(configuration);
        return hdfsAdapter;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.EXTERNAL_DATASET;
    }

    @Override
    public String getName() {
        return "hdfs";
    }

}
