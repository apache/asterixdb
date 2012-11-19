package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter;
import edu.uci.ics.asterix.om.types.IAType;

public class NCFileSystemAdapterFactory implements IExternalDatasetAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType atype) throws Exception {
        NCFileSystemAdapter fsAdapter = new NCFileSystemAdapter(atype);
        fsAdapter.configure(configuration);
        return fsAdapter;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.EXTERNAL_DATASET;
    }

    @Override
    public String getName() {
        return "localfs";
    }
}
