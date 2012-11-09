package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.HiveAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;

public class HiveAdapterFactory implements IExternalDatasetAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType type) throws Exception {
        HiveAdapter hiveAdapter = new HiveAdapter(type);
        hiveAdapter.configure(configuration);
        return hiveAdapter;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.EXTERNAL_DATASET;
    }

    @Override
    public String getName() {
        return "hive";
    }
}
