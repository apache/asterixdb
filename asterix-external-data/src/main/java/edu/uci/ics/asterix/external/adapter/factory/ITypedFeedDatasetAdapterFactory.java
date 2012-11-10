package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;

public interface ITypedFeedDatasetAdapterFactory extends IFeedDatasetAdapterFactory {

    public IDatasourceAdapter createAdapter(Map<String, String> configuration) throws Exception;

}
