package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;

public interface IExternalDatasetAdapterFactory extends IAdapterFactory {

    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType sourceType) throws Exception;
    
}
