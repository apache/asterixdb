package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;

public interface IGenericFeedDatasetAdapterFactory extends IFeedDatasetAdapterFactory {

    public static final String KEY_TYPE_NAME="output-type-name";
    
    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType atype) throws Exception;

}
