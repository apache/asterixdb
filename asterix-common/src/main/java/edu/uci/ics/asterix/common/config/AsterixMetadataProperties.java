package edu.uci.ics.asterix.common.config;

import java.util.Map;
import java.util.Set;

public class AsterixMetadataProperties extends AbstractAsterixProperties {

    public AsterixMetadataProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public String getMetadataNodeName() {
        return accessor.getMetadataNodeName();
    }

    public String getMetadataStore() {
        return accessor.getMetadataStore();
    }

    public Map<String, String[]> getStores() {
        return accessor.getStores();
    }

    public Set<String> getNodeNames() {
        return accessor.getNodeNames();
    }

}
