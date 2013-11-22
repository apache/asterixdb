package edu.uci.ics.asterix.metadata.entities;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

public class Library implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String dataverse;
    private final String name;

    public Library(String dataverseName, String libraryName) {
        this.dataverse = dataverseName;
        this.name = libraryName;
    }

    public String getDataverseName() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addLibraryIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropLibrary(this);
    }

}
