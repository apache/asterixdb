package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactory;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

public class PersistentLocalResourceFactory implements ILocalResourceFactory {
    
    private final ILocalResourceMetadata localResourceMetadata;
    private final int resourceType;

    public PersistentLocalResourceFactory(ILocalResourceMetadata localResourceMetadata, int resourceType) {
        this.localResourceMetadata = localResourceMetadata;
        this.resourceType = resourceType;
    }

    @Override
    public LocalResource createLocalResource(long resourceId, String resourceName, int partition) {
        return new LocalResource(resourceId, resourceName, partition, resourceType, localResourceMetadata);
    }
}
