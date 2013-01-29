package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public class PersistentLocalResourceFactoryProvider implements ILocalResourceFactoryProvider {

    private static final long serialVersionUID = 1L;
    private final ILocalResourceMetadata localResourceMetadata;
    private final int resourceType;

    public PersistentLocalResourceFactoryProvider(ILocalResourceMetadata localResourceMetadata, int resourceType) {
        this.localResourceMetadata = localResourceMetadata;
        this.resourceType = resourceType;
    }

    @Override
    public ILocalResourceFactory getLocalResourceFactory() {
        return new PersistentLocalResourceFactory(localResourceMetadata, resourceType);
    }
}
