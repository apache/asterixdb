package edu.uci.ics.hyracks.storage.common.file;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ResourceIdFactoryFactory {
    private ILocalResourceRepository localResourceRepository;

    public ResourceIdFactoryFactory(ILocalResourceRepository localResourceRepository) {
        this.localResourceRepository = localResourceRepository;
    }

    public ResourceIdFactory createResourceIdFactory() throws HyracksDataException {
        List<ILocalResource> localResources = localResourceRepository.getAllResources();
        long largestResourceId = 0;
        for (ILocalResource localResource : localResources) {
            if (largestResourceId < localResource.getResourceId()) {
                largestResourceId = localResource.getResourceId();
            }
        }
        return new ResourceIdFactory(largestResourceId);
    }
}
