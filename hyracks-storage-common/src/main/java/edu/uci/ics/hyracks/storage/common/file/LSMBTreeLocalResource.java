package edu.uci.ics.hyracks.storage.common.file;

public class LSMBTreeLocalResource implements ILocalResource {
    private static final long serialVersionUID = -8638308062094620884L;
    private final long resourceId;
    private final String resourceName;
    private final Object object;
    private final LSMBTreeLocalResourceClass resourceClass;

    public LSMBTreeLocalResource(long resourceId, String resourceName, Object object,
            LSMBTreeLocalResourceClass resourceClass) {
        this.resourceId = resourceId;
        this.resourceName = resourceName;
        this.object = object;
        this.resourceClass = resourceClass;
    }

    @Override
    public long getResourceId() {
        return resourceId;
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public Object getResourceObject() {
        return object;
    }

    @Override
    public ILocalResourceClass getResourceClass() {
        return resourceClass;
    }

}
