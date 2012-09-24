package edu.uci.ics.hyracks.storage.common.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class IndexLocalResourceRepository implements ILocalResourceRepository {

    private final IIOManager ioManager;
    private final List<String> mountPoints;
    private final String rootDir;
    private static final String METADATA_FILE_NAME = ".metadata";
    private Map<String, ILocalResource> name2ResourceMap = new HashMap<String, ILocalResource>();
    private Map<Long, ILocalResource> id2ResourceMap = new HashMap<Long, ILocalResource>();

    public IndexLocalResourceRepository(IIOManager ioManager, List<String> mountPoints, String rootDir)
            throws HyracksDataException {
        this.ioManager = ioManager;
        this.mountPoints = mountPoints;
        this.rootDir = rootDir;

        //TODO initialize the Maps
    }

    @Override
    public ILocalResource getResourceById(long id) throws HyracksDataException {
        return id2ResourceMap.get(id);
    }

    @Override
    public ILocalResource getResourceByName(String name) throws HyracksDataException {
        return id2ResourceMap.get(name);
    }

    @Override
    public synchronized void insert(ILocalResource resource) throws HyracksDataException {
        long id = resource.getResourceId();

        if (id2ResourceMap.containsKey(id)) {
            throw new HyracksDataException("Duplicate resource");
        }
        ILocalResource resourceClone = cloneResource(resource);
        id2ResourceMap.put(id, resourceClone);
        name2ResourceMap.put(resource.getResourceName(), resourceClone);

        FileOutputStream fos = null;
        ObjectOutputStream oosToFos = null;
        try {
            fos = new FileOutputStream(getFileName(mountPoints.get(0), resource.getResourceName()));
            oosToFos = new ObjectOutputStream(fos);
            byte[] outputBytes = resource.getResourceClass().serialize(resource);
            oosToFos.writeInt(outputBytes.length);
            oosToFos.write(outputBytes);
            oosToFos.flush();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        } finally {
            if (oosToFos != null) {
                try {
                    oosToFos.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (oosToFos == null && fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    @Override
    public synchronized void deleteResourceById(long id) throws HyracksDataException {
        ILocalResource resource = id2ResourceMap.get(id);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(id);
        name2ResourceMap.remove(resource.getResourceName());
        File file = new File(getFileName(mountPoints.get(0), resource.getResourceName()));
        file.delete();
    }

    @Override
    public synchronized void deleteResourceByName(String name) throws HyracksDataException {
        ILocalResource resource = id2ResourceMap.get(name);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(name);
        name2ResourceMap.remove(resource.getResourceId());
        File file = new File(getFileName(mountPoints.get(0), resource.getResourceName()));
        file.delete();
    }

    @Override
    public List<ILocalResource> getAllResources() throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    private String getFileName(String mountPoint, String baseDir) {

        String fileName = new String(mountPoint);

        if (!fileName.endsWith(System.getProperty("file.separator"))) {
            fileName += System.getProperty("file.separator");
        }
        if (!baseDir.endsWith(System.getProperty("file.separate"))) {
            baseDir += System.getProperty("file.separator");
        }
        fileName += baseDir + METADATA_FILE_NAME;

        return fileName;
    }

    private ILocalResource cloneResource(ILocalResource resource) {
        switch (resource.getResourceClass().getResourceClassId()) {
            case ILocalResourceClass.LSMBTree:
                return new LSMBTreeLocalResource();//TODO change the constructor appropriately
            default:
                throw new IllegalArgumentException();
        }
    }
}
