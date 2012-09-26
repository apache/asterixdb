/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.common.file;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class IndexLocalResourceRepository implements ILocalResourceRepository {

    private final List<ILocalResourceClass> resourceClasses;
    private final List<String> mountPoints;
    private static final String METADATA_FILE_NAME = ".metadata";
    private Map<String, ILocalResource> name2ResourceMap = new HashMap<String, ILocalResource>();
    private Map<Long, ILocalResource> id2ResourceMap = new HashMap<Long, ILocalResource>();

    public IndexLocalResourceRepository(List<ILocalResourceClass> resourceClasses, List<String> mountPoints,
            String rootDir) throws HyracksDataException {
        this.resourceClasses = resourceClasses;
        this.mountPoints = mountPoints;

        File rootFile = new File(this.mountPoints.get(0), rootDir);
        if (!rootFile.exists()) {
            throw new HyracksDataException(rootFile.getAbsolutePath() + "doesn't exist.");
        }

        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.equalsIgnoreCase(METADATA_FILE_NAME)) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        File[] childFileList = rootFile.listFiles();
        for (File childFile : childFileList) {
            if (childFile.isDirectory()) {
                File[] targetFileList = childFile.listFiles(filter);
                for (File targetFile : targetFileList) {
                    ILocalResource localResource = readLocalResource(targetFile);
                    id2ResourceMap.put(localResource.getResourceId(), localResource);
                    name2ResourceMap.put(localResource.getResourceName(), localResource);
                }
            }
        }
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
        id2ResourceMap.put(id, resource);
        name2ResourceMap.put(resource.getResourceName(), resource);

        FileOutputStream fos = null;
        ObjectOutputStream oosToFos = null;
        try {
            fos = new FileOutputStream(getFileName(mountPoints.get(0), resource.getResourceName()));
            oosToFos = new ObjectOutputStream(fos);
            byte[] outputBytes = resource.getResourceClass().serialize(resource);
            oosToFos.writeInt(resource.getResourceClass().getResourceClassId());
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
        List<ILocalResource> resources = new ArrayList<ILocalResource>();
        for (ILocalResource resource : id2ResourceMap.values()) {
            resources.add(resource);
        }
        return resources;
    }

    private String getFileName(String mountPoint, String baseDir) {

        String fileName = new String(mountPoint);

        if (!baseDir.endsWith(System.getProperty("file.separator"))) {
            baseDir += System.getProperty("file.separator");
        }
        fileName += baseDir + METADATA_FILE_NAME;

        return fileName;
    }

    private ILocalResource readLocalResource(File file) throws HyracksDataException {
        FileInputStream fis = null;
        ObjectInputStream oisFromFis = null;
        ByteArrayInputStream bais = null;
        ObjectInputStream oisFromBais = null;
        ILocalResource resource = null;

        try {
            fis = new FileInputStream(file);
            oisFromFis = new ObjectInputStream(fis);

            int resourceClassId = oisFromFis.readInt();
            int inputLength = oisFromFis.readInt();
            byte[] inputBytes = new byte[inputLength];
            if (inputLength != oisFromFis.read(inputBytes)) {
                throw new HyracksDataException("Corrupted file");
            }

            bais = new ByteArrayInputStream(inputBytes);
            oisFromBais = new ObjectInputStream(bais);
            for (ILocalResourceClass resourceClass : resourceClasses) {
                if (resourceClass.getResourceClassId() == resourceClassId) {
                    resource = resourceClass.deserialize(inputBytes);
                    break;
                }
            }
            return resource;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        } finally {
            if (oisFromFis != null) {
                try {
                    oisFromFis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                } finally {
                    closeInputStream(oisFromBais, bais);
                }
            }
            if (oisFromFis == null && fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                } finally {
                    closeInputStream(oisFromBais, bais);
                }
            }
        }
    }

    private void closeInputStream(ObjectInputStream oisFromBais, ByteArrayInputStream bais) throws HyracksDataException {
        if (oisFromBais != null) {
            try {
                oisFromBais.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        if (oisFromBais == null && bais != null) {
            try {
                bais.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }
}
