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
package edu.uci.ics.asterix.transaction.management.resource;

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
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    private final String mountPoint;
    private static final String ROOT_METADATA_DIRECTORY = "asterix_root_metadata/";
    private static final String ROOT_METADATA_FILE_NAME_PREFIX = ".asterix_root_metadata_";
    private static final long ROOT_LOCAL_RESOURCE_ID = -4321;
    private static final String METADATA_FILE_NAME = ".metadata";
    private Map<String, LocalResource> name2ResourceMap = new HashMap<String, LocalResource>();
    private Map<Long, LocalResource> id2ResourceMap = new HashMap<Long, LocalResource>();
    private String rootMetadataFileName;
    private String rootDir;

    public PersistentLocalResourceRepository(String mountPoint) throws HyracksDataException {
        File mountPointDir = new File(mountPoint);
        if (!mountPointDir.exists()) {
            throw new HyracksDataException(mountPointDir.getAbsolutePath() + "doesn't exist.");
        }
        if (!mountPoint.endsWith(System.getProperty("file.separator"))) {
            this.mountPoint = new String(mountPoint + System.getProperty("file.separator"));
        } else {
            this.mountPoint = new String(mountPoint);
        }
    }

    public void initialize(String nodeId, String rootDir, boolean isNewUniverse, ResourceIdFactory resourceIdFactory)
            throws HyracksDataException {
        LocalResource rootLocalResource = null;

        //#. if the rootMetadataFile doesn't exist, create it and return.
        rootMetadataFileName = new String(mountPoint + ROOT_METADATA_DIRECTORY + ROOT_METADATA_FILE_NAME_PREFIX
                + nodeId);
        File rootMetadataFile = new File(rootMetadataFileName);
        if (isNewUniverse) {
            File rootMetadataDir = new File(mountPoint + ROOT_METADATA_DIRECTORY);
            if (!rootMetadataDir.exists()) {
                rootMetadataDir.mkdir();
            }

            rootMetadataFile.delete();
            if (rootDir.startsWith(System.getProperty("file.separator"))) {
                this.rootDir = new String(mountPoint + rootDir.substring(System.getProperty("file.separator").length()));
            } else {
                this.rootDir = new String(mountPoint + rootDir);
            }
            rootLocalResource = new LocalResource(ROOT_LOCAL_RESOURCE_ID, rootMetadataFileName, 0, 0, this.rootDir);
            insert(rootLocalResource);
            return;
        }

        //#. if the rootMetadataFile exists, read it and set this.rootDir.
        rootLocalResource = readLocalResource(rootMetadataFile);
        this.rootDir = (String) rootLocalResource.getResourceObject();

        //#. load all local resources. 
        File rootDirFile = new File(this.rootDir);
        if (!rootDirFile.exists()) {
            throw new HyracksDataException(rootDirFile.getAbsolutePath() + "doesn't exist.");
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

        long maxResourceId = 0;
        File[] dataverseFileList = rootDirFile.listFiles();
        if (dataverseFileList == null) {
            throw new HyracksDataException("Metadata dataverse doesn't exist.");
        }
        for (File dataverseFile : dataverseFileList) {
            if (dataverseFile.isDirectory()) {
                File[] indexFileList = dataverseFile.listFiles();
                if (indexFileList != null) {
                    for (File indexFile : indexFileList) {
                        if (indexFile.isDirectory()) {
                            File[] metadataFiles = indexFile.listFiles(filter);
                            if (metadataFiles != null) {
                                for (File metadataFile : metadataFiles) {
                                    LocalResource localResource = readLocalResource(metadataFile);
                                    id2ResourceMap.put(localResource.getResourceId(), localResource);
                                    name2ResourceMap.put(localResource.getResourceName(), localResource);
                                    maxResourceId = Math.max(localResource.getResourceId(), maxResourceId);
                                }
                            }
                        }
                    }
                }
            }
        }
        resourceIdFactory.initId(maxResourceId + 1);
    }

    @Override
    public LocalResource getResourceById(long id) throws HyracksDataException {
        return id2ResourceMap.get(id);
    }

    @Override
    public LocalResource getResourceByName(String name) throws HyracksDataException {
        return name2ResourceMap.get(name);
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        long id = resource.getResourceId();

        if (id2ResourceMap.containsKey(id)) {
            throw new HyracksDataException("Duplicate resource");
        }

        if (resource.getResourceId() != ROOT_LOCAL_RESOURCE_ID) {
            id2ResourceMap.put(id, resource);
            name2ResourceMap.put(resource.getResourceName(), resource);
        }

        FileOutputStream fos = null;
        ObjectOutputStream oosToFos = null;
        try {
            fos = new FileOutputStream(getFileName(mountPoint, resource.getResourceName(), resource.getResourceId()));
            oosToFos = new ObjectOutputStream(fos);
            oosToFos.writeObject(resource);
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
        LocalResource resource = id2ResourceMap.get(id);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(id);
        name2ResourceMap.remove(resource.getResourceName());
        File file = new File(getFileName(mountPoint, resource.getResourceName(), resource.getResourceId()));
        file.delete();
    }

    @Override
    public synchronized void deleteResourceByName(String name) throws HyracksDataException {
        LocalResource resource = name2ResourceMap.get(name);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(resource.getResourceId());
        name2ResourceMap.remove(name);
        File file = new File(getFileName(mountPoint, resource.getResourceName(), resource.getResourceId()));
        file.delete();
    }

    @Override
    public List<LocalResource> getAllResources() throws HyracksDataException {
        List<LocalResource> resources = new ArrayList<LocalResource>();
        for (LocalResource resource : id2ResourceMap.values()) {
            resources.add(resource);
        }
        return resources;
    }

    private String getFileName(String mountPoint, String baseDir, long resourceId) {

        if (resourceId == ROOT_LOCAL_RESOURCE_ID) {
            return baseDir;
        } else {
            String fileName = new String(mountPoint);
            if (!baseDir.endsWith(System.getProperty("file.separator"))) {
                baseDir += System.getProperty("file.separator");
            }
            fileName += baseDir + METADATA_FILE_NAME;
            return fileName;
        }
    }

    private LocalResource readLocalResource(File file) throws HyracksDataException {
        FileInputStream fis = null;
        ObjectInputStream oisFromFis = null;

        try {
            fis = new FileInputStream(file);
            oisFromFis = new ObjectInputStream(fis);
            LocalResource resource = (LocalResource) oisFromFis.readObject();
            return resource;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            if (oisFromFis != null) {
                try {
                    oisFromFis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (oisFromFis == null && fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }
}
