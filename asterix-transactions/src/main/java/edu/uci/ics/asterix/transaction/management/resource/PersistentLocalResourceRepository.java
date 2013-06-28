/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    private static final Logger LOGGER = Logger.getLogger(PersistentLocalResourceRepository.class.getName());
    private final String[] mountPoints;
    private static final String ROOT_METADATA_DIRECTORY = "asterix_root_metadata";
    private static final String ROOT_METADATA_FILE_NAME_PREFIX = ".asterix_root_metadata";
    private static final long ROOT_LOCAL_RESOURCE_ID = -4321;
    private static final String METADATA_FILE_NAME = ".metadata";
    private Map<String, LocalResource> name2ResourceMap = new HashMap<String, LocalResource>();
    private Map<Long, LocalResource> id2ResourceMap = new HashMap<Long, LocalResource>();
    private final int numIODevices;

    public PersistentLocalResourceRepository(List<IODeviceHandle> devices) throws HyracksDataException {
        numIODevices = devices.size();
        this.mountPoints = new String[numIODevices];
        for (int i = 0; i < numIODevices; i++) {
            String mountPoint = devices.get(i).getPath().getPath();
            File mountPointDir = new File(mountPoint);
            if (!mountPointDir.exists()) {
                throw new HyracksDataException(mountPointDir.getAbsolutePath() + "doesn't exist.");
            }
            if (!mountPoint.endsWith(System.getProperty("file.separator"))) {
                mountPoints[i] = new String(mountPoint + System.getProperty("file.separator"));
            } else {
                mountPoints[i] = new String(mountPoint);
            }
        }
    }

    private String prepareRootMetaDataFileName(String mountPoint, String nodeId, int ioDeviceId) {
        return mountPoint + ROOT_METADATA_DIRECTORY + File.separator + nodeId + "_" + "iodevice" + ioDeviceId;
    }

    public void initialize(String nodeId, String rootDir, boolean isNewUniverse, ResourceIdFactory resourceIdFactory)
            throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Initializing local resource repository ... ");
        }

        if (isNewUniverse) {
            //#. if the rootMetadataFile doesn't exist, create it and return.
            for (int i = 0; i < numIODevices; i++) {
                String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                        + ROOT_METADATA_FILE_NAME_PREFIX;
                File rootMetadataFile = new File(rootMetadataFileName);

                File rootMetadataDir = new File(prepareRootMetaDataFileName(mountPoints[i], nodeId, i));
                if (!rootMetadataDir.exists()) {
                    boolean success = rootMetadataDir.mkdirs();
                    if (!success) {
                        if (LOGGER.isLoggable(Level.SEVERE)) {
                            LOGGER.severe("Unable to create root metadata directory"
                                    + rootMetadataDir.getAbsolutePath());
                        }
                    }
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("created the root-metadata-file's directory: " + rootMetadataDir.getAbsolutePath());
                    }
                }

                rootMetadataFile.delete();
                String mountedRootDir;
                if (rootDir.startsWith(System.getProperty("file.separator"))) {
                    mountedRootDir = new String(mountPoints[i]
                            + rootDir.substring(System.getProperty("file.separator").length()));
                } else {
                    mountedRootDir = new String(mountPoints[i] + rootDir);
                }
                LocalResource rootLocalResource = new LocalResource(ROOT_LOCAL_RESOURCE_ID, rootMetadataFileName, 0, 0,
                        mountedRootDir);
                insert(rootLocalResource);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("created the root-metadata-file: " + rootMetadataFileName);
                }

                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Completed the initialization of the local resource repository");
                }
            }
            return;
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
        for (int i = 0; i < numIODevices; i++) {
            String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                    + ROOT_METADATA_FILE_NAME_PREFIX;
            File rootMetadataFile = new File(rootMetadataFileName);
            //#. if the rootMetadataFile exists, read it and set this.rootDir.
            LocalResource rootLocalResource = readLocalResource(rootMetadataFile);
            String mountedRootDir = (String) rootLocalResource.getResourceObject();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("The root directory of the local resource repository is " + mountedRootDir);
            }

            //#. load all local resources. 
            File rootDirFile = new File(mountedRootDir);
            if (!rootDirFile.exists()) {
                //rootDir may not exist if this node is not the metadata node and doesn't have any user data.
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("The root directory of the local resource repository doesn't exist: there is no local resource.");
                    LOGGER.info("Completed the initialization of the local resource repository");
                }
                continue;
            }

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
                                File[] ioDevicesList = indexFile.listFiles();
                                if (ioDevicesList != null) {
                                    for (File ioDeviceFile : ioDevicesList) {
                                        if (ioDeviceFile.isDirectory()) {
                                            File[] metadataFiles = ioDeviceFile.listFiles(filter);
                                            if (metadataFiles != null) {
                                                for (File metadataFile : metadataFiles) {
                                                    LocalResource localResource = readLocalResource(metadataFile);
                                                    id2ResourceMap.put(localResource.getResourceId(), localResource);
                                                    name2ResourceMap
                                                            .put(localResource.getResourceName(), localResource);
                                                    maxResourceId = Math.max(localResource.getResourceId(),
                                                            maxResourceId);
                                                    if (LOGGER.isLoggable(Level.INFO)) {
                                                        LOGGER.info("loaded local resource - [id: "
                                                                + localResource.getResourceId() + ", name: "
                                                                + localResource.getResourceName() + "]");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        resourceIdFactory.initId(maxResourceId + 1);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("The resource id factory is intialized with the value: " + (maxResourceId + 1));
            LOGGER.info("Completed the initialization of the local resource repository");
        }
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
            fos = new FileOutputStream(getFileName(resource.getResourceName(), resource.getResourceId()));
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
        File file = new File(getFileName(resource.getResourceName(), resource.getResourceId()));
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
        File file = new File(getFileName(resource.getResourceName(), resource.getResourceId()));
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

    private String getFileName(String baseDir, long resourceId) {

        if (resourceId == ROOT_LOCAL_RESOURCE_ID) {
            return baseDir;
        } else {
            if (!baseDir.endsWith(System.getProperty("file.separator"))) {
                baseDir += System.getProperty("file.separator");
            }
            String fileName = new String(baseDir + METADATA_FILE_NAME);
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
