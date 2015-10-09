/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.transaction.management.resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    private static final Logger LOGGER = Logger.getLogger(PersistentLocalResourceRepository.class.getName());
    private final String[] mountPoints;
    private static final String ROOT_METADATA_DIRECTORY = "asterix_root_metadata";
    private static final String ROOT_METADATA_FILE_NAME_PREFIX = ".asterix_root_metadata";
    private static final long ROOT_LOCAL_RESOURCE_ID = -4321;
    private static final String METADATA_FILE_NAME = ".metadata";
    private final Cache<String, LocalResource> resourceCache;
    private final String nodeId;
    private static final int MAX_CACHED_RESOURCES = 1000;

    public PersistentLocalResourceRepository(List<IODeviceHandle> devices, String nodeId) throws HyracksDataException {
        mountPoints = new String[devices.size()];
        this.nodeId = nodeId;
        for (int i = 0; i < mountPoints.length; i++) {
            String mountPoint = devices.get(i).getPath().getPath();
            File mountPointDir = new File(mountPoint);
            if (!mountPointDir.exists()) {
                throw new HyracksDataException(mountPointDir.getAbsolutePath() + " doesn't exist.");
            }
            if (!mountPoint.endsWith(System.getProperty("file.separator"))) {
                mountPoints[i] = new String(mountPoint + System.getProperty("file.separator"));
            } else {
                mountPoints[i] = new String(mountPoint);
            }
        }

        resourceCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHED_RESOURCES).build();
    }

    private String prepareRootMetaDataFileName(String mountPoint, String nodeId, int ioDeviceId) {
        return mountPoint + ROOT_METADATA_DIRECTORY + File.separator + nodeId + "_" + "iodevice" + ioDeviceId;
    }

    public void initialize(String nodeId, String rootDir) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Initializing local resource repository ... ");
        }

        //if the rootMetadataFile doesn't exist, create it.
        for (int i = 0; i < mountPoints.length; i++) {
            String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                    + ROOT_METADATA_FILE_NAME_PREFIX;
            File rootMetadataFile = new File(rootMetadataFileName);

            File rootMetadataDir = new File(prepareRootMetaDataFileName(mountPoints[i], nodeId, i));
            if (!rootMetadataDir.exists()) {
                boolean success = rootMetadataDir.mkdirs();
                if (!success) {
                    throw new IllegalStateException(
                            "Unable to create root metadata directory of PersistentLocalResourceRepository in "
                                    + rootMetadataDir.getAbsolutePath());
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
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Completed the initialization of the local resource repository");
        }
    }

    @Override
    public LocalResource getResourceByName(String name) throws HyracksDataException {
        LocalResource resource = resourceCache.getIfPresent(name);
        if (resource == null) {
            File resourceFile = getLocalResourceFileByName(name);
            if (resourceFile.exists()) {
                resource = readLocalResource(resourceFile);
                resourceCache.put(name, resource);
            }
        }
        return resource;
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        File resourceFile = new File(getFileName(resource.getResourceName(), resource.getResourceId()));

        if (resourceFile.exists()) {
            throw new HyracksDataException("Duplicate resource");
        }

        if (resource.getResourceId() != ROOT_LOCAL_RESOURCE_ID) {
            resourceCache.put(resource.getResourceName(), resource);
        }

        FileOutputStream fos = null;
        ObjectOutputStream oosToFos = null;

        try {
            fos = new FileOutputStream(resourceFile);
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
    public synchronized void deleteResourceByName(String name) throws HyracksDataException {
        File resourceFile = getLocalResourceFileByName(name);
        if (resourceFile.exists()) {
            resourceFile.delete();
            resourceCache.invalidate(name);
        } else {
            throw new HyracksDataException("Resource doesn't exist");
        }
    }

    private static File getLocalResourceFileByName(String resourceName) {
        return new File(resourceName + File.separator + METADATA_FILE_NAME);
    }

    public HashMap<Long, LocalResource> loadAndGetAllResources() throws HyracksDataException {
        //TODO During recovery, the memory usage currently is proportional to the number of resources available.
        //This could be fixed by traversing all resources on disk until the required resource is found.
        HashMap<Long, LocalResource> resourcesMap = new HashMap<Long, LocalResource>();

        for (int i = 0; i < mountPoints.length; i++) {
            String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                    + ROOT_METADATA_FILE_NAME_PREFIX;
            File rootMetadataFile = new File(rootMetadataFileName);
            if (!rootMetadataFile.exists()) {
                continue;
            }
            //if the rootMetadataFile exists, read it and set it as mounting point root
            LocalResource rootLocalResource = readLocalResource(rootMetadataFile);
            String mountedRootDir = (String) rootLocalResource.getResourceObject();

            File rootDirFile = new File(mountedRootDir);
            if (!rootDirFile.exists()) {
                //rootDir may not exist if this node is not the metadata node and doesn't have any user data.
                continue;
            }

            //load all local resources.
            File[] dataverseFileList = rootDirFile.listFiles();
            if (dataverseFileList != null) {
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
                                                File[] metadataFiles = ioDeviceFile.listFiles(METADATA_FILES_FILTER);
                                                if (metadataFiles != null) {
                                                    for (File metadataFile : metadataFiles) {
                                                        LocalResource localResource = readLocalResource(metadataFile);
                                                        resourcesMap.put(localResource.getResourceId(), localResource);
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

        return resourcesMap;
    }

    @Override
    public long getMaxResourceID() throws HyracksDataException {
        long maxResourceId = 0;

        for (int i = 0; i < mountPoints.length; i++) {
            String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                    + ROOT_METADATA_FILE_NAME_PREFIX;
            File rootMetadataFile = new File(rootMetadataFileName);
            if (!rootMetadataFile.exists()) {
                continue;
            }

            //if the rootMetadataFile exists, read it and set it as mounting point root
            LocalResource rootLocalResource = readLocalResource(rootMetadataFile);
            String mountedRootDir = (String) rootLocalResource.getResourceObject();

            File rootDirFile = new File(mountedRootDir);
            if (!rootDirFile.exists()) {
                continue;
            }

            //traverse all local resources.
            File[] dataverseFileList = rootDirFile.listFiles();
            if (dataverseFileList != null) {
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
                                                File[] metadataFiles = ioDeviceFile.listFiles(METADATA_FILES_FILTER);
                                                if (metadataFiles != null) {
                                                    for (File metadataFile : metadataFiles) {
                                                        LocalResource localResource = readLocalResource(metadataFile);
                                                        maxResourceId = Math.max(maxResourceId,
                                                                localResource.getResourceId());
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

        return maxResourceId;
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

    private static final FilenameFilter METADATA_FILES_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            if (name.equalsIgnoreCase(METADATA_FILE_NAME)) {
                return true;
            } else {
                return false;
            }
        }
    };
}
