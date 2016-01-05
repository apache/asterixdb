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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.replication.AsterixReplicationJob;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationJobType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    private static final Logger LOGGER = Logger.getLogger(PersistentLocalResourceRepository.class.getName());
    private final String[] mountPoints;
    private static final String STORAGE_METADATA_DIRECTORY = "asterix_root_metadata";
    private static final String STORAGE_METADATA_FILE_NAME_PREFIX = ".asterix_root_metadata";
    private static final long STORAGE_LOCAL_RESOURCE_ID = -4321;
    public static final String METADATA_FILE_NAME = ".metadata";
    private final Cache<String, LocalResource> resourceCache;
    private final String nodeId;
    private static final int MAX_CACHED_RESOURCES = 1000;
    private IReplicationManager replicationManager;
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;

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

    private static String getStorageMetadataDirPath(String mountPoint, String nodeId, int ioDeviceId) {
        return mountPoint + STORAGE_METADATA_DIRECTORY + File.separator + nodeId + "_" + "iodevice" + ioDeviceId;
    }

    private static File getStorageMetadataBaseDir(File storageMetadataFile) {
        //STORAGE_METADATA_DIRECTORY / Node Id / STORAGE_METADATA_FILE_NAME_PREFIX
        return storageMetadataFile.getParentFile().getParentFile();
    }

    public void initializeNewUniverse(String storageRootDirName) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Initializing local resource repository ... ");
        }

        //create storage metadata file (This file is used to locate the root storage directory after instance restarts).
        //TODO with the existing cluster configuration file being static and distributed on all NCs, we can find out the storage root
        //directory without looking at this file. This file could potentially store more information, otherwise no need to keep it. 
        for (int i = 0; i < mountPoints.length; i++) {
            File storageMetadataFile = getStorageMetadataFile(mountPoints[i], nodeId, i);
            File storageMetadataDir = storageMetadataFile.getParentFile();
            //make dirs for the storage metadata file
            boolean success = storageMetadataDir.mkdirs();
            if (!success) {
                throw new IllegalStateException(
                        "Unable to create storage metadata directory of PersistentLocalResourceRepository in "
                                + storageMetadataDir.getAbsolutePath() + " or directory already exists");
            }

            LOGGER.log(Level.INFO,
                    "created the root-metadata-file's directory: " + storageMetadataDir.getAbsolutePath());

            String storageRootDirPath;
            if (storageRootDirName.startsWith(System.getProperty("file.separator"))) {
                storageRootDirPath = new String(
                        mountPoints[i] + storageRootDirName.substring(System.getProperty("file.separator").length()));
            } else {
                storageRootDirPath = new String(mountPoints[i] + storageRootDirName);
            }

            LocalResource rootLocalResource = new LocalResource(STORAGE_LOCAL_RESOURCE_ID,
                    storageMetadataFile.getAbsolutePath(), 0, storageMetadataFile.getAbsolutePath(), 0,
                    storageRootDirPath);
            insert(rootLocalResource);
            LOGGER.log(Level.INFO, "created the root-metadata-file: " + storageMetadataFile.getAbsolutePath());
        }
        LOGGER.log(Level.INFO, "Completed the initialization of the local resource repository");
    }

    @Override
    public LocalResource getResourceByPath(String path) throws HyracksDataException {
        LocalResource resource = resourceCache.getIfPresent(path);
        if (resource == null) {
            File resourceFile = getLocalResourceFileByName(path);
            if (resourceFile.exists()) {
                resource = readLocalResource(resourceFile);
                resourceCache.put(path, resource);
            }
        }
        return resource;
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        File resourceFile = new File(getFileName(resource.getResourcePath(), resource.getResourceId()));
        if (resourceFile.exists()) {
            throw new HyracksDataException("Duplicate resource: " + resourceFile.getAbsolutePath());
        } else {
            resourceFile.getParentFile().mkdirs();
        }

        if (resource.getResourceId() != STORAGE_LOCAL_RESOURCE_ID) {
            resourceCache.put(resource.getResourcePath(), resource);
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

            //if replication enabled, send resource metadata info to remote nodes
            if (isReplicationEnabled && resource.getResourceId() != STORAGE_LOCAL_RESOURCE_ID) {
                String filePath = getFileName(resource.getResourcePath(), resource.getResourceId());
                createReplicationJob(ReplicationOperation.REPLICATE, filePath);
            }
        }
    }

    @Override
    public synchronized void deleteResourceByPath(String resourcePath) throws HyracksDataException {
        File resourceFile = getLocalResourceFileByName(resourcePath);
        if (resourceFile.exists()) {
            resourceFile.delete();
            resourceCache.invalidate(resourcePath);

            //if replication enabled, delete resource from remote replicas
            if (isReplicationEnabled && !resourceFile.getName().startsWith(STORAGE_METADATA_FILE_NAME_PREFIX)) {
                createReplicationJob(ReplicationOperation.DELETE, resourceFile.getAbsolutePath());
            }
        } else {
            throw new HyracksDataException("Resource doesn't exist");
        }
    }

    private static File getLocalResourceFileByName(String resourcePath) {
        return new File(resourcePath + File.separator + METADATA_FILE_NAME);
    }

    public HashMap<Long, LocalResource> loadAndGetAllResources() throws HyracksDataException {
        //TODO During recovery, the memory usage currently is proportional to the number of resources available.
        //This could be fixed by traversing all resources on disk until the required resource is found.
        HashMap<Long, LocalResource> resourcesMap = new HashMap<Long, LocalResource>();

        for (int i = 0; i < mountPoints.length; i++) {
            File storageRootDir = getStorageRootDirectoryIfExists(mountPoints[i], nodeId, i);
            if (storageRootDir == null) {
                continue;
            }

            //load all local resources.
            File[] partitions = storageRootDir.listFiles();
            for (File partition : partitions) {
                File[] dataverseFileList = partition.listFiles();
                if (dataverseFileList != null) {
                    for (File dataverseFile : dataverseFileList) {
                        if (dataverseFile.isDirectory()) {
                            File[] indexFileList = dataverseFile.listFiles();
                            if (indexFileList != null) {
                                for (File indexFile : indexFileList) {
                                    if (indexFile.isDirectory()) {
                                        File[] metadataFiles = indexFile.listFiles(METADATA_FILES_FILTER);
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

        return resourcesMap;
    }

    @Override
    public long getMaxResourceID() throws HyracksDataException {
        long maxResourceId = 0;

        for (int i = 0; i < mountPoints.length; i++) {
            File storageRootDir = getStorageRootDirectoryIfExists(mountPoints[i], nodeId, i);
            if (storageRootDir == null) {
                continue;
            }

            //load all local resources.
            File[] partitions = storageRootDir.listFiles();
            for (File partition : partitions) {
                //traverse all local resources.
                File[] dataverseFileList = partition.listFiles();
                if (dataverseFileList != null) {
                    for (File dataverseFile : dataverseFileList) {
                        if (dataverseFile.isDirectory()) {
                            File[] indexFileList = dataverseFile.listFiles();
                            if (indexFileList != null) {
                                for (File indexFile : indexFileList) {
                                    if (indexFile.isDirectory()) {
                                        File[] metadataFiles = indexFile.listFiles(METADATA_FILES_FILTER);
                                        if (metadataFiles != null) {
                                            for (File metadataFile : metadataFiles) {
                                                LocalResource localResource = readLocalResource(metadataFile);
                                                maxResourceId = Math.max(maxResourceId, localResource.getResourceId());
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

    private static String getFileName(String baseDir, long resourceId) {
        if (resourceId == STORAGE_LOCAL_RESOURCE_ID) {
            return baseDir;
        } else {
            if (!baseDir.endsWith(System.getProperty("file.separator"))) {
                baseDir += System.getProperty("file.separator");
            }
            return new String(baseDir + METADATA_FILE_NAME);
        }
    }

    public static LocalResource readLocalResource(File file) throws HyracksDataException {
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

    public void setReplicationManager(IReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        isReplicationEnabled = replicationManager.isReplicationEnabled();

        if (isReplicationEnabled) {
            filesToBeReplicated = new HashSet<String>();
        }
    }

    private void createReplicationJob(ReplicationOperation operation, String filePath) throws HyracksDataException {
        filesToBeReplicated.clear();
        filesToBeReplicated.add(filePath);
        AsterixReplicationJob job = new AsterixReplicationJob(ReplicationJobType.METADATA, operation,
                ReplicationExecutionType.SYNC, filesToBeReplicated);
        try {
            replicationManager.submitJob(job);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public String[] getStorageMountingPoints() {
        return mountPoints;
    }

    /**
     * Deletes physical files of all data verses.
     *
     * @param deleteStorageMetadata
     * @throws IOException
     */
    public void deleteStorageData(boolean deleteStorageMetadata) throws IOException {
        for (int i = 0; i < mountPoints.length; i++) {
            File storageDir = getStorageRootDirectoryIfExists(mountPoints[i], nodeId, i);
            if (storageDir != null) {
                if (storageDir.isDirectory()) {
                    FileUtils.deleteDirectory(storageDir);
                }
            }

            if (deleteStorageMetadata) {
                //delete the metadata root directory
                File storageMetadataFile = getStorageMetadataFile(mountPoints[i], nodeId, i);
                File storageMetadataDir = getStorageMetadataBaseDir(storageMetadataFile);
                if (storageMetadataDir.exists() && storageMetadataDir.isDirectory()) {
                    FileUtils.deleteDirectory(storageMetadataDir);
                }
            }
        }
    }

    /**
     * @param mountPoint
     * @param nodeId
     * @param ioDeviceId
     * @return A file reference to the storage metadata file.
     */
    private static File getStorageMetadataFile(String mountPoint, String nodeId, int ioDeviceId) {
        String storageMetadataFileName = getStorageMetadataDirPath(mountPoint, nodeId, ioDeviceId) + File.separator
                + STORAGE_METADATA_FILE_NAME_PREFIX;
        File storageMetadataFile = new File(storageMetadataFileName);
        return storageMetadataFile;
    }

    /**
     * @param mountPoint
     * @param nodeId
     * @param ioDeviceId
     * @return A file reference to the storage root directory if exists, otherwise null.
     * @throws HyracksDataException
     */
    public static File getStorageRootDirectoryIfExists(String mountPoint, String nodeId, int ioDeviceId)
            throws HyracksDataException {
        File storageRootDir = null;
        File storageMetadataFile = getStorageMetadataFile(mountPoint, nodeId, ioDeviceId);
        if (storageMetadataFile.exists()) {
            LocalResource rootLocalResource = readLocalResource(storageMetadataFile);
            String storageRootDirPath = (String) rootLocalResource.getResourceObject();
            Path path = Paths.get(storageRootDirPath);
            if (Files.exists(path)) {
                storageRootDir = new File(storageRootDirPath);
            }
        }
        return storageRootDir;
    }
}