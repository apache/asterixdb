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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.ReplicationJob;
import org.apache.asterix.common.storage.IndexFileProperties;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationJobType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    // Public constants
    public static final String METADATA_FILE_NAME = ".metadata";
    // Private constants
    private static final Logger LOGGER = Logger.getLogger(PersistentLocalResourceRepository.class.getName());
    private static final String STORAGE_METADATA_DIRECTORY = StorageConstants.METADATA_ROOT;
    private static final String STORAGE_METADATA_FILE_NAME_PREFIX = "." + StorageConstants.METADATA_ROOT;
    private static final int MAX_CACHED_RESOURCES = 1000;
    private static final FilenameFilter METADATA_FILES_FILTER =
            (File dir, String name) -> name.equalsIgnoreCase(METADATA_FILE_NAME);
    // Finals
    private final IIOManager ioManager;
    private final String[] mountPoints;
    private final String nodeId;
    private final Cache<String, LocalResource> resourceCache;
    private final SortedMap<Integer, ClusterPartition> clusterPartitions;
    private final Set<Integer> nodeOriginalPartitions;
    private final Set<Integer> nodeActivePartitions;
    // Mutables
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;
    private IReplicationManager replicationManager;
    private Set<Integer> nodeInactivePartitions;

    public PersistentLocalResourceRepository(IIOManager ioManager, List<IODeviceHandle> devices, String nodeId,
            MetadataProperties metadataProperties) throws HyracksDataException {
        this.ioManager = ioManager;
        mountPoints = new String[devices.size()];
        this.nodeId = nodeId;
        this.clusterPartitions = metadataProperties.getClusterPartitions();
        for (int i = 0; i < mountPoints.length; i++) {
            String mountPoint = devices.get(i).getMount().getPath();
            File mountPointDir = new File(mountPoint);
            if (!mountPointDir.exists()) {
                throw new HyracksDataException(mountPointDir.getAbsolutePath() + " doesn't exist.");
            }
            if (!mountPoint.endsWith(File.separator)) {
                mountPoints[i] = mountPoint + File.separator;
            } else {
                mountPoints[i] = mountPoint;
            }
        }
        resourceCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHED_RESOURCES).build();

        ClusterPartition[] nodePartitions = metadataProperties.getNodePartitions().get(nodeId);
        //initially the node active partitions are the same as the original partitions
        nodeOriginalPartitions = new HashSet<>(nodePartitions.length);
        nodeActivePartitions = new HashSet<>(nodePartitions.length);
        for (ClusterPartition partition : nodePartitions) {
            nodeOriginalPartitions.add(partition.getPartitionId());
            nodeActivePartitions.add(partition.getPartitionId());
        }
    }

    @Override
    public String toString() {
        StringBuilder aString = new StringBuilder().append(PersistentLocalResourceRepository.class.getSimpleName())
                .append(Character.LINE_SEPARATOR).append(ioManager.getClass().getSimpleName()).append(':')
                .append(Character.LINE_SEPARATOR).append(ioManager.toString()).append(Character.LINE_SEPARATOR)
                .append("Cached Resources:").append(Character.LINE_SEPARATOR);
        for (Entry<String, LocalResource> pair : resourceCache.asMap().entrySet()) {
            aString.append(pair.getKey()).append("->").append(pair.getValue()).append(Character.LINE_SEPARATOR);
        }
        return aString.toString();
    }

    public void initializeNewUniverse(String storageRoot) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Initializing local resource repository ... ");
        }
        /*
         * create storage metadata file
         * (This file is used to locate the root storage directory after instance restarts).
         * TODO with the existing cluster configuration file being static and distributed on all NCs
         * we can find out the storage root directory without looking at this file.
         * This file could potentially store more information, otherwise no need to keep it.
         */
        String storageRootDirName = storageRoot;
        while (storageRootDirName.startsWith(File.separator)) {
            storageRootDirName = storageRootDirName.substring(File.separator.length());
        }
        for (int i = 0; i < mountPoints.length; i++) {
            FileReference storageMetadataFile = getStorageMetadataFile(ioManager, nodeId, i);
            File storageMetadataDir = storageMetadataFile.getFile().getParentFile();
            if (storageMetadataDir.exists()) {
                throw HyracksDataException.create(ErrorCode.ROOT_LOCAL_RESOURCE_EXISTS, getClass().getSimpleName(),
                        storageMetadataDir.getAbsolutePath());
            }
            //make dirs for the storage metadata file
            boolean success = storageMetadataDir.mkdirs();
            if (!success) {
                throw HyracksDataException.create(ErrorCode.ROOT_LOCAL_RESOURCE_COULD_NOT_BE_CREATED,
                        getClass().getSimpleName(), storageMetadataDir.getAbsolutePath());
            }
            LOGGER.log(Level.INFO,
                    "created the root-metadata-file's directory: " + storageMetadataDir.getAbsolutePath());
            try (FileOutputStream fos = new FileOutputStream(storageMetadataFile.getFile())) {
                fos.write(storageRootDirName.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            LOGGER.log(Level.INFO, "created the root-metadata-file: " + storageMetadataFile.getAbsolutePath());
        }
        LOGGER.log(Level.INFO, "Completed the initialization of the local resource repository");
    }

    @Override
    public LocalResource get(String relativePath) throws HyracksDataException {
        LocalResource resource = resourceCache.getIfPresent(relativePath);
        if (resource == null) {
            FileReference resourceFile = getLocalResourceFileByName(ioManager, relativePath);
            if (resourceFile.getFile().exists()) {
                resource = readLocalResource(resourceFile.getFile());
                resourceCache.put(relativePath, resource);
            }
        }
        return resource;
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        String relativePath = getFileName(resource.getPath());
        FileReference resourceFile = ioManager.resolve(relativePath);
        if (resourceFile.getFile().exists()) {
            throw new HyracksDataException("Duplicate resource: " + resourceFile.getAbsolutePath());
        } else {
            resourceFile.getFile().getParentFile().mkdirs();
        }
        resourceCache.put(resource.getPath(), resource);

        try (FileOutputStream fos = new FileOutputStream(resourceFile.getFile());
                ObjectOutputStream oosToFos = new ObjectOutputStream(fos)) {
            oosToFos.writeObject(resource);
            oosToFos.flush();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

        //if replication enabled, send resource metadata info to remote nodes
        if (isReplicationEnabled) {
            createReplicationJob(ReplicationOperation.REPLICATE, resourceFile);
        }
    }

    @Override
    public synchronized void delete(String relativePath) throws HyracksDataException {
        FileReference resourceFile = getLocalResourceFileByName(ioManager, relativePath);
        if (resourceFile.getFile().exists()) {
            try {
                // Invalidate before deleting the file just in case file deletion throws some exception.
                // Since it's just a cache invalidation, it should not affect correctness.
                resourceCache.invalidate(relativePath);
                resourceFile.delete();
            } finally {
                // Regardless of successfully deleted or not, the operation should be replicated.
                //if replication enabled, delete resource from remote replicas
                if (isReplicationEnabled
                        && !resourceFile.getFile().getName().startsWith(STORAGE_METADATA_FILE_NAME_PREFIX)) {
                    createReplicationJob(ReplicationOperation.DELETE, resourceFile);
                }
            }
        } else {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.RESOURCE_DOES_NOT_EXIST,
                    relativePath);
        }
    }

    private static FileReference getLocalResourceFileByName(IIOManager ioManager, String resourcePath)
            throws HyracksDataException {
        String fileName = resourcePath + File.separator + METADATA_FILE_NAME;
        return ioManager.resolve(fileName);
    }

    public Map<Long, LocalResource> loadAndGetAllResources() throws IOException {
        //TODO During recovery, the memory usage currently is proportional to the number of resources available.
        //This could be fixed by traversing all resources on disk until the required resource is found.
        LOGGER.log(Level.INFO, "Loading all resources");
        Map<Long, LocalResource> resourcesMap = new HashMap<>();
        for (int i = 0; i < mountPoints.length; i++) {
            File storageRootDir = getStorageRootDirectoryIfExists(ioManager, nodeId, i);
            if (storageRootDir == null) {
                LOGGER.log(Level.INFO, "Getting storage root dir returned null. Returning");
                continue;
            }
            LOGGER.log(Level.INFO, "Getting storage root dir returned " + storageRootDir.getAbsolutePath());
            //load all local resources.
            File[] partitions = storageRootDir.listFiles();
            LOGGER.log(Level.INFO, "Number of partitions found = " + partitions.length);
            for (File partition : partitions) {
                File[] dataverseFileList = partition.listFiles();
                LOGGER.log(Level.INFO, "Reading partition = " + partition.getName() + ". Number of dataverses found: "
                        + dataverseFileList.length);
                if (dataverseFileList != null) {
                    for (File dataverseFile : dataverseFileList) {
                        loadDataverse(dataverseFile, resourcesMap);
                    }
                }
            }
        }
        return resourcesMap;
    }

    private void loadDataverse(File dataverseFile, Map<Long, LocalResource> resourcesMap) throws HyracksDataException {
        LOGGER.log(Level.INFO, "Loading dataverse:" + dataverseFile.getName());
        if (dataverseFile.isDirectory()) {
            File[] indexFileList = dataverseFile.listFiles();
            if (indexFileList != null) {
                for (File indexFile : indexFileList) {
                    loadIndex(indexFile, resourcesMap);
                }
            }
        }
    }

    private void loadIndex(File indexFile, Map<Long, LocalResource> resourcesMap) throws HyracksDataException {
        LOGGER.log(Level.INFO, "Loading index:" + indexFile.getName());
        if (indexFile.isDirectory()) {
            File[] metadataFiles = indexFile.listFiles(METADATA_FILES_FILTER);
            if (metadataFiles != null) {
                for (File metadataFile : metadataFiles) {
                    LocalResource localResource = readLocalResource(metadataFile);
                    LOGGER.log(Level.INFO, "Resource loaded " + localResource.getId() + ":" + localResource.getPath());
                    resourcesMap.put(localResource.getId(), localResource);
                }
            }
        }
    }

    @Override
    public long maxId() throws HyracksDataException {
        long maxResourceId = 0;
        for (int i = 0; i < mountPoints.length; i++) {
            File storageRootDir = getStorageRootDirectoryIfExists(ioManager, nodeId, i);
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
                        maxResourceId = getMaxResourceIdForDataverse(dataverseFile, maxResourceId);
                    }
                }
            }
        }
        return maxResourceId;
    }

    private long getMaxResourceIdForDataverse(File dataverseFile, long maxSoFar) throws HyracksDataException {
        long maxResourceId = maxSoFar;
        if (dataverseFile.isDirectory()) {
            File[] indexFileList = dataverseFile.listFiles();
            if (indexFileList != null) {
                for (File indexFile : indexFileList) {
                    maxResourceId = getMaxResourceIdForIndex(indexFile, maxResourceId);
                }
            }
        }
        return maxResourceId;
    }

    private long getMaxResourceIdForIndex(File indexFile, long maxSoFar) throws HyracksDataException {
        long maxResourceId = maxSoFar;
        if (indexFile.isDirectory()) {
            File[] metadataFiles = indexFile.listFiles(METADATA_FILES_FILTER);
            if (metadataFiles != null) {
                for (File metadataFile : metadataFiles) {
                    LocalResource localResource = readLocalResource(metadataFile);
                    maxResourceId = Math.max(maxResourceId, localResource.getId());
                }
            }
        }
        return maxResourceId;
    }

    private static String getFileName(String path) {
        return path.endsWith(File.separator) ? (path + METADATA_FILE_NAME)
                : (path + File.separator + METADATA_FILE_NAME);
    }

    public static LocalResource readLocalResource(File file) throws HyracksDataException {
        try (FileInputStream fis = new FileInputStream(file);
                ObjectInputStream oisFromFis = new ObjectInputStream(fis)) {
            LocalResource resource = (LocalResource) oisFromFis.readObject();
            if (resource.getVersion() == ITreeIndexFrame.Constants.VERSION) {
                return resource;
            } else {
                throw new AsterixException("Storage version mismatch.");
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public void setReplicationManager(IReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        isReplicationEnabled = replicationManager.isReplicationEnabled();

        if (isReplicationEnabled) {
            filesToBeReplicated = new HashSet<>();
            nodeInactivePartitions = ConcurrentHashMap.newKeySet();
        }
    }

    private void createReplicationJob(ReplicationOperation operation, FileReference fileRef)
            throws HyracksDataException {
        /**
         * Durable resources path format:
         * /partition/dataverse/idx/fileName
         * Temporary resources path format:
         * /partition/TEMP_DATASETS_STORAGE_FOLDER/dataverse/idx/fileName
         */
        String[] fileNameTokens = fileRef.getAbsolutePath().split(File.separator);
        String partitionDir = fileNameTokens[fileNameTokens.length - 4];
        //exclude temporary datasets resources
        if (!partitionDir.equals(StoragePathUtil.TEMP_DATASETS_STORAGE_FOLDER)) {
            filesToBeReplicated.clear();
            filesToBeReplicated.add(fileRef.getAbsolutePath());
            ReplicationJob job = new ReplicationJob(ReplicationJobType.METADATA, operation,
                    ReplicationExecutionType.SYNC, filesToBeReplicated);
            try {
                replicationManager.submitJob(job);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
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
            File storageDir = getStorageRootDirectoryIfExists(ioManager, nodeId, i);
            if (storageDir != null && storageDir.isDirectory()) {
                FileUtils.deleteDirectory(storageDir);
            }
            if (deleteStorageMetadata) {
                //delete the metadata root directory
                FileReference storageMetadataFile = getStorageMetadataFile(ioManager, nodeId, i);
                File storageMetadataDir = storageMetadataFile.getFile().getParentFile().getParentFile();
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
    private static FileReference getStorageMetadataFile(IIOManager ioManager, String nodeId, int ioDeviceId) {
        String storageMetadataFileName = STORAGE_METADATA_DIRECTORY + File.separator + nodeId + "_" + "iodevice"
                + ioDeviceId + File.separator + STORAGE_METADATA_FILE_NAME_PREFIX;
        return new FileReference(ioManager.getIODevices().get(ioDeviceId), storageMetadataFileName);
    }

    /**
     * @param mountPoint
     * @param nodeId
     * @param ioDeviceId
     * @return A file reference to the storage root directory if exists, otherwise null.
     * @throws HyracksDataException
     */
    public static File getStorageRootDirectoryIfExists(IIOManager ioManager, String nodeId, int ioDeviceId)
            throws HyracksDataException {
        try {
            FileReference storageMetadataFile = getStorageMetadataFile(ioManager, nodeId, ioDeviceId);
            LOGGER.log(Level.INFO, "Storage metadata file is " + storageMetadataFile.getAbsolutePath());
            if (storageMetadataFile.getFile().exists()) {
                String storageRootDirPath =
                        new String(Files.readAllBytes(storageMetadataFile.getFile().toPath()), StandardCharsets.UTF_8);
                LOGGER.log(Level.INFO, "Storage metadata file found and root dir is " + storageRootDirPath);
                FileReference storageRootFileRef =
                        new FileReference(ioManager.getIODevices().get(ioDeviceId), storageRootDirPath);
                if (storageRootFileRef.getFile().exists()) {
                    return storageRootFileRef.getFile();
                } else {
                    LOGGER.log(Level.INFO, "Storage root doesn't exist");
                }
            } else {
                LOGGER.log(Level.INFO, "Storage metadata file doesn't exist");
            }
            return null;
        } catch (IOException ioe) {
            throw HyracksDataException.create(ioe);
        }
    }

    /**
     * @param partition
     * @return The partition local path on this NC.
     */
    public String getPartitionPath(int partition) {
        //currently each partition is replicated on the same IO device number on all NCs.
        return mountPoints[getIODeviceNum(partition)];
    }

    public int getIODeviceNum(int partition) {
        return clusterPartitions.get(partition).getIODeviceNum();
    }

    public Set<Integer> getActivePartitions() {
        return Collections.unmodifiableSet(nodeActivePartitions);
    }

    public Set<Integer> getInactivePartitions() {
        return Collections.unmodifiableSet(nodeInactivePartitions);
    }

    public Set<Integer> getNodeOrignalPartitions() {
        return Collections.unmodifiableSet(nodeOriginalPartitions);
    }

    public synchronized void addActivePartition(int partitonId) {
        nodeActivePartitions.add(partitonId);
        nodeInactivePartitions.remove(partitonId);
    }

    public synchronized void addInactivePartition(int partitonId) {
        nodeInactivePartitions.add(partitonId);
        nodeActivePartitions.remove(partitonId);
    }

    private static String getLocalResourceRelativePath(String absolutePath) {
        final String[] tokens = absolutePath.split(File.separator);
        // Format: storage_dir/partition/dataverse/idx
        return tokens[tokens.length - 5] + File.separator + tokens[tokens.length - 4] + File.separator
                + tokens[tokens.length - 3] + File.separator + tokens[tokens.length - 2];
    }

    public IndexFileProperties getIndexFileRef(String absoluteFilePath) throws HyracksDataException {
        //TODO pass relative path
        final String[] tokens = absoluteFilePath.split(File.separator);
        if (tokens.length < 5) {
            throw new HyracksDataException("Invalid file format");
        }
        String fileName = tokens[tokens.length - 1];
        String index = tokens[tokens.length - 2];
        String dataverse = tokens[tokens.length - 3];
        String partition = tokens[tokens.length - 4];
        int partitionId = StoragePathUtil.getPartitionNumFromName(partition);
        String relativePath = getLocalResourceRelativePath(absoluteFilePath);
        final LocalResource lr = get(relativePath);
        int datasetId = lr == null ? -1 : ((DatasetLocalResource) lr.getResource()).getDatasetId();
        return new IndexFileProperties(partitionId, dataverse, index, fileName, datasetId);
    }
}
