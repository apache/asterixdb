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

import static org.apache.asterix.common.storage.ResourceReference.getComponentSequence;
import static org.apache.asterix.common.utils.StorageConstants.INDEX_NON_DATA_FILES_PREFIX;
import static org.apache.asterix.common.utils.StorageConstants.METADATA_FILE_NAME;
import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;
import static org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_CREATE_FILE;
import static org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager.COMPONENT_FILES_FILTER;
import static org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager.UNINITIALIZED_COMPONENT_SEQ;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.ReplicationJob;
import org.apache.asterix.common.storage.DatasetCopyIdentifier;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.storage.ResourceStorageStats;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationJobType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String METADATA_FILE_MASK_NAME =
            StorageConstants.MASK_FILE_PREFIX + StorageConstants.METADATA_FILE_NAME;
    private static final FilenameFilter LSM_INDEX_FILES_FILTER =
            (dir, name) -> name.startsWith(METADATA_FILE_NAME) || !name.startsWith(INDEX_NON_DATA_FILES_PREFIX);
    private static final FilenameFilter MASK_FILES_FILTER =
            (dir, name) -> name.startsWith(StorageConstants.MASK_FILE_PREFIX);
    private static final FilenameFilter METADATA_FILES_FILTER =
            (dir, name) -> name.equals(StorageConstants.METADATA_FILE_NAME);
    private static final FilenameFilter METADATA_MASK_FILES_FILTER =
            (dir, name) -> name.equals(METADATA_FILE_MASK_NAME);

    private static final int MAX_CACHED_RESOURCES = 1000;

    // Finals
    private final IIOManager ioManager;
    private final Cache<String, LocalResource> resourceCache;
    // Mutables
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;
    private IReplicationManager replicationManager;
    private final List<FileReference> storageRoots;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    private final IPersistedResourceRegistry persistedResourceRegistry;

    public PersistentLocalResourceRepository(IIOManager ioManager,
            IIndexCheckpointManagerProvider indexCheckpointManagerProvider,
            IPersistedResourceRegistry persistedResourceRegistry) {
        this.ioManager = ioManager;
        this.indexCheckpointManagerProvider = indexCheckpointManagerProvider;
        this.persistedResourceRegistry = persistedResourceRegistry;
        storageRoots = new ArrayList<>();
        final List<IODeviceHandle> ioDevices = ioManager.getIODevices();
        for (int i = 0; i < ioDevices.size(); i++) {
            storageRoots.add(new FileReference(ioDevices.get(i), STORAGE_ROOT_DIR_NAME));
        }
        createStorageRoots();
        resourceCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHED_RESOURCES).build();
    }

    @Override
    public String toString() {
        StringBuilder aString = new StringBuilder().append(PersistentLocalResourceRepository.class.getSimpleName())
                .append(Character.LINE_SEPARATOR).append(ioManager.getClass().getSimpleName()).append(':')
                .append(Character.LINE_SEPARATOR).append(ioManager).append(Character.LINE_SEPARATOR)
                .append("Cached Resources:").append(Character.LINE_SEPARATOR);
        resourceCache.asMap().forEach(
                (key, value) -> aString.append(key).append("->").append(value).append(Character.LINE_SEPARATOR));
        return aString.toString();
    }

    @Override
    public synchronized LocalResource get(String relativePath) throws HyracksDataException {
        LocalResource resource = resourceCache.getIfPresent(relativePath);
        if (resource == null) {
            FileReference resourceFile = getLocalResourceFileByName(ioManager, relativePath);
            resource = readLocalResource(resourceFile);
            if (resource != null) {
                resourceCache.put(relativePath, resource);
            }
        }
        return resource;
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public void insert(LocalResource resource) throws HyracksDataException {
        FileReference resourceFile;
        synchronized (this) {
            String relativePath = getFileName(resource.getPath());
            resourceFile = ioManager.resolve(relativePath);
            if (resourceFile.getFile().exists()) {
                throw new HyracksDataException("Duplicate resource: " + resourceFile.getAbsolutePath());
            }

            final FileReference parent = resourceFile.getParent();
            if (!ioManager.exists(parent) && !ioManager.makeDirectories(parent)) {
                throw HyracksDataException.create(CANNOT_CREATE_FILE, parent.getAbsolutePath());
            }
            // The next block should be all or nothing
            try {
                createResourceFileMask(resourceFile);
                byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(resource.toJson(persistedResourceRegistry));
                ioManager.overwrite(resourceFile, bytes);
                indexCheckpointManagerProvider.get(DatasetResourceReference.of(resource)).init(
                        UNINITIALIZED_COMPONENT_SEQ, 0, LSMComponentId.EMPTY_INDEX_LAST_COMPONENT_ID.getMaxId(), null);
                deleteResourceFileMask(resourceFile);
            } catch (Exception e) {
                cleanup(resourceFile);
                throw HyracksDataException.create(e);
            } catch (Throwable th) {
                LOGGER.error("Error creating resource {}", resourceFile, th);
                ExitUtil.halt(ExitUtil.EC_ERROR_CREATING_RESOURCES);
            }
            resourceCache.put(resource.getPath(), resource);
        }
        // do not do the replication operation on the synchronized to avoid blocking other threads
        // on network operations
        //if replication enabled, send resource metadata info to remote nodes
        if (isReplicationEnabled) {
            try {
                createReplicationJob(ReplicationOperation.REPLICATE, resourceFile);
            } catch (Exception e) {
                LOGGER.error("failed to send resource file {} to replicas", resourceFile);
            }
        }
    }

    @SuppressWarnings("squid:S1181")
    private void cleanup(FileReference resourceFile) {
        if (resourceFile.getFile().exists()) {
            try {
                ioManager.delete(resourceFile);
            } catch (Throwable th) {
                LOGGER.error("Error cleaning up corrupted resource {}", resourceFile, th);
                ExitUtil.halt(ExitUtil.EC_FAILED_TO_DELETE_CORRUPTED_RESOURCES);
            }
        }
    }

    @Override
    public void delete(String relativePath) throws HyracksDataException {
        FileReference resourceFile = getLocalResourceFileByName(ioManager, relativePath);
        final LocalResource localResource = readLocalResource(resourceFile);

        boolean resourceExists = localResource != null;
        if (isReplicationEnabled && resourceExists) {
            try {
                createReplicationJob(ReplicationOperation.DELETE, resourceFile);
            } catch (Exception e) {
                LOGGER.error("failed to delete resource file {} from replicas", resourceFile);
            }
        }
        synchronized (this) {
            try {
                if (resourceExists) {
                    ioManager.delete(resourceFile);
                    // delete all checkpoints
                    indexCheckpointManagerProvider.get(DatasetResourceReference.of(localResource)).delete();
                } else {
                    throw HyracksDataException
                            .create(org.apache.hyracks.api.exceptions.ErrorCode.RESOURCE_DOES_NOT_EXIST, relativePath);
                }
            } finally {
                invalidateResource(relativePath);
            }
        }
    }

    public static FileReference getLocalResourceFileByName(IIOManager ioManager, String resourcePath)
            throws HyracksDataException {
        String fileName = resourcePath + File.separator + StorageConstants.METADATA_FILE_NAME;
        return ioManager.resolve(fileName);
    }

    public synchronized Map<Long, LocalResource> getResources(Predicate<LocalResource> filter,
            List<FileReference> roots) throws HyracksDataException {
        Map<Long, LocalResource> resourcesMap = new HashMap<>();
        for (FileReference root : roots) {
            final Collection<FileReference> files = ioManager.list(root, METADATA_FILES_FILTER);
            try {
                for (FileReference file : files) {
                    final LocalResource localResource = readLocalResource(file);
                    if (localResource != null && filter.test(localResource)) {
                        LocalResource duplicate = resourcesMap.putIfAbsent(localResource.getId(), localResource);
                        if (duplicate != null) {
                            LOGGER.warn("found duplicate resource ids {} and {}", localResource, duplicate);
                        }
                    }
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
        return resourcesMap;
    }

    public synchronized Map<Long, LocalResource> getResources(Predicate<LocalResource> filter)
            throws HyracksDataException {
        return getResources(filter, storageRoots);
    }

    public synchronized Map<Long, LocalResource> getResources(Predicate<LocalResource> filter, Set<Integer> partitions)
            throws HyracksDataException {
        List<FileReference> partitionsRoots = new ArrayList<>();
        for (Integer partition : partitions) {
            partitionsRoots.add(getPartitionRoot(partition));
        }
        return getResources(filter, partitionsRoots);
    }

    public synchronized void deleteInvalidIndexes(Predicate<LocalResource> filter) throws HyracksDataException {
        for (FileReference root : storageRoots) {
            final Collection<FileReference> files = ioManager.list(root, METADATA_FILES_FILTER);
            try {
                for (FileReference file : files) {
                    final LocalResource localResource = readLocalResource(file);
                    if (localResource != null && filter.test(localResource)) {
                        FileReference parent = file.getParent();
                        LOGGER.warn("deleting invalid metadata index {}", parent);
                        ioManager.delete(parent);
                    }
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
        resourceCache.invalidateAll();
    }

    public Map<Long, LocalResource> loadAndGetAllResources() throws HyracksDataException {
        return getResources(p -> true);
    }

    @Override
    public synchronized long maxId() throws HyracksDataException {
        final Map<Long, LocalResource> allResources = loadAndGetAllResources();
        final Optional<Long> max = allResources.keySet().stream().max(Long::compare);
        return max.isPresent() ? max.get() : 0;
    }

    public void invalidateResource(String relativePath) {
        resourceCache.invalidate(relativePath);
    }

    public void clearResourcesCache() {
        resourceCache.invalidateAll();
    }

    private static String getFileName(String path) {
        return path.endsWith(File.separator) ? (path + StorageConstants.METADATA_FILE_NAME)
                : (path + File.separator + StorageConstants.METADATA_FILE_NAME);
    }

    private LocalResource readLocalResource(FileReference fileRef) throws HyracksDataException {
        byte[] bytes = ioManager.readAllBytes(fileRef);
        if (bytes == null) {
            return null;
        }

        try {
            final JsonNode jsonNode = OBJECT_MAPPER.readValue(bytes, JsonNode.class);
            LocalResource resource = (LocalResource) persistedResourceRegistry.deserialize(jsonNode);
            if (resource.getVersion() == ITreeIndexFrame.Constants.VERSION) {
                return resource;
            } else {
                throw new AsterixException("Storage version mismatch.");
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public synchronized void setReplicationManager(IReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        isReplicationEnabled = replicationManager.isReplicationEnabled();

        if (isReplicationEnabled) {
            filesToBeReplicated = new HashSet<>();
        }
    }

    private void createReplicationJob(ReplicationOperation operation, FileReference fileRef)
            throws HyracksDataException {
        filesToBeReplicated.clear();
        filesToBeReplicated.add(fileRef.getAbsolutePath());
        ReplicationJob job = new ReplicationJob(ReplicationJobType.METADATA, operation, ReplicationExecutionType.SYNC,
                filesToBeReplicated);
        try {
            replicationManager.submitJob(job);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Deletes physical files of all data verses.
     */
    public synchronized void deleteStorageData() throws HyracksDataException {
        for (FileReference root : storageRoots) {
            ioManager.deleteDirectory(root);
        }
        createStorageRoots();
    }

    public synchronized Set<Integer> getAllPartitions() throws HyracksDataException {
        return loadAndGetAllResources().values().stream().map(LocalResource::getResource)
                .map(DatasetLocalResource.class::cast).map(DatasetLocalResource::getPartition)
                .collect(Collectors.toSet());
    }

    public synchronized Optional<DatasetResourceReference> getLocalResourceReference(String absoluteFilePath)
            throws HyracksDataException {
        final String localResourcePath = StoragePathUtil.getIndexFileRelativePath(absoluteFilePath);
        final LocalResource lr = get(localResourcePath);
        return lr != null ? Optional.of(DatasetResourceReference.of(lr)) : Optional.empty();
    }

    /**
     * Gets a set of files for the indexes in partition {@code partition}. Each file points
     * to where the index's files are stored.
     *
     * @param partition
     * @return The set of indexes files
     * @throws HyracksDataException
     */
    public synchronized Set<FileReference> getPartitionIndexes(int partition) throws HyracksDataException {
        FileReference partitionRoot = getPartitionRoot(partition);
        final Map<Long, LocalResource> partitionResourcesMap = getResources(resource -> {
            DatasetLocalResource dsResource = (DatasetLocalResource) resource.getResource();
            return dsResource.getPartition() == partition;
        }, Collections.singletonList(partitionRoot));
        Set<FileReference> indexes = new HashSet<>();
        for (LocalResource localResource : partitionResourcesMap.values()) {
            indexes.add(ioManager.resolve(localResource.getPath()));
        }
        return indexes;
    }

    public synchronized Map<Long, LocalResource> getPartitionResources(int partition) throws HyracksDataException {
        return getResources(r -> true, Collections.singleton(partition));
    }

    public synchronized Map<String, Long> getPartitionReplicatedResources(int partition, IReplicationStrategy strategy)
            throws HyracksDataException {
        final Map<String, Long> partitionReplicatedResources = new HashMap<>();
        final Map<Long, LocalResource> partitionResources = getPartitionResources(partition);
        for (LocalResource lr : partitionResources.values()) {
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) lr.getResource();
            if (strategy.isMatch(datasetLocalResource.getDatasetId())) {
                DatasetResourceReference drr = DatasetResourceReference.of(lr);
                partitionReplicatedResources.put(drr.getFileRelativePath().toString(), lr.getId());
            }
        }
        return partitionReplicatedResources;
    }

    public synchronized List<String> getPartitionReplicatedFiles(int partition, IReplicationStrategy strategy)
            throws HyracksDataException {
        final List<String> partitionReplicatedFiles = new ArrayList<>();
        final Set<FileReference> replicatedIndexes = new HashSet<>();
        final Map<Long, LocalResource> partitionResources = getPartitionResources(partition);
        for (LocalResource lr : partitionResources.values()) {
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) lr.getResource();
            if (strategy.isMatch(datasetLocalResource.getDatasetId())) {
                replicatedIndexes.add(ioManager.resolve(lr.getPath()));
            }
        }
        for (FileReference indexDir : replicatedIndexes) {
            partitionReplicatedFiles.addAll(getIndexFiles(indexDir));
        }
        return partitionReplicatedFiles;
    }

    public synchronized long getReplicatedIndexesMaxComponentId(int partition, IReplicationStrategy strategy)
            throws HyracksDataException {
        long maxComponentId = LSMComponentId.MIN_VALID_COMPONENT_ID;
        final Map<Long, LocalResource> partitionResources = getPartitionResources(partition);
        for (LocalResource lr : partitionResources.values()) {
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) lr.getResource();
            if (strategy.isMatch(datasetLocalResource.getDatasetId())) {
                final IIndexCheckpointManager indexCheckpointManager =
                        indexCheckpointManagerProvider.get(DatasetResourceReference.of(lr));
                maxComponentId = Math.max(maxComponentId, indexCheckpointManager.getLatest().getLastComponentId());
            }
        }
        return maxComponentId;
    }

    private List<String> getIndexFiles(FileReference indexDir) throws HyracksDataException {
        final List<String> indexFiles = new ArrayList<>();
        Collection<FileReference> indexFilteredFiles = ioManager.list(indexDir, LSM_INDEX_FILES_FILTER);
        indexFilteredFiles.stream().map(FileReference::getAbsolutePath).forEach(indexFiles::add);
        return indexFiles;
    }

    private void createStorageRoots() {
        for (FileReference root : storageRoots) {
            ioManager.makeDirectories(root);
        }
    }

    public synchronized void cleanup(int partition) throws HyracksDataException {
        final Set<FileReference> partitionIndexes = getPartitionIndexes(partition);
        try {
            for (FileReference index : partitionIndexes) {
                deleteIndexMaskedFiles(index);
                if (isValidIndex(index)) {
                    deleteIndexInvalidComponents(index);
                }
            }
        } catch (IOException | ParseException e) {
            throw HyracksDataException.create(e);
        }
    }

    public List<ResourceStorageStats> getStorageStats() throws HyracksDataException {
        final List<DatasetResourceReference> allResources = loadAndGetAllResources().values().stream()
                .map(DatasetResourceReference::of).collect(Collectors.toList());
        final List<ResourceStorageStats> resourcesStats = new ArrayList<>();
        for (DatasetResourceReference res : allResources) {
            final ResourceStorageStats resourceStats = getResourceStats(res);
            if (resourceStats != null) {
                resourcesStats.add(resourceStats);
            }
        }
        return resourcesStats;
    }

    public synchronized void deleteCorruptedResources() throws HyracksDataException {
        for (FileReference root : storageRoots) {
            final Collection<FileReference> metadataMaskFiles = ioManager.list(root, METADATA_MASK_FILES_FILTER);
            for (FileReference metadataMaskFile : metadataMaskFiles) {
                final FileReference resourceFile = metadataMaskFile.getParent().getChild(METADATA_FILE_NAME);
                ioManager.delete(resourceFile);
                ioManager.delete(metadataMaskFile);
            }
        }
    }

    private void deleteIndexMaskedFiles(FileReference index) throws IOException {
        Collection<FileReference> masks = ioManager.list(index, MASK_FILES_FILTER);
        for (FileReference mask : masks) {
            deleteIndexMaskedFiles(index, mask);
            // delete the mask itself
            ioManager.delete(mask);
        }
    }

    private boolean isValidIndex(FileReference index) throws IOException {
        // any index without any checkpoint files is invalid
        // this can happen if a crash happens when the index metadata file is created
        // but before the initial checkpoint is persisted. The index metadata file will
        // be deleted and recreated when the index is created again
        return getIndexCheckpointManager(index).getCheckpointCount() != 0;
    }

    private void deleteIndexInvalidComponents(FileReference index) throws IOException, ParseException {
        final Collection<FileReference> indexComponentFiles = ioManager.list(index, COMPONENT_FILES_FILTER);
        if (indexComponentFiles == null) {
            throw new IOException(index + " doesn't exist or an IO error occurred");
        }
        final long validComponentSequence = getIndexCheckpointManager(index).getValidComponentSequence();
        for (FileReference componentFileRef : indexComponentFiles) {
            // delete any file with start or end sequence > valid component sequence
            final long fileStart = IndexComponentFileReference.of(componentFileRef.getName()).getSequenceStart();
            final long fileEnd = IndexComponentFileReference.of(componentFileRef.getName()).getSequenceEnd();
            if (fileStart > validComponentSequence || fileEnd > validComponentSequence) {
                LOGGER.warn(() -> "Deleting invalid component file " + componentFileRef.getAbsolutePath()
                        + " based on valid sequence " + validComponentSequence);
                ioManager.delete(componentFileRef);
            }
        }
    }

    private IIndexCheckpointManager getIndexCheckpointManager(FileReference index) throws HyracksDataException {
        final String indexFile = index.getChild(METADATA_FILE_NAME).getAbsolutePath();
        final ResourceReference indexRef = ResourceReference.of(indexFile);
        return indexCheckpointManagerProvider.get(indexRef);
    }

    private void deleteIndexMaskedFiles(FileReference index, FileReference mask) throws IOException {
        if (!mask.getFile().getName().startsWith(StorageConstants.MASK_FILE_PREFIX)) {
            throw new IllegalArgumentException("Unrecognized mask file: " + mask);
        }
        Collection<FileReference> maskedFiles;
        if (isComponentMask(mask)) {
            final String componentId = mask.getName().substring(StorageConstants.COMPONENT_MASK_FILE_PREFIX.length());
            maskedFiles = ioManager.list(index, (dir, name) -> name.startsWith(componentId));
        } else {
            final String maskedFileName = mask.getName().substring(StorageConstants.MASK_FILE_PREFIX.length());
            maskedFiles = ioManager.list(index, (dir, name) -> name.equals(maskedFileName));
        }
        if (maskedFiles != null) {
            for (FileReference maskedFile : maskedFiles) {
                LOGGER.info(() -> "deleting masked file: " + maskedFile.getAbsolutePath());
                ioManager.delete(maskedFile);
            }
        }
    }

    private ResourceStorageStats getResourceStats(DatasetResourceReference resource) {
        try {
            final FileReference resolvedPath = ioManager.resolve(resource.getRelativePath().toString());
            long totalSize = 0;
            final Collection<FileReference> indexFiles = ioManager.list(resolvedPath);
            final Map<String, Long> componentsStats = new HashMap<>();
            if (indexFiles != null) {
                for (FileReference file : indexFiles) {
                    long fileSize = ioManager.getSize(file);
                    totalSize += fileSize;
                    if (isComponentFile(resolvedPath, file.getName())) {
                        String componentSeq = getComponentSequence(file.getAbsolutePath());
                        componentsStats.put(componentSeq, componentsStats.getOrDefault(componentSeq, 0L) + fileSize);
                    }
                }
            }
            return new ResourceStorageStats(resource, componentsStats, totalSize);
        } catch (Exception e) {
            LOGGER.warn("Couldn't get stats for resource {}", resource.getRelativePath(), e);
        }
        return null;
    }

    public long getDatasetSize(DatasetCopyIdentifier datasetIdentifier, Set<Integer> nodePartitions)
            throws HyracksDataException {
        long totalSize = 0;
        final Map<Long, LocalResource> dataverse = getResources(lr -> {
            final ResourceReference resourceReference = ResourceReference.ofIndex(lr.getPath());
            return datasetIdentifier.isMatch(resourceReference);
        }, nodePartitions);
        final List<DatasetResourceReference> allResources =
                dataverse.values().stream().map(DatasetResourceReference::of).collect(Collectors.toList());
        for (DatasetResourceReference res : allResources) {
            final ResourceStorageStats resourceStats = getResourceStats(res);
            if (resourceStats != null) {
                totalSize += resourceStats.getTotalSize();
            }
        }
        return totalSize;
    }

    private void createResourceFileMask(FileReference resourceFile) throws HyracksDataException {
        FileReference maskFile = getResourceMaskFilePath(resourceFile);
        try {
            ioManager.create(maskFile);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void deleteResourceFileMask(FileReference resourceFile) throws HyracksDataException {
        FileReference maskFile = getResourceMaskFilePath(resourceFile);
        ioManager.delete(maskFile);
    }

    private FileReference getResourceMaskFilePath(FileReference resourceFile) {
        FileReference resourceFileParent = resourceFile.getParent();
        return resourceFileParent.getChild(METADATA_FILE_MASK_NAME);
    }

    private static boolean isComponentMask(FileReference mask) {
        return mask.getName().startsWith(StorageConstants.COMPONENT_MASK_FILE_PREFIX);
    }

    private static boolean isComponentFile(FileReference indexDir, String fileName) {
        return COMPONENT_FILES_FILTER.accept(indexDir.getFile(), fileName);
    }

    public synchronized void keepPartitions(Set<Integer> keepPartitions) throws HyracksDataException {
        List<FileReference> onDiskPartitions = getOnDiskPartitions();
        for (FileReference onDiskPartition : onDiskPartitions) {
            int partitionNum = StoragePathUtil.getPartitionNumFromRelativePath(onDiskPartition.getAbsolutePath());
            if (!keepPartitions.contains(partitionNum)) {
                LOGGER.warn("deleting partition {} since it is not on partitions to keep {}", partitionNum,
                        keepPartitions);
                ioManager.delete(onDiskPartition);
            }
        }
    }

    public synchronized List<FileReference> getOnDiskPartitions() {
        List<FileReference> onDiskPartitions = new ArrayList<>();
        for (FileReference root : storageRoots) {
            onDiskPartitions.addAll(IoUtil.getMatchingChildren(root, (dir, name) -> dir != null && dir.isDirectory()
                    && name.startsWith(StorageConstants.PARTITION_DIR_PREFIX)));
        }
        return onDiskPartitions;
    }

    public FileReference getPartitionRoot(int partition) throws HyracksDataException {
        String path = StorageConstants.STORAGE_ROOT_DIR_NAME + File.separator + StorageConstants.PARTITION_DIR_PREFIX
                + partition;
        return ioManager.resolve(path);
    }

    public void deletePartition(int partitionId) throws HyracksDataException {
        Collection<FileReference> onDiskPartitions = getOnDiskPartitions();
        for (FileReference onDiskPartition : onDiskPartitions) {
            int partitionNum = StoragePathUtil.getPartitionNumFromRelativePath(onDiskPartition.getAbsolutePath());
            if (partitionNum == partitionId) {
                LOGGER.warn("deleting partition {}", partitionNum);
                ioManager.delete(onDiskPartition);
                return;
            }
        }
    }
}
