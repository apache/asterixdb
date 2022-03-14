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
import static org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_CREATE_FILE;
import static org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager.COMPONENT_FILES_FILTER;
import static org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager.UNINITIALIZED_COMPONENT_SEQ;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Stream;

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
import org.apache.commons.io.FileUtils;
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
import org.apache.hyracks.util.file.FileUtil;
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
    private static final int MAX_CACHED_RESOURCES = 1000;
    private static final FilenameFilter METADATA_FILES_FILTER =
            (dir, name) -> name.equals(StorageConstants.METADATA_FILE_NAME);
    private static final FilenameFilter METADATA_MASK_FILES_FILTER =
            (dir, name) -> name.equals(METADATA_FILE_MASK_NAME);

    // Finals
    private final IIOManager ioManager;
    private final Cache<String, LocalResource> resourceCache;
    // Mutables
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;
    private IReplicationManager replicationManager;
    private final List<Path> storageRoots;
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
            storageRoots.add(
                    Paths.get(ioDevices.get(i).getMount().getAbsolutePath(), StorageConstants.STORAGE_ROOT_DIR_NAME));
        }
        createStorageRoots();
        resourceCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHED_RESOURCES).build();
    }

    @Override
    public String toString() {
        StringBuilder aString = new StringBuilder().append(PersistentLocalResourceRepository.class.getSimpleName())
                .append(Character.LINE_SEPARATOR).append(ioManager.getClass().getSimpleName()).append(':')
                .append(Character.LINE_SEPARATOR).append(ioManager.toString()).append(Character.LINE_SEPARATOR)
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
            if (resourceFile.getFile().exists()) {
                resource = readLocalResource(resourceFile.getFile());
                resourceCache.put(relativePath, resource);
            }
        }
        return resource;
    }

    @SuppressWarnings("squid:S1181")
    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        String relativePath = getFileName(resource.getPath());
        FileReference resourceFile = ioManager.resolve(relativePath);
        if (resourceFile.getFile().exists()) {
            throw new HyracksDataException("Duplicate resource: " + resourceFile.getAbsolutePath());
        }

        final File parent = resourceFile.getFile().getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
            throw HyracksDataException.create(CANNOT_CREATE_FILE, parent.getAbsolutePath());
        }
        // The next block should be all or nothing
        try {
            createResourceFileMask(resourceFile);
            byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(resource.toJson(persistedResourceRegistry));
            FileUtil.writeAndForce(Paths.get(resourceFile.getAbsolutePath()), bytes);
            indexCheckpointManagerProvider.get(DatasetResourceReference.of(resource)).init(UNINITIALIZED_COMPONENT_SEQ,
                    0, LSMComponentId.EMPTY_INDEX_LAST_COMPONENT_ID.getMaxId(), null);
            deleteResourceFileMask(resourceFile);
        } catch (Exception e) {
            cleanup(resourceFile);
            throw HyracksDataException.create(e);
        } catch (Throwable th) {
            LOGGER.error("Error creating resource {}", resourceFile, th);
            ExitUtil.halt(ExitUtil.EC_ERROR_CREATING_RESOURCES);
        }
        resourceCache.put(resource.getPath(), resource);
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
                IoUtil.delete(resourceFile);
            } catch (Throwable th) {
                LOGGER.error("Error cleaning up corrupted resource {}", resourceFile, th);
                ExitUtil.halt(ExitUtil.EC_FAILED_TO_DELETE_CORRUPTED_RESOURCES);
            }
        }
    }

    @Override
    public synchronized void delete(String relativePath) throws HyracksDataException {
        FileReference resourceFile = getLocalResourceFileByName(ioManager, relativePath);
        try {
            if (resourceFile.getFile().exists()) {
                try {
                    createReplicationJob(ReplicationOperation.DELETE, resourceFile);
                } catch (Exception e) {
                    LOGGER.error("failed to delete resource file {} from replicas", resourceFile);
                }
                final LocalResource localResource = readLocalResource(resourceFile.getFile());
                IoUtil.delete(resourceFile);
                // delete all checkpoints
                indexCheckpointManagerProvider.get(DatasetResourceReference.of(localResource)).delete();
            } else {
                throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.RESOURCE_DOES_NOT_EXIST,
                        relativePath);
            }
        } finally {
            invalidateResource(relativePath);
        }
    }

    public static FileReference getLocalResourceFileByName(IIOManager ioManager, String resourcePath)
            throws HyracksDataException {
        String fileName = resourcePath + File.separator + StorageConstants.METADATA_FILE_NAME;
        return ioManager.resolve(fileName);
    }

    public synchronized Map<Long, LocalResource> getResources(Predicate<LocalResource> filter, List<Path> roots)
            throws HyracksDataException {
        Map<Long, LocalResource> resourcesMap = new HashMap<>();
        for (Path root : roots) {
            if (!Files.exists(root) || !Files.isDirectory(root)) {
                continue;
            }
            final Collection<File> files = IoUtil.getMatchingFiles(root, METADATA_FILES_FILTER);
            try {
                for (File file : files) {
                    final LocalResource localResource = readLocalResource(file);
                    if (filter.test(localResource)) {
                        resourcesMap.put(localResource.getId(), localResource);
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
        List<Path> partitionsRoots = new ArrayList<>();
        for (Integer partition : partitions) {
            partitionsRoots.add(getPartitionRoot(partition));
        }
        return getResources(filter, partitionsRoots);
    }

    public synchronized void deleteInvalidIndexes(Predicate<LocalResource> filter) throws HyracksDataException {
        for (Path root : storageRoots) {
            final Collection<File> files = IoUtil.getMatchingFiles(root, METADATA_FILES_FILTER);
            try {
                for (File file : files) {
                    final LocalResource localResource = readLocalResource(file);
                    if (filter.test(localResource)) {
                        LOGGER.warn("deleting invalid metadata index {}", file.getParentFile());
                        IoUtil.delete(file.getParentFile());
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

    private LocalResource readLocalResource(File file) throws HyracksDataException {
        final Path path = Paths.get(file.getAbsolutePath());
        try {
            final JsonNode jsonNode = OBJECT_MAPPER.readValue(Files.readAllBytes(path), JsonNode.class);
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
     *
     * @throws IOException
     */
    public synchronized void deleteStorageData() throws IOException {
        for (Path root : storageRoots) {
            final File rootFile = root.toFile();
            if (rootFile.exists()) {
                FileUtils.deleteDirectory(rootFile);
            }
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
    public synchronized Set<File> getPartitionIndexes(int partition) throws HyracksDataException {
        Path partitionRoot = getPartitionRoot(partition);
        final Map<Long, LocalResource> partitionResourcesMap = getResources(resource -> {
            DatasetLocalResource dsResource = (DatasetLocalResource) resource.getResource();
            return dsResource.getPartition() == partition;
        }, Collections.singletonList(partitionRoot));
        Set<File> indexes = new HashSet<>();
        for (LocalResource localResource : partitionResourcesMap.values()) {
            indexes.add(ioManager.resolve(localResource.getPath()).getFile());
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
        final Set<File> replicatedIndexes = new HashSet<>();
        final Map<Long, LocalResource> partitionResources = getPartitionResources(partition);
        for (LocalResource lr : partitionResources.values()) {
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) lr.getResource();
            if (strategy.isMatch(datasetLocalResource.getDatasetId())) {
                replicatedIndexes.add(ioManager.resolve(lr.getPath()).getFile());
            }
        }
        for (File indexDir : replicatedIndexes) {
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

    private List<String> getIndexFiles(File indexDir) {
        final List<String> indexFiles = new ArrayList<>();
        if (indexDir.isDirectory()) {
            File[] indexFilteredFiles = indexDir.listFiles(LSM_INDEX_FILES_FILTER);
            if (indexFilteredFiles != null) {
                Stream.of(indexFilteredFiles).map(File::getAbsolutePath).forEach(indexFiles::add);
            }
        }
        return indexFiles;
    }

    private void createStorageRoots() {
        for (Path root : storageRoots) {
            try {
                Files.createDirectories(root);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create storage root directory at " + root, e);
            }
        }
    }

    public synchronized void cleanup(int partition) throws HyracksDataException {
        final Set<File> partitionIndexes = getPartitionIndexes(partition);
        try {
            for (File index : partitionIndexes) {
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
        for (Path root : storageRoots) {
            final Collection<File> metadataMaskFiles = IoUtil.getMatchingFiles(root, METADATA_MASK_FILES_FILTER);
            for (File metadataMaskFile : metadataMaskFiles) {
                final File resourceFile = new File(metadataMaskFile.getParent(), METADATA_FILE_NAME);
                if (resourceFile.exists()) {
                    IoUtil.delete(resourceFile);
                }
                IoUtil.delete(metadataMaskFile);
            }
        }
    }

    private void deleteIndexMaskedFiles(File index) throws IOException {
        File[] masks = index.listFiles(MASK_FILES_FILTER);
        if (masks != null) {
            for (File mask : masks) {
                deleteIndexMaskedFiles(index, mask);
                // delete the mask itself
                Files.delete(mask.toPath());
            }
        }
    }

    private boolean isValidIndex(File index) throws IOException {
        // any index without any checkpoint files is invalid
        // this can happen if a crash happens when the index metadata file is created
        // but before the initial checkpoint is persisted. The index metadata file will
        // be deleted and recreated when the index is created again
        return getIndexCheckpointManager(index).getCheckpointCount() != 0;
    }

    private void deleteIndexInvalidComponents(File index) throws IOException, ParseException {
        final File[] indexComponentFiles = index.listFiles(COMPONENT_FILES_FILTER);
        if (indexComponentFiles == null) {
            throw new IOException(index + " doesn't exist or an IO error occurred");
        }
        final long validComponentSequence = getIndexCheckpointManager(index).getValidComponentSequence();
        for (File componentFile : indexComponentFiles) {
            // delete any file with start or end sequence > valid component sequence
            final long fileStart = IndexComponentFileReference.of(componentFile.getName()).getSequenceStart();
            final long fileEnd = IndexComponentFileReference.of(componentFile.getName()).getSequenceEnd();
            if (fileStart > validComponentSequence || fileEnd > validComponentSequence) {
                LOGGER.warn(() -> "Deleting invalid component file " + componentFile.getAbsolutePath()
                        + " based on valid sequence " + validComponentSequence);
                Files.delete(componentFile.toPath());
            }
        }
    }

    private IIndexCheckpointManager getIndexCheckpointManager(File index) throws HyracksDataException {
        final String indexFile = Paths.get(index.getAbsolutePath(), StorageConstants.METADATA_FILE_NAME).toString();
        final ResourceReference indexRef = ResourceReference.of(indexFile);
        return indexCheckpointManagerProvider.get(indexRef);
    }

    private void deleteIndexMaskedFiles(File index, File mask) throws IOException {
        if (!mask.getName().startsWith(StorageConstants.MASK_FILE_PREFIX)) {
            throw new IllegalArgumentException("Unrecognized mask file: " + mask);
        }
        File[] maskedFiles;
        if (isComponentMask(mask)) {
            final String componentId = mask.getName().substring(StorageConstants.COMPONENT_MASK_FILE_PREFIX.length());
            maskedFiles = index.listFiles((dir, name) -> name.startsWith(componentId));
        } else {
            final String maskedFileName = mask.getName().substring(StorageConstants.MASK_FILE_PREFIX.length());
            maskedFiles = index.listFiles((dir, name) -> name.equals(maskedFileName));
        }
        if (maskedFiles != null) {
            for (File maskedFile : maskedFiles) {
                LOGGER.info(() -> "deleting masked file: " + maskedFile.getAbsolutePath());
                Files.delete(maskedFile.toPath());
            }
        }
    }

    private ResourceStorageStats getResourceStats(DatasetResourceReference resource) {
        try {
            final FileReference resolvedPath = ioManager.resolve(resource.getRelativePath().toString());
            long totalSize = 0;
            final File[] indexFiles = resolvedPath.getFile().listFiles();
            final Map<String, Long> componentsStats = new HashMap<>();
            if (indexFiles != null) {
                for (File file : indexFiles) {
                    long fileSize = file.length();
                    totalSize += fileSize;
                    if (isComponentFile(resolvedPath.getFile(), file.getName())) {
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
        Path maskFile = getResourceMaskFilePath(resourceFile);
        try {
            Files.createFile(maskFile);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void deleteResourceFileMask(FileReference resourceFile) throws HyracksDataException {
        Path maskFile = getResourceMaskFilePath(resourceFile);
        IoUtil.delete(maskFile);
    }

    private Path getResourceMaskFilePath(FileReference resourceFile) {
        return Paths.get(resourceFile.getFile().getParentFile().getAbsolutePath(), METADATA_FILE_MASK_NAME);
    }

    private static boolean isComponentMask(File mask) {
        return mask.getName().startsWith(StorageConstants.COMPONENT_MASK_FILE_PREFIX);
    }

    private static boolean isComponentFile(File indexDir, String fileName) {
        return COMPONENT_FILES_FILTER.accept(indexDir, fileName);
    }

    public List<Path> getStorageRoots() {
        return storageRoots;
    }

    public synchronized void keepPartitions(Set<Integer> keepPartitions) {
        List<File> onDiskPartitions = getOnDiskPartitions();
        for (File onDiskPartition : onDiskPartitions) {
            int partitionNum = StoragePathUtil.getPartitionNumFromRelativePath(onDiskPartition.getAbsolutePath());
            if (!keepPartitions.contains(partitionNum)) {
                LOGGER.warn("deleting partition {} since it is not on partitions to keep {}", partitionNum,
                        keepPartitions);
                FileUtils.deleteQuietly(onDiskPartition);
            }
        }
    }

    public synchronized List<File> getOnDiskPartitions() {
        List<File> onDiskPartitions = new ArrayList<>();
        for (Path root : storageRoots) {
            File[] partitions = root.toFile().listFiles(
                    (dir, name) -> dir.isDirectory() && name.startsWith(StorageConstants.PARTITION_DIR_PREFIX));
            if (partitions != null) {
                onDiskPartitions.addAll(Arrays.asList(partitions));
            }
        }
        return onDiskPartitions;
    }

    public Path getPartitionRoot(int partition) throws HyracksDataException {
        Path path =
                Paths.get(StorageConstants.STORAGE_ROOT_DIR_NAME, StorageConstants.PARTITION_DIR_PREFIX + partition);
        FileReference resolve = ioManager.resolve(path.toString());
        return resolve.getFile().toPath();
    }

    public void deletePartition(int partitionId) {
        List<File> onDiskPartitions = getOnDiskPartitions();
        for (File onDiskPartition : onDiskPartitions) {
            int partitionNum = StoragePathUtil.getPartitionNumFromRelativePath(onDiskPartition.getAbsolutePath());
            if (partitionNum == partitionId) {
                LOGGER.warn("deleting partition {}", partitionNum);
                FileUtils.deleteQuietly(onDiskPartition);
                return;
            }
        }
    }
}
