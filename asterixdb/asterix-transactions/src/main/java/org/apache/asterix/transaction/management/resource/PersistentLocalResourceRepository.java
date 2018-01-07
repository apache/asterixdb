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

import static org.apache.asterix.common.utils.StorageConstants.INDEX_CHECKPOINT_FILE_PREFIX;
import static org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_CREATE_FILE;

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
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.asterix.common.replication.ReplicationJob;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationJobType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    public static final Predicate<Path> INDEX_COMPONENTS = path -> !path.endsWith(StorageConstants.METADATA_FILE_NAME);
    private static final Logger LOGGER = LogManager.getLogger();
    private static final FilenameFilter LSM_INDEX_FILES_FILTER =
            (dir, name) -> !name.startsWith(INDEX_CHECKPOINT_FILE_PREFIX);
    private static final FilenameFilter MASK_FILES_FILTER =
            (dir, name) -> name.startsWith(StorageConstants.MASK_FILE_PREFIX);
    private static final int MAX_CACHED_RESOURCES = 1000;
    private static final IOFileFilter METADATA_FILES_FILTER = new IOFileFilter() {
        @Override
        public boolean accept(File file) {
            return file.getName().equals(StorageConstants.METADATA_FILE_NAME);
        }

        @Override
        public boolean accept(File dir, String name) {
            return false;
        }
    };

    private static final IOFileFilter ALL_DIR_FILTER = new IOFileFilter() {
        @Override
        public boolean accept(File file) {
            return true;
        }

        @Override
        public boolean accept(File dir, String name) {
            return true;
        }
    };

    // Finals
    private final IIOManager ioManager;
    private final Cache<String, LocalResource> resourceCache;
    // Mutables
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;
    private IReplicationManager replicationManager;
    private final Path[] storageRoots;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;

    public PersistentLocalResourceRepository(IIOManager ioManager,
            IIndexCheckpointManagerProvider indexCheckpointManagerProvider) {
        this.ioManager = ioManager;
        this.indexCheckpointManagerProvider = indexCheckpointManagerProvider;
        storageRoots = new Path[ioManager.getIODevices().size()];
        final List<IODeviceHandle> ioDevices = ioManager.getIODevices();
        for (int i = 0; i < ioDevices.size(); i++) {
            storageRoots[i] = Paths.get(ioDevices.get(i).getMount().getAbsolutePath(),
                    StorageConstants.STORAGE_ROOT_DIR_NAME);
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

        try (FileOutputStream fos = new FileOutputStream(resourceFile.getFile());
                ObjectOutputStream oosToFos = new ObjectOutputStream(fos)) {
            oosToFos.writeObject(resource);
            oosToFos.flush();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

        resourceCache.put(resource.getPath(), resource);
        indexCheckpointManagerProvider.get(DatasetResourceReference.of(resource)).init(0);
        //if replication enabled, send resource metadata info to remote nodes
        if (isReplicationEnabled) {
            createReplicationJob(ReplicationOperation.REPLICATE, resourceFile);
        }
    }

    @Override
    public synchronized void delete(String relativePath) throws HyracksDataException {
        FileReference resourceFile = getLocalResourceFileByName(ioManager, relativePath);
        if (resourceFile.getFile().exists()) {
            if (isReplicationEnabled) {
                createReplicationJob(ReplicationOperation.DELETE, resourceFile);
            }
            // delete all checkpoints
            final LocalResource localResource = readLocalResource(resourceFile.getFile());
            indexCheckpointManagerProvider.get(DatasetResourceReference.of(localResource)).delete();
            // Invalidate before deleting the file just in case file deletion throws some exception.
            // Since it's just a cache invalidation, it should not affect correctness.
            resourceCache.invalidate(relativePath);
            IoUtil.delete(resourceFile);
        } else {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.RESOURCE_DOES_NOT_EXIST,
                    relativePath);
        }
    }

    private static FileReference getLocalResourceFileByName(IIOManager ioManager, String resourcePath)
            throws HyracksDataException {
        String fileName = resourcePath + File.separator + StorageConstants.METADATA_FILE_NAME;
        return ioManager.resolve(fileName);
    }

    public synchronized Map<Long, LocalResource> getResources(Predicate<LocalResource> filter)
            throws HyracksDataException {
        Map<Long, LocalResource> resourcesMap = new HashMap<>();
        for (Path root : storageRoots) {
            final Collection<File> files = FileUtils.listFiles(root.toFile(), METADATA_FILES_FILTER, ALL_DIR_FILTER);
            try {
                for (File file : files) {
                    final LocalResource localResource = PersistentLocalResourceRepository.readLocalResource(file);
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

    public Map<Long, LocalResource> loadAndGetAllResources() throws HyracksDataException {
        return getResources(p -> true);
    }

    @Override
    public long maxId() throws HyracksDataException {
        final Map<Long, LocalResource> allResources = loadAndGetAllResources();
        final Optional<Long> max = allResources.keySet().stream().max(Long::compare);
        return max.isPresent() ? max.get() : 0;
    }

    private static String getFileName(String path) {
        return path.endsWith(File.separator) ? (path + StorageConstants.METADATA_FILE_NAME)
                : (path + File.separator + StorageConstants.METADATA_FILE_NAME);
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
            throw HyracksDataException.create(e);
        }
    }

    public void setReplicationManager(IReplicationManager replicationManager) {
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
    public void deleteStorageData() throws IOException {
        for (Path root : storageRoots) {
            final File rootFile = root.toFile();
            if (rootFile.exists()) {
                FileUtils.deleteDirectory(rootFile);
            }
        }
        createStorageRoots();
    }

    public Set<Integer> getAllPartitions() throws HyracksDataException {
        return loadAndGetAllResources().values().stream().map(LocalResource::getResource)
                .map(DatasetLocalResource.class::cast).map(DatasetLocalResource::getPartition)
                .collect(Collectors.toSet());
    }

    public DatasetResourceReference getLocalResourceReference(String absoluteFilePath) throws HyracksDataException {
        final String localResourcePath = StoragePathUtil.getIndexFileRelativePath(absoluteFilePath);
        final LocalResource lr = get(localResourcePath);
        return DatasetResourceReference.of(lr);
    }

    /**
     * Gets a set of files for the indexes in partition {@code partition}. Each file points
     * to where the index's files are stored.
     *
     * @param partition
     * @return The set of indexes files
     * @throws HyracksDataException
     */
    public Set<File> getPartitionIndexes(int partition) throws HyracksDataException {
        final Map<Long, LocalResource> partitionResourcesMap = getResources(resource -> {
            DatasetLocalResource dsResource = (DatasetLocalResource) resource.getResource();
            return dsResource.getPartition() == partition;
        });
        Set<File> indexes = new HashSet<>();
        for (LocalResource localResource : partitionResourcesMap.values()) {
            indexes.add(ioManager.resolve(localResource.getPath()).getFile());
        }
        return indexes;
    }

    public Map<Long, LocalResource> getPartitionResources(int partition) throws HyracksDataException {
        return getResources(resource -> {
            DatasetLocalResource dsResource = (DatasetLocalResource) resource.getResource();
            return dsResource.getPartition() == partition;
        });
    }

    public List<String> getPartitionIndexesFiles(int partition) throws HyracksDataException {
        List<String> partitionFiles = new ArrayList<>();
        Set<File> partitionIndexes = getPartitionIndexes(partition);
        for (File indexDir : partitionIndexes) {
            if (indexDir.isDirectory()) {
                File[] indexFiles = indexDir.listFiles(LSM_INDEX_FILES_FILTER);
                if (indexFiles != null) {
                    Stream.of(indexFiles).map(File::getAbsolutePath).forEach(partitionFiles::add);
                }
            }
        }
        return partitionFiles;
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

    public void cleanup(int partition) throws HyracksDataException {
        final Set<File> partitionIndexes = getPartitionIndexes(partition);
        // find masks
        for (File index : partitionIndexes) {
            File[] masks = index.listFiles(MASK_FILES_FILTER);
            if (masks != null) {
                try {
                    for (File mask : masks) {
                        deleteIndexMaskedFiles(index, mask);
                        // delete the mask itself
                        Files.delete(mask.toPath());
                    }
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        }
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

    /**
     * Gets a component id based on its unique timestamp.
     * e.g. a component file 2018-01-08-01-08-50-439_2018-01-08-01-08-50-439_b
     * will return a component id 2018-01-08-01-08-50-439_2018-01-08-01-08-50-439
     *
     * @param componentFile any component file
     * @return The component id
     */
    public static String getComponentId(String componentFile) {
        final ResourceReference ref = ResourceReference.of(componentFile);
        return ref.getName().substring(0, ref.getName().lastIndexOf(AbstractLSMIndexFileManager.DELIMITER));
    }

    private static boolean isComponentMask(File mask) {
        return mask.getName().startsWith(StorageConstants.COMPONENT_MASK_FILE_PREFIX);
    }
}
