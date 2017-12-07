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
package org.apache.asterix.replication.storage;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;

public class ReplicaResourcesManager implements IReplicaResourcesManager {
    public static final String LSM_COMPONENT_MASK_SUFFIX = "_mask";
    private final PersistentLocalResourceRepository localRepository;
    private final Map<String, ClusterPartition[]> nodePartitions;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;

    public ReplicaResourcesManager(ILocalResourceRepository localRepository, MetadataProperties metadataProperties,
            IIndexCheckpointManagerProvider indexCheckpointManagerProvider) {
        this.localRepository = (PersistentLocalResourceRepository) localRepository;
        this.indexCheckpointManagerProvider = indexCheckpointManagerProvider;
        nodePartitions = metadataProperties.getNodePartitions();
    }

    public void deleteIndexFile(LSMIndexFileProperties afp) throws HyracksDataException {
        String indexPath = getIndexPath(afp);
        if (indexPath != null) {
            if (afp.isLSMComponentFile()) {
                //delete index file
                String indexFilePath = indexPath + File.separator + afp.getFileName();
                File destFile = new File(indexFilePath);
                FileUtils.deleteQuietly(destFile);
            } else {
                //delete index directory
                FileUtils.deleteQuietly(new File(indexPath));
            }
        }
    }

    public String getIndexPath(LSMIndexFileProperties fileProperties) throws HyracksDataException {
        final FileReference indexPath = localRepository.getIndexPath(Paths.get(fileProperties.getFilePath()));
        if (!indexPath.getFile().exists()) {
            indexPath.getFile().mkdirs();
        }
        return indexPath.toString();
    }

    public void createRemoteLSMComponentMask(LSMComponentProperties lsmComponentProperties) throws IOException {
        String maskPath = lsmComponentProperties.getMaskPath(this);
        Path path = Paths.get(maskPath);
        if (!Files.exists(path)) {
            File maskFile = new File(maskPath);
            maskFile.createNewFile();
        }
    }

    public void markLSMComponentReplicaAsValid(LSMComponentProperties lsmComponentProperties) throws IOException {
        //remove mask to mark component as valid
        String maskPath = lsmComponentProperties.getMaskPath(this);
        Path path = Paths.get(maskPath);
        Files.deleteIfExists(path);
    }

    public Set<File> getReplicaIndexes(String replicaId) throws HyracksDataException {
        Set<File> remoteIndexesPaths = new HashSet<File>();
        ClusterPartition[] partitions = nodePartitions.get(replicaId);
        for (ClusterPartition partition : partitions) {
            remoteIndexesPaths.addAll(localRepository.getPartitionIndexes(partition.getPartitionId()));
        }
        return remoteIndexesPaths;
    }

    @Override
    public long getPartitionsMinLSN(Set<Integer> partitions) throws HyracksDataException {
        long minRemoteLSN = Long.MAX_VALUE;
        for (Integer partition : partitions) {
            final List<DatasetResourceReference> partitionResources = localRepository.getResources(resource -> {
                DatasetLocalResource dsResource = (DatasetLocalResource) resource.getResource();
                return dsResource.getPartition() == partition;
            }).values().stream().map(DatasetResourceReference::of).collect(Collectors.toList());
            for (DatasetResourceReference indexRef : partitionResources) {
                long remoteIndexMaxLSN = indexCheckpointManagerProvider.get(indexRef).getLowWatermark();
                minRemoteLSN = Math.min(minRemoteLSN, remoteIndexMaxLSN);
            }
        }
        return minRemoteLSN;
    }

    public Map<Long, DatasetResourceReference> getLaggingReplicaIndexesId2PathMap(String replicaId, long targetLSN)
            throws HyracksDataException {
        Map<Long, DatasetResourceReference> laggingReplicaIndexes = new HashMap<>();
        final List<Integer> replicaPartitions =
                Arrays.stream(nodePartitions.get(replicaId)).map(ClusterPartition::getPartitionId)
                        .collect(Collectors.toList());
        for (int patition : replicaPartitions) {
            final Map<Long, LocalResource> partitionResources = localRepository.getPartitionResources(patition);
            final List<DatasetResourceReference> indexesRefs =
                    partitionResources.values().stream().map(DatasetResourceReference::of).collect(Collectors.toList());
            for (DatasetResourceReference ref : indexesRefs) {
                if (indexCheckpointManagerProvider.get(ref).getLowWatermark() < targetLSN) {
                    laggingReplicaIndexes.put(ref.getResourceId(), ref);
                }
            }
        }
        return laggingReplicaIndexes;
    }

    public void cleanInvalidLSMComponents(String replicaId) {
        //for every index in replica
        Set<File> remoteIndexes = null;
        try {
            remoteIndexes = getReplicaIndexes(replicaId);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        for (File remoteIndexFile : remoteIndexes) {
            //search for any mask
            File[] masks = remoteIndexFile.listFiles(LSM_COMPONENTS_MASKS_FILTER);

            for (File mask : masks) {
                //delete all files belonging to this mask
                deleteLSMComponentFilesForMask(mask);
                //delete the mask itself
                mask.delete();
            }
        }
    }

    private static void deleteLSMComponentFilesForMask(File maskFile) {
        String lsmComponentTimeStamp = maskFile.getName().substring(0,
                maskFile.getName().length() - LSM_COMPONENT_MASK_SUFFIX.length());
        File indexFolder = maskFile.getParentFile();
        File[] lsmComponentsFiles = indexFolder.listFiles(LSM_COMPONENTS_NON_MASKS_FILTER);
        for (File lsmComponentFile : lsmComponentsFiles) {
            if (lsmComponentFile.getName().contains(lsmComponentTimeStamp)) {
                //match based on time stamp
                lsmComponentFile.delete();
            }
        }
    }

    /**
     * @param partition
     * @return Absolute paths to all partition files
     */
    public List<String> getPartitionIndexesFiles(int partition, boolean relativePath) throws HyracksDataException {
        List<String> partitionFiles = new ArrayList<String>();
        Set<File> partitionIndexes = localRepository.getPartitionIndexes(partition);
        for (File indexDir : partitionIndexes) {
            if (indexDir.isDirectory()) {
                File[] indexFiles = indexDir.listFiles(LSM_INDEX_FILES_FILTER);
                if (indexFiles != null) {
                    for (File file : indexFiles) {
                        if (!relativePath) {
                            partitionFiles.add(file.getAbsolutePath());
                        } else {
                            partitionFiles.add(StoragePathUtil.getIndexFileRelativePath(file.getAbsolutePath()));
                        }
                    }
                }
            }
        }
        return partitionFiles;
    }

    private static final FilenameFilter LSM_COMPONENTS_MASKS_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(LSM_COMPONENT_MASK_SUFFIX);
        }
    };

    private static final FilenameFilter LSM_COMPONENTS_NON_MASKS_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return !name.endsWith(LSM_COMPONENT_MASK_SUFFIX);
        }
    };

    private static final FilenameFilter LSM_INDEX_FILES_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.equalsIgnoreCase(StorageConstants.METADATA_FILE_NAME) || !name.startsWith(".");
        }
    };
}
