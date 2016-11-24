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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

public class ReplicaResourcesManager implements IReplicaResourcesManager {
    private static final Logger LOGGER = Logger.getLogger(ReplicaResourcesManager.class.getName());
    public final static String LSM_COMPONENT_MASK_SUFFIX = "_mask";
    private final static String REPLICA_INDEX_LSN_MAP_NAME = ".LSN_MAP";
    public static final long REPLICA_INDEX_CREATION_LSN = -1;
    private final PersistentLocalResourceRepository localRepository;
    private final Map<String, ClusterPartition[]> nodePartitions;

    public ReplicaResourcesManager(ILocalResourceRepository localRepository,
            AsterixMetadataProperties metadataProperties) {
        this.localRepository = (PersistentLocalResourceRepository) localRepository;
        nodePartitions = metadataProperties.getNodePartitions();
    }

    public void deleteIndexFile(LSMIndexFileProperties afp) {
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

    public String getIndexPath(LSMIndexFileProperties fileProperties) {
        fileProperties.splitFileName();
        //get partition path in this node
        String partitionPath = localRepository.getPartitionPath(fileProperties.getPartition());
        //get index path
        String indexPath = SplitsAndConstraintsUtil.getIndexPath(partitionPath, fileProperties.getPartition(),
                fileProperties.getDataverse(), fileProperties.getIdxName());

        Path path = Paths.get(indexPath);
        if (!Files.exists(path)) {
            File indexFolder = new File(indexPath);
            indexFolder.mkdirs();
        }
        return indexPath;
    }

    public void initializeReplicaIndexLSNMap(String indexPath, long currentLSN) throws IOException {
        HashMap<Long, Long> lsnMap = new HashMap<Long, Long>();
        lsnMap.put(REPLICA_INDEX_CREATION_LSN, currentLSN);
        updateReplicaIndexLSNMap(indexPath, lsnMap);
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

        //add component LSN to the index LSNs map
        Map<Long, Long> lsnMap = getReplicaIndexLSNMap(lsmComponentProperties.getReplicaComponentPath(this));
        lsnMap.put(lsmComponentProperties.getOriginalLSN(), lsmComponentProperties.getReplicaLSN());

        //update map on disk
        updateReplicaIndexLSNMap(lsmComponentProperties.getReplicaComponentPath(this), lsnMap);
    }

    public Set<File> getReplicaIndexes(String replicaId) {
        Set<File> remoteIndexesPaths = new HashSet<File>();
        ClusterPartition[] partitions = nodePartitions.get(replicaId);
        for (ClusterPartition partition : partitions) {
            remoteIndexesPaths.addAll(getPartitionIndexes(partition.getPartitionId()));
        }
        return remoteIndexesPaths;
    }

    @Override
    public long getPartitionsMinLSN(Set<Integer> partitions) {
        long minRemoteLSN = Long.MAX_VALUE;
        for (Integer partition : partitions) {
            //for every index in replica
            Set<File> remoteIndexes = getPartitionIndexes(partition);
            for (File indexFolder : remoteIndexes) {
                //read LSN map
                try {
                    //get max LSN per index
                    long remoteIndexMaxLSN = getReplicaIndexMaxLSN(indexFolder);

                    //get min of all maximums
                    minRemoteLSN = Math.min(minRemoteLSN, remoteIndexMaxLSN);
                } catch (IOException e) {
                    LOGGER.log(Level.INFO,
                            indexFolder.getAbsolutePath() + " Couldn't read LSN map for index " + indexFolder);
                    continue;
                }
            }
        }
        return minRemoteLSN;
    }

    public Map<Long, String> getLaggingReplicaIndexesId2PathMap(String replicaId, long targetLSN) throws IOException {
        Map<Long, String> laggingReplicaIndexes = new HashMap<Long, String>();
        try {
            //for every index in replica
            Set<File> remoteIndexes = getReplicaIndexes(replicaId);
            for (File indexFolder : remoteIndexes) {
                if (getReplicaIndexMaxLSN(indexFolder) < targetLSN) {
                    File localResource = new File(
                            indexFolder + File.separator + PersistentLocalResourceRepository.METADATA_FILE_NAME);
                    LocalResource resource = PersistentLocalResourceRepository.readLocalResource(localResource);
                    laggingReplicaIndexes.put(resource.getId(), indexFolder.getAbsolutePath());
                }
            }
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }

        return laggingReplicaIndexes;
    }

    private long getReplicaIndexMaxLSN(File indexFolder) throws IOException {
        long remoteIndexMaxLSN = 0;
        //get max LSN per index
        Map<Long, Long> lsnMap = getReplicaIndexLSNMap(indexFolder.getAbsolutePath());
        if (lsnMap != null) {
            for (Long lsn : lsnMap.values()) {
                remoteIndexMaxLSN = Math.max(remoteIndexMaxLSN, lsn);
            }
        }
        return remoteIndexMaxLSN;
    }

    public void cleanInvalidLSMComponents(String replicaId) {
        //for every index in replica
        Set<File> remoteIndexes = getReplicaIndexes(replicaId);
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

    @SuppressWarnings({ "unchecked" })
    public synchronized Map<Long, Long> getReplicaIndexLSNMap(String indexPath) throws IOException {
        try (FileInputStream fis = new FileInputStream(indexPath + File.separator + REPLICA_INDEX_LSN_MAP_NAME);
                ObjectInputStream oisFromFis = new ObjectInputStream(fis)) {
            Map<Long, Long> lsnMap = null;
            try {
                lsnMap = (Map<Long, Long>) oisFromFis.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return lsnMap;
        }
    }

    public synchronized void updateReplicaIndexLSNMap(String indexPath, Map<Long, Long> lsnMap) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(indexPath + File.separator + REPLICA_INDEX_LSN_MAP_NAME);
                ObjectOutputStream oosToFos = new ObjectOutputStream(fos)) {
            oosToFos.writeObject(lsnMap);
            oosToFos.flush();
        }
    }

    /**
     * @param partition
     * @return Set of file references to each index in the partition
     */
    public Set<File> getPartitionIndexes(int partition) {
        Set<File> partitionIndexes = new HashSet<File>();
        String storageDirName = ClusterProperties.INSTANCE.getStorageDirectoryName();
        String partitionStoragePath = localRepository.getPartitionPath(partition)
                + StoragePathUtil.prepareStoragePartitionPath(storageDirName, partition);
        File partitionRoot = new File(partitionStoragePath);
        if (partitionRoot.exists() && partitionRoot.isDirectory()) {
            File[] dataverseFileList = partitionRoot.listFiles();
            if (dataverseFileList != null) {
                for (File dataverseFile : dataverseFileList) {
                    if (dataverseFile.isDirectory()) {
                        File[] indexFileList = dataverseFile.listFiles();
                        if (indexFileList != null) {
                            for (File indexFile : indexFileList) {
                                if (indexFile.isDirectory()) {
                                    partitionIndexes.add(indexFile);
                                }
                            }
                        }
                    }
                }
            }
        }
        return partitionIndexes;
    }

    /**
     * @param partition
     * @return Absolute paths to all partition files
     */
    public List<String> getPartitionIndexesFiles(int partition, boolean relativePath) {
        List<String> partitionFiles = new ArrayList<String>();
        Set<File> partitionIndexes = getPartitionIndexes(partition);
        for (File indexDir : partitionIndexes) {
            if (indexDir.isDirectory()) {
                File[] indexFiles = indexDir.listFiles(LSM_INDEX_FILES_FILTER);
                if (indexFiles != null) {
                    for (File file : indexFiles) {
                        if (!relativePath) {
                            partitionFiles.add(file.getAbsolutePath());
                        } else {
                            partitionFiles.add(
                                    PersistentLocalResourceRepository.getResourceRelativePath(file.getAbsolutePath()));
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
            return name.equalsIgnoreCase(PersistentLocalResourceRepository.METADATA_FILE_NAME) || !name.startsWith(".");
        }
    };
}
