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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.common.file.LocalResource;

public class ReplicaResourcesManager implements IReplicaResourcesManager {
    private static final Logger LOGGER = Logger.getLogger(ReplicaResourcesManager.class.getName());
    private final String[] mountPoints;
    private final int numIODevices;
    private static final String REPLICA_FOLDER_SUFFIX = "_replica";
    private final String replicationStorageFolder;
    public final String localStorageFolder;
    private final String localNodeID;
    public final static String LSM_COMPONENT_MASK_SUFFIX = "_mask";
    private final static String REPLICA_INDEX_LSN_MAP_NAME = ".LSN_MAP";
    public static final long REPLICA_INDEX_CREATION_LSN = -1;
    private final AtomicLong lastMinRemoteLSN;

    public ReplicaResourcesManager(List<IODeviceHandle> devices, String localStorageFolder, String localNodeID,
            String replicationStorageFolder) throws HyracksDataException {
        numIODevices = devices.size();
        this.mountPoints = new String[numIODevices];
        for (int i = 0; i < numIODevices; i++) {
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
        this.localStorageFolder = localStorageFolder;
        this.localNodeID = localNodeID;
        this.replicationStorageFolder = replicationStorageFolder;
        lastMinRemoteLSN = new AtomicLong(-1);
    }

    @Override
    public String getLocalStorageFolder() {
        return localStorageFolder;
    }

    private String getReplicaStorageFolder(String replicaId, int IODeviceNum) {
        if (replicaId.equals(localNodeID)) {
            return mountPoints[IODeviceNum] + localStorageFolder;
        } else {
            return mountPoints[IODeviceNum] + replicationStorageFolder + File.separator + replicaId
                    + REPLICA_FOLDER_SUFFIX;
        }
    }

    public void deleteRemoteFile(AsterixLSMIndexFileProperties afp) throws IOException {
        String indexPath = getIndexPath(afp.getNodeId(), afp.getIoDeviceNum(), afp.getDataverse(), afp.getIdxName());
        if (indexPath != null) {
            if (afp.isLSMComponentFile()) {
                String backupFilePath = indexPath + File.separator + afp.getFileName();

                //delete file
                File destFile = new File(backupFilePath);
                if (destFile.exists()) {
                    destFile.delete();
                }
            } else {
                //delete index files
                indexPath = indexPath.substring(0, indexPath.lastIndexOf(File.separator));
                AsterixFilesUtil.deleteFolder(indexPath);
            }
        }
    }

    public String getIndexPath(String replicaId, int IODeviceNum, String dataverse, String dataset) {
        //mounting point/backupNodeId_replica/Dataverse/Dataset/device_id_#/
        String remoteIndexFolderPath = getReplicaStorageFolder(replicaId, IODeviceNum) + File.separator + dataverse
                + File.separator + dataset + File.separator + IndexFileNameUtil.IO_DEVICE_NAME_PREFIX + IODeviceNum;
        Path path = Paths.get(remoteIndexFolderPath);
        if (!Files.exists(path)) {
            File indexFolder = new File(remoteIndexFolderPath);
            indexFolder.mkdirs();
        }
        return remoteIndexFolderPath;
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

        if (Files.exists(path)) {
            File maskFile = new File(maskPath);
            maskFile.delete();
        }

        //add component LSN to the index LSNs map
        HashMap<Long, Long> lsnMap = getReplicaIndexLSNMap(lsmComponentProperties.getReplicaComponentPath(this));
        lsnMap.put(lsmComponentProperties.getOriginalLSN(), lsmComponentProperties.getReplicaLSN());

        //update map on disk
        updateReplicaIndexLSNMap(lsmComponentProperties.getReplicaComponentPath(this), lsnMap);

    }

    public List<String> getResourcesForReplica(String nodeId) throws HyracksDataException {
        List<String> resourcesList = new ArrayList<String>();
        String rootFolder;
        for (int i = 0; i < numIODevices; i++) {
            rootFolder = getReplicaStorageFolder(nodeId, i);
            File rootDirFile = new File(rootFolder);
            if (!rootDirFile.exists()) {
                continue;
            }

            File[] dataverseFileList = rootDirFile.listFiles();
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
                                            File[] metadataFiles = ioDeviceFile.listFiles(LSM_INDEX_FILES_FILTER);
                                            if (metadataFiles != null) {
                                                for (File metadataFile : metadataFiles) {
                                                    resourcesList.add(metadataFile.getAbsolutePath());
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
        return resourcesList;
    }

    public Set<File> getReplicaIndexes(String replicaId) throws HyracksDataException {
        Set<File> remoteIndexesPaths = new HashSet<File>();
        for (int i = 0; i < numIODevices; i++) {
            String rootReplicaFolder = getReplicaStorageFolder(replicaId, i);
            File rootDirFile = new File(rootReplicaFolder);
            if (!rootDirFile.exists()) {
                continue;
            }
            File[] dataverseFileList = rootDirFile.listFiles();
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
                                            remoteIndexesPaths.add(ioDeviceFile);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return remoteIndexesPaths;
    }

    @Override
    public long getMinRemoteLSN(Set<String> replicaIds) {
        if (lastMinRemoteLSN.get() != -1) {
            return lastMinRemoteLSN.get();
        }
        long minRemoteLSN = Long.MAX_VALUE;
        for (String replica : replicaIds) {
            try {
                //for every index in replica
                Set<File> remoteIndexes = getReplicaIndexes(replica);
                for (File indexFolder : remoteIndexes) {
                    //read LSN map
                    try {
                        //get max LSN per index
                        long remoteIndexMaxLSN = getReplicaIndexMaxLSN(indexFolder);

                        //get min of all maximums
                        minRemoteLSN = Math.min(minRemoteLSN, remoteIndexMaxLSN);
                    } catch (IOException e) {
                        LOGGER.log(Level.INFO, indexFolder.getAbsolutePath() + " Couldn't read LSN map for index "
                                + indexFolder);
                        continue;
                    }
                }
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }
        lastMinRemoteLSN.set(minRemoteLSN);
        return minRemoteLSN;
    }

    public HashMap<Long, String> getLaggingReplicaIndexesId2PathMap(String replicaId, long targetLSN)
            throws IOException {
        HashMap<Long, String> laggingReplicaIndexes = new HashMap<Long, String>();
        try {
            //for every index in replica
            Set<File> remoteIndexes = getReplicaIndexes(replicaId);
            for (File indexFolder : remoteIndexes) {
                if (getReplicaIndexMaxLSN(indexFolder) < targetLSN) {
                    File localResource = new File(indexFolder + File.separator
                            + PersistentLocalResourceRepository.METADATA_FILE_NAME);
                    LocalResource resource = PersistentLocalResourceRepository.readLocalResource(localResource);
                    laggingReplicaIndexes.put(resource.getResourceId(), indexFolder.getAbsolutePath());
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
        HashMap<Long, Long> lsnMap = getReplicaIndexLSNMap(indexFolder.getAbsolutePath());
        if (lsnMap != null) {
            for (Long lsn : lsnMap.values()) {
                remoteIndexMaxLSN = Math.max(remoteIndexMaxLSN, lsn);
            }
        }

        return remoteIndexMaxLSN;
    }

    public void cleanInvalidLSMComponents(String replicaId) throws HyracksDataException {
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

    private void deleteLSMComponentFilesForMask(File maskFile) {
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

    @SuppressWarnings("unchecked")
    public synchronized HashMap<Long, Long> getReplicaIndexLSNMap(String indexPath) throws IOException {
        FileInputStream fis = null;
        ObjectInputStream oisFromFis = null;
        try {
            fis = new FileInputStream(indexPath + File.separator + REPLICA_INDEX_LSN_MAP_NAME);
            oisFromFis = new ObjectInputStream(fis);
            Map<Long, Long> lsnMap = null;
            try {
                lsnMap = (Map<Long, Long>) oisFromFis.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return (HashMap<Long, Long>) lsnMap;
        } finally {
            if (oisFromFis != null) {
                oisFromFis.close();
            }
            if (oisFromFis == null && fis != null) {
                fis.close();
            }
        }
    }

    public synchronized void updateReplicaIndexLSNMap(String indexPath, HashMap<Long, Long> lsnMap) throws IOException {
        FileOutputStream fos = null;
        ObjectOutputStream oosToFos = null;
        try {
            fos = new FileOutputStream(indexPath + File.separator + REPLICA_INDEX_LSN_MAP_NAME);
            oosToFos = new ObjectOutputStream(fos);
            oosToFos.writeObject(lsnMap);
            oosToFos.flush();
            lastMinRemoteLSN.set(-1);
        } finally {
            if (oosToFos != null) {
                oosToFos.close();
            }
            if (oosToFos == null && fos != null) {
                fos.close();
            }
        }
    }

    private static final FilenameFilter LSM_COMPONENTS_MASKS_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            if (name.endsWith(LSM_COMPONENT_MASK_SUFFIX)) {
                return true;
            } else {
                return false;
            }
        }
    };

    private static final FilenameFilter LSM_COMPONENTS_NON_MASKS_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            if (!name.endsWith(LSM_COMPONENT_MASK_SUFFIX)) {
                return true;
            } else {
                return false;
            }
        }
    };

    private static final FilenameFilter LSM_INDEX_FILES_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            if (name.equalsIgnoreCase(PersistentLocalResourceRepository.METADATA_FILE_NAME)) {
                return true;
            } else if (!name.startsWith(".")) {
                return true;
            } else {
                return false;
            }
        }
    };
}