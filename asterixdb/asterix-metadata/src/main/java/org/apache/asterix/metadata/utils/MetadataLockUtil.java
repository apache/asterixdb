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
package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.metadata.LockList;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class MetadataLockUtil implements IMetadataLockUtil {

    @Override
    public void createDataverseBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName)
            throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
    }

    @Override
    public void dropDataverseBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName)
            throws AlgebricksException {
        lockMgr.acquireDataverseWriteLock(locks, dataverseName);
    }

    @Override
    public void createDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName, DataverseName itemTypeDataverseName, String itemTypeName, boolean itemTypeAnonymous,
            DataverseName metaItemTypeDataverseName, String metaItemTypeName, boolean metaItemTypeAnonymous,
            String nodeGroupName, String compactionPolicyName, boolean isDefaultCompactionPolicy,
            DatasetConfig.DatasetType datasetType, Object datasetDetails) throws AlgebricksException {
        createDatasetBeginPre(lockMgr, locks, dataverseName, itemTypeDataverseName, itemTypeName, itemTypeAnonymous,
                metaItemTypeDataverseName, metaItemTypeName, metaItemTypeAnonymous, nodeGroupName, compactionPolicyName,
                isDefaultCompactionPolicy);
        lockMgr.acquireDatasetWriteLock(locks, dataverseName, datasetName);
    }

    protected final void createDatasetBeginPre(IMetadataLockManager lockMgr, LockList locks,
            DataverseName dataverseName, DataverseName itemTypeDataverseName, String itemTypeName,
            boolean itemTypeAnonymous, DataverseName metaItemTypeDataverseName, String metaItemTypeName,
            boolean metaItemTypeAnonymous, String nodeGroupName, String compactionPolicyName,
            boolean isDefaultCompactionPolicy) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        if (itemTypeDataverseName != null && !dataverseName.equals(itemTypeDataverseName)) {
            lockMgr.acquireDataverseReadLock(locks, itemTypeDataverseName);
        }
        if (metaItemTypeDataverseName != null && !metaItemTypeDataverseName.equals(dataverseName)
                && !metaItemTypeDataverseName.equals(itemTypeDataverseName)) {
            lockMgr.acquireDataverseReadLock(locks, metaItemTypeDataverseName);
        }
        if (itemTypeAnonymous) {
            // the datatype will be created
            lockMgr.acquireDataTypeWriteLock(locks, itemTypeDataverseName, itemTypeName);
        } else {
            lockMgr.acquireDataTypeReadLock(locks, itemTypeDataverseName, itemTypeName);
        }
        if (metaItemTypeDataverseName != null && !metaItemTypeDataverseName.equals(itemTypeDataverseName)
                && !metaItemTypeName.equals(itemTypeName)) {
            if (metaItemTypeAnonymous) {
                // the datatype will be created
                lockMgr.acquireDataTypeWriteLock(locks, metaItemTypeDataverseName, metaItemTypeName);
            } else {
                lockMgr.acquireDataTypeReadLock(locks, metaItemTypeDataverseName, metaItemTypeName);
            }
        }
        if (nodeGroupName != null) {
            lockMgr.acquireNodeGroupReadLock(locks, nodeGroupName);
        }
        if (!isDefaultCompactionPolicy) {
            lockMgr.acquireMergePolicyReadLock(locks, compactionPolicyName);
        }
    }

    @Override
    public void createIndexBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetCreateIndexLock(locks, dataverseName, datasetName);
    }

    @Override
    public void dropIndexBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, dataverseName, datasetName);
    }

    @Override
    public void createTypeBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String typeName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, dataverseName, typeName);
    }

    @Override
    public void dropDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, dataverseName, datasetName);
    }

    @Override
    public void dropTypeBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String typeName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, dataverseName, typeName);
    }

    @Override
    public void createLibraryBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireLibraryWriteLock(locks, dataverseName, libraryName);
    }

    @Override
    public void dropLibraryBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireLibraryWriteLock(locks, dataverseName, libraryName);
    }

    @Override
    public void createFunctionBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String functionName, DataverseName libraryDataverseName, String libraryName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, dataverseName, functionName);
        if (libraryName != null) {
            if (!dataverseName.equals(libraryDataverseName)) {
                lockMgr.acquireDataverseReadLock(locks, libraryDataverseName);
            }
            lockMgr.acquireLibraryReadLock(locks, libraryDataverseName, libraryName);
        }
    }

    @Override
    public void dropFunctionBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String functionName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, dataverseName, functionName);
    }

    @Override
    public void createAdapterBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String adapterName, DataverseName libraryDataverseName, String libraryName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireAdapterWriteLock(locks, dataverseName, adapterName);
        if (libraryName != null) {
            if (!dataverseName.equals(libraryDataverseName)) {
                lockMgr.acquireDataverseReadLock(locks, libraryDataverseName);
            }
            lockMgr.acquireLibraryReadLock(locks, libraryDataverseName, libraryName);
        }
    }

    @Override
    public void dropAdapterBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String adapterName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireAdapterWriteLock(locks, dataverseName, adapterName);
    }

    @Override
    public void createSynonymBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireSynonymWriteLock(locks, dataverseName, synonymName);
    }

    @Override
    public void dropSynonymBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireSynonymWriteLock(locks, dataverseName, synonymName);
    }

    @Override
    public void modifyDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetModifyLock(locks, dataverseName, datasetName);
    }

    @Override
    public void insertDeleteUpsertBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetModifyLock(locks, dataverseName, datasetName);
    }

    @Override
    public void dropFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, dataverseName, feedName);
    }

    @Override
    public void dropFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, dataverseName, policyName);
    }

    @Override
    public void startFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
    }

    @Override
    public void stopFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        // TODO: dataset lock?
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
    }

    @Override
    public void createFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, dataverseName, feedName);
    }

    @Override
    public void connectFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName, String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
        lockMgr.acquireDatasetReadLock(locks, dataverseName, datasetName);
    }

    @Override
    public void createFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFeedPolicyWriteLock(locks, dataverseName, policyName);
    }

    @Override
    public void disconnectFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName, String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
        lockMgr.acquireDatasetReadLock(locks, dataverseName, datasetName);
    }

    @Override
    public void compactBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetReadLock(locks, dataverseName, datasetName);
    }

    @Override
    public void refreshDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetExclusiveModificationLock(locks, dataverseName, datasetName);
    }
}
