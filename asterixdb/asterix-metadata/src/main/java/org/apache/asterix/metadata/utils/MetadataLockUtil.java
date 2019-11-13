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
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.LockList;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class MetadataLockUtil {

    private MetadataLockUtil() {
    }

    public static void createDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName, DataverseName itemTypeDataverseName, String itemTypeName,
            DataverseName metaItemTypeDataverseName, String metaItemTypeName, String nodeGroupName,
            String compactionPolicyName, boolean isDefaultCompactionPolicy) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        if (!dataverseName.equals(itemTypeDataverseName)) {
            lockMgr.acquireDataverseReadLock(locks, itemTypeDataverseName);
        }
        if (metaItemTypeDataverseName != null && !metaItemTypeDataverseName.equals(dataverseName)
                && !metaItemTypeDataverseName.equals(itemTypeDataverseName)) {
            lockMgr.acquireDataverseReadLock(locks, metaItemTypeDataverseName);
        }
        lockMgr.acquireDataTypeReadLock(locks, itemTypeDataverseName, itemTypeName);
        if (metaItemTypeDataverseName != null && !metaItemTypeDataverseName.equals(itemTypeDataverseName)
                && !metaItemTypeName.equals(itemTypeName)) {
            lockMgr.acquireDataTypeReadLock(locks, metaItemTypeDataverseName, metaItemTypeName);
        }
        if (nodeGroupName != null) {
            lockMgr.acquireNodeGroupReadLock(locks, nodeGroupName);
        }
        if (!isDefaultCompactionPolicy) {
            lockMgr.acquireMergePolicyReadLock(locks, compactionPolicyName);
        }
        lockMgr.acquireDatasetWriteLock(locks, dataverseName, datasetName);
    }

    public static void createIndexBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetCreateIndexLock(locks, dataverseName, datasetName);
    }

    public static void dropIndexBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, dataverseName, datasetName);
    }

    public static void createTypeBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String typeName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, dataverseName, typeName);
    }

    public static void dropDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, dataverseName, datasetName);
    }

    public static void dropTypeBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String typeName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, dataverseName, typeName);
    }

    public static void createFunctionBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String functionName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, dataverseName, functionName);
    }

    public static void dropFunctionBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String functionName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, dataverseName, functionName);
    }

    public static void modifyDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetModifyLock(locks, dataverseName, datasetName);
    }

    public static void insertDeleteUpsertBegin(IMetadataLockManager lockMgr, LockList locks,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetModifyLock(locks, dataverseName, datasetName);
    }

    public static void dropFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, dataverseName, feedName);
    }

    public static void dropFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, dataverseName, policyName);
    }

    public static void startFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
    }

    public static void stopFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        // TODO: dataset lock?
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
    }

    public static void createFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, dataverseName, feedName);
    }

    public static void connectFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName, String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
        lockMgr.acquireDatasetReadLock(locks, dataverseName, datasetName);
    }

    public static void createFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String policyName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireFeedPolicyWriteLock(locks, dataverseName, policyName);
    }

    public static void disconnectFeedBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName, String feedName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, dataverseName, feedName);
        lockMgr.acquireDatasetReadLock(locks, dataverseName, datasetName);
    }

    public static void compactBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetReadLock(locks, dataverseName, datasetName);
    }

    public static void refreshDatasetBegin(IMetadataLockManager lockMgr, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDataverseReadLock(locks, dataverseName);
        lockMgr.acquireDatasetExclusiveModificationLock(locks, dataverseName, datasetName);
    }
}
