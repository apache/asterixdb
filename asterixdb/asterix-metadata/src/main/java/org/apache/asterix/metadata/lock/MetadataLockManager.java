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
package org.apache.asterix.metadata.lock;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.utils.DatasetUtil;

public class MetadataLockManager {

    public static final MetadataLockManager INSTANCE = new MetadataLockManager();
    private static final Function<String, MetadataLock> LOCK_FUNCTION = MetadataLock::new;
    private static final Function<String, DatasetLock> DATASET_LOCK_FUNCTION = DatasetLock::new;

    private final ConcurrentHashMap<String, IMetadataLock> mdlocks;

    private static final String DATAVERSE_PREFIX = "Dataverse:";
    private static final String DATASET_PREFIX = "Dataset:";
    private static final String FUNCTION_PREFIX = "Function:";
    private static final String NODE_GROUP_PREFIX = "NodeGroup:";
    private static final String FEED_PREFIX = "Feed:";
    private static final String FEED_POLICY_PREFIX = "FeedPolicy:";
    private static final String MERGE_POLICY_PREFIX = "MergePolicy:";
    private static final String DATATYPE_PREFIX = "DataType:";
    private static final String EXTENSION_PREFIX = "Extension:";

    private MetadataLockManager() {
        mdlocks = new ConcurrentHashMap<>();
    }

    // Dataverse
    public void acquireDataverseReadLock(LockList locks, String dataverseName) throws AsterixException {
        String key = DATAVERSE_PREFIX + dataverseName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireDataverseWriteLock(LockList locks, String dataverseName) throws AsterixException {
        String key = DATAVERSE_PREFIX + dataverseName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    // Dataset
    public void acquireDatasetReadLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireDatasetWriteLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    public void acquireDatasetModifyLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.MODIFY, lock);
    }

    public void acquireDatasetCreateIndexLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.INDEX_BUILD, lock);
    }

    public void acquireExternalDatasetRefreshLock(LockList locks, String datasetName) throws AsterixException {
        String key = DATASET_PREFIX + datasetName;
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.INDEX_BUILD, lock);
    }

    // Function
    public void acquireFunctionReadLock(LockList locks, String functionName) throws AsterixException {
        String key = FUNCTION_PREFIX + functionName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireFunctionWriteLock(LockList locks, String functionName) throws AsterixException {
        String key = FUNCTION_PREFIX + functionName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    // Node Group
    public void acquireNodeGroupReadLock(LockList locks, String nodeGroupName) throws AsterixException {
        String key = NODE_GROUP_PREFIX + nodeGroupName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireNodeGroupWriteLock(LockList locks, String nodeGroupName) throws AsterixException {
        String key = NODE_GROUP_PREFIX + nodeGroupName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    // Feeds
    public void acquireFeedReadLock(LockList locks, String feedName) throws AsterixException {
        String key = FEED_PREFIX + feedName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireFeedWriteLock(LockList locks, String feedName) throws AsterixException {
        String key = FEED_PREFIX + feedName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    public void acquireFeedPolicyWriteLock(LockList locks, String feedPolicyName) throws AsterixException {
        String key = FEED_POLICY_PREFIX + feedPolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    public void acquireFeedPolicyReadLock(LockList locks, String feedPolicyName) throws AsterixException {
        String key = FEED_POLICY_PREFIX + feedPolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    // CompactionPolicy
    public void acquireMergePolicyReadLock(LockList locks, String mergePolicyName) throws AsterixException {
        String key = MERGE_POLICY_PREFIX + mergePolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireMergePolicyWriteLock(LockList locks, String mergePolicyName) throws AsterixException {
        String key = MERGE_POLICY_PREFIX + mergePolicyName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    // DataType
    public void acquireDataTypeReadLock(LockList locks, String datatypeName) throws AsterixException {
        String key = DATATYPE_PREFIX + datatypeName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireDataTypeWriteLock(LockList locks, String datatypeName) throws AsterixException {
        String key = DATATYPE_PREFIX + datatypeName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    // Extensions
    public void acquireExtensionReadLock(LockList locks, String extensionName) throws AsterixException {
        String key = EXTENSION_PREFIX + extensionName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    public void acquireExtensionWriteLock(LockList locks, String extensionName) throws AsterixException {
        String key = EXTENSION_PREFIX + extensionName;
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    public void createDatasetBegin(LockList locks, String dataverseName, String itemTypeDataverseName,
            String itemTypeFullyQualifiedName, String metaItemTypeDataverseName, String metaItemTypeFullyQualifiedName,
            String nodeGroupName, String compactionPolicyName, String datasetFullyQualifiedName,
            boolean isDefaultCompactionPolicy) throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        if (!dataverseName.equals(itemTypeDataverseName)) {
            acquireDataverseReadLock(locks, itemTypeDataverseName);
        }
        if (metaItemTypeDataverseName != null && !metaItemTypeDataverseName.equals(dataverseName)
                && !metaItemTypeDataverseName.equals(itemTypeDataverseName)) {
            acquireDataverseReadLock(locks, metaItemTypeDataverseName);
        }
        acquireDataTypeReadLock(locks, itemTypeFullyQualifiedName);
        if (metaItemTypeFullyQualifiedName != null
                && !metaItemTypeFullyQualifiedName.equals(itemTypeFullyQualifiedName)) {
            acquireDataTypeReadLock(locks, metaItemTypeFullyQualifiedName);
        }
        if (nodeGroupName != null) {
            acquireNodeGroupReadLock(locks, nodeGroupName);
        }
        if (!isDefaultCompactionPolicy) {
            acquireMergePolicyReadLock(locks, compactionPolicyName);
        }
        acquireDatasetWriteLock(locks, datasetFullyQualifiedName);
    }

    public void createIndexBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetCreateIndexLock(locks, datasetFullyQualifiedName);
    }

    public void createTypeBegin(LockList locks, String dataverseName, String itemTypeFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDataTypeWriteLock(locks, itemTypeFullyQualifiedName);
    }

    public void dropDatasetBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetWriteLock(locks, datasetFullyQualifiedName);
    }

    public void dropIndexBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetWriteLock(locks, datasetFullyQualifiedName);
    }

    public void dropTypeBegin(LockList locks, String dataverseName, String dataTypeFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDataTypeWriteLock(locks, dataTypeFullyQualifiedName);
    }

    public void functionStatementBegin(LockList locks, String dataverseName, String functionFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireFunctionWriteLock(locks, functionFullyQualifiedName);
    }

    public void modifyDatasetBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetModifyLock(locks, datasetFullyQualifiedName);
    }

    public void insertDeleteUpsertBegin(LockList locks, String datasetFullyQualifiedName) throws AsterixException {
        acquireDataverseReadLock(locks, DatasetUtil.getDataverseFromFullyQualifiedName(datasetFullyQualifiedName));
        acquireDatasetModifyLock(locks, datasetFullyQualifiedName);
    }

    public void dropFeedBegin(LockList locks, String dataverseName, String feedFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireFeedWriteLock(locks, feedFullyQualifiedName);
    }

    public void dropFeedPolicyBegin(LockList locks, String dataverseName, String policyName) throws AsterixException {
        acquireFeedWriteLock(locks, policyName);
        acquireDataverseReadLock(locks, dataverseName);
    }

    public void startFeedBegin(LockList locks, String dataverseName, String feedName,
            List<FeedConnection> feedConnections) throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireFeedReadLock(locks, feedName);
        for (FeedConnection feedConnection : feedConnections) {
            // what if the dataset is in a different dataverse
            String fqName = dataverseName + "." + feedConnection.getDatasetName();
            acquireDatasetReadLock(locks, fqName);
        }
    }

    public void stopFeedBegin(LockList locks, String dataverseName, String feedName) throws AsterixException {
        // TODO: dataset lock?
        // Dataset locks are not required here since datasets are protected by the active event listener
        acquireDataverseReadLock(locks, dataverseName);
        acquireFeedReadLock(locks, feedName);
    }

    public void createFeedBegin(LockList locks, String dataverseName, String feedFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireFeedWriteLock(locks, feedFullyQualifiedName);
    }

    public void connectFeedBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName,
            String feedFullyQualifiedName) throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetReadLock(locks, datasetFullyQualifiedName);
        acquireFeedReadLock(locks, feedFullyQualifiedName);
    }

    public void createFeedPolicyBegin(LockList locks, String dataverseName, String policyName) throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireFeedPolicyWriteLock(locks, policyName);
    }

    public void disconnectFeedBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName,
            String feedFullyQualifiedName) throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetReadLock(locks, datasetFullyQualifiedName);
        acquireFeedReadLock(locks, feedFullyQualifiedName);
    }

    public void compactBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireDatasetReadLock(locks, datasetFullyQualifiedName);
    }

    public void refreshDatasetBegin(LockList locks, String dataverseName, String datasetFullyQualifiedName)
            throws AsterixException {
        acquireDataverseReadLock(locks, dataverseName);
        acquireExternalDatasetRefreshLock(locks, datasetFullyQualifiedName);
    }
}
