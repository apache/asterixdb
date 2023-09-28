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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLock;
import org.apache.asterix.common.metadata.LockList;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class MetadataLockManager implements IMetadataLockManager {

    private static final Function<MetadataLockKey, MetadataLock> LOCK_FUNCTION = MetadataLock::new;
    private static final Function<MetadataLockKey, DatasetLock> DATASET_LOCK_FUNCTION = DatasetLock::new;

    private final ConcurrentMap<MetadataLockKey, IMetadataLock> mdlocks;

    public MetadataLockManager() {
        mdlocks = new ConcurrentHashMap<>();
    }

    @Override
    public void acquireDatabaseReadLock(LockList locks, String database) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatabaseLockKey(database);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDatabaseWriteLock(LockList locks, String database) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatabaseLockKey(database);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDataverseReadLock(LockList locks, String database, DataverseName dataverseName)
            throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDataverseLockKey(database, dataverseName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDataverseWriteLock(LockList locks, String database, DataverseName dataverseName)
            throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDataverseLockKey(database, dataverseName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDatasetReadLock(LockList locks, String database, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDatasetWriteLock(LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDatasetModifyLock(LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.MODIFY, lock);
    }

    @Override
    public void acquireDatasetCreateIndexLock(LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.INDEX_BUILD, lock);
    }

    @Override
    public void acquireDatasetExclusiveModificationLock(LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.EXCLUSIVE_MODIFY, lock);
    }

    @Override
    public void acquireFunctionReadLock(LockList locks, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFunctionLockKey(database, dataverseName, synonymName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireFunctionWriteLock(LockList locks, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFunctionLockKey(database, dataverseName, synonymName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFullTextConfigReadLock(LockList locks, String database, DataverseName dataverseName,
            String fullTextConfigName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFullTextConfigLockKey(database, dataverseName, fullTextConfigName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireFullTextConfigWriteLock(LockList locks, String database, DataverseName dataverseName,
            String fullTextConfigName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFullTextConfigLockKey(database, dataverseName, fullTextConfigName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFullTextFilterReadLock(LockList locks, String database, DataverseName dataverseName,
            String fullTextFilterName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFullTextFilterLockKey(database, dataverseName, fullTextFilterName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireFullTextFilterWriteLock(LockList locks, String database, DataverseName dataverseName,
            String fullTextFilterName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFullTextFilterLockKey(database, dataverseName, fullTextFilterName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireLibraryReadLock(LockList locks, String database, DataverseName dataverseName, String libraryName)
            throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createLibraryLockKey(database, dataverseName, libraryName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireLibraryWriteLock(LockList locks, String database, DataverseName dataverseName,
            String libraryName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createLibraryLockKey(database, dataverseName, libraryName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireAdapterReadLock(LockList locks, String database, DataverseName dataverseName, String adapterName)
            throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createAdapterLockKey(database, dataverseName, adapterName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireAdapterWriteLock(LockList locks, String database, DataverseName dataverseName,
            String adapterName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createAdapterLockKey(database, dataverseName, adapterName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireNodeGroupReadLock(LockList locks, String nodeGroupName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createNodeGroupLockKey(nodeGroupName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireNodeGroupWriteLock(LockList locks, String nodeGroupName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createNodeGroupLockKey(nodeGroupName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireActiveEntityReadLock(LockList locks, String database, DataverseName dataverseName,
            String entityName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createActiveEntityLockKey(database, dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireActiveEntityWriteLock(LockList locks, String database, DataverseName dataverseName,
            String entityName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createActiveEntityLockKey(database, dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFeedPolicyWriteLock(LockList locks, String database, DataverseName dataverseName,
            String feedPolicyName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFeedPolicyLockKey(database, dataverseName, feedPolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFeedPolicyReadLock(LockList locks, String database, DataverseName dataverseName,
            String feedPolicyName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createFeedPolicyLockKey(database, dataverseName, feedPolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireMergePolicyReadLock(LockList locks, String mergePolicyName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createMergePolicyLockKey(mergePolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireMergePolicyWriteLock(LockList locks, String mergePolicyName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createMergePolicyLockKey(mergePolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDataTypeReadLock(LockList locks, String database, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDataTypeLockKey(database, dataverseName, datatypeName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDataTypeWriteLock(LockList locks, String database, DataverseName dataverseName,
            String datatypeName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDataTypeLockKey(database, dataverseName, datatypeName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireSynonymReadLock(LockList locks, String database, DataverseName dataverseName, String synonymName)
            throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createSynonymLockKey(database, dataverseName, synonymName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireSynonymWriteLock(LockList locks, String database, DataverseName dataverseName,
            String synonymName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createSynonymLockKey(database, dataverseName, synonymName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireExtensionEntityReadLock(LockList locks, String extension, String database,
            DataverseName dataverseName, String entityName) throws AlgebricksException {
        MetadataLockKey key =
                MetadataLockKey.createExtensionEntityLockKey(extension, database, dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireExtensionEntityWriteLock(LockList locks, String extension, String database,
            DataverseName dataverseName, String entityName) throws AlgebricksException {
        MetadataLockKey key =
                MetadataLockKey.createExtensionEntityLockKey(extension, database, dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void upgradeDatasetLockToWrite(LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.upgrade(IMetadataLock.Mode.UPGRADED_WRITE, lock);
    }

    @Override
    public void downgradeDatasetLockToExclusiveModify(LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        MetadataLockKey key = MetadataLockKey.createDatasetLockKey(database, dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.downgrade(IMetadataLock.Mode.EXCLUSIVE_MODIFY, lock);
    }

}
