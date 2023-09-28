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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.metadata.LockList;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class MetadataLockUtil implements IMetadataLockUtil {

    @Override
    public void createDatabaseBegin(IMetadataLockManager lockMgr, LockList locks, String database)
            throws AlgebricksException {
        //TODO(DB): write lock?
        lockMgr.acquireDatabaseReadLock(locks, database);
    }

    @Override
    public void dropDatabaseBegin(IMetadataLockManager lockMgr, LockList locks, String database)
            throws AlgebricksException {
        lockMgr.acquireDatabaseWriteLock(locks, database);
    }

    @Override
    public void createDataverseBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseWriteLock(locks, database, dataverseName);
    }

    @Override
    public void dropDataverseBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseWriteLock(locks, database, dataverseName);
    }

    @Override
    public void createDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName, String itemTypeDatabase,
            DataverseName itemTypeDataverseName, String itemTypeName, boolean itemTypeAnonymous,
            String metaItemTypeDatabase, DataverseName metaItemTypeDataverseName, String metaItemTypeName,
            boolean metaItemTypeAnonymous, String nodeGroupName, String compactionPolicyName,
            boolean isDefaultCompactionPolicy, DatasetConfig.DatasetType datasetType, Object datasetDetails)
            throws AlgebricksException {
        createDatasetBeginPre(lockMgr, locks, database, dataverseName, itemTypeDatabase, itemTypeDataverseName,
                itemTypeName, itemTypeAnonymous, metaItemTypeDatabase, metaItemTypeDataverseName, metaItemTypeName,
                metaItemTypeAnonymous, nodeGroupName, compactionPolicyName, isDefaultCompactionPolicy);
        lockMgr.acquireDatasetWriteLock(locks, database, dataverseName, datasetName);
    }

    protected final void createDatasetBeginPre(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String itemTypeDatabase, DataverseName itemTypeDataverseName,
            String itemTypeName, boolean itemTypeAnonymous, String metaItemTypeDatabase,
            DataverseName metaItemTypeDataverseName, String metaItemTypeName, boolean metaItemTypeAnonymous,
            String nodeGroupName, String compactionPolicyName, boolean isDefaultCompactionPolicy)
            throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockIfDifferentNamespace(lockMgr, locks, database, dataverseName, itemTypeDatabase, itemTypeDataverseName);
        lockIfDifferentNamespace(lockMgr, locks, database, dataverseName, itemTypeDatabase, itemTypeDataverseName,
                metaItemTypeDatabase, metaItemTypeDataverseName);

        if (itemTypeAnonymous) {
            // the datatype will be created
            lockMgr.acquireDataTypeWriteLock(locks, itemTypeDatabase, itemTypeDataverseName, itemTypeName);
        } else {
            lockMgr.acquireDataTypeReadLock(locks, itemTypeDatabase, itemTypeDataverseName, itemTypeName);
        }
        if (metaItemTypeDatabase != null && metaItemTypeDataverseName != null && !sameNamespace(metaItemTypeDatabase,
                metaItemTypeDataverseName, itemTypeDatabase, itemTypeDataverseName)
                && !metaItemTypeName.equals(itemTypeName)) {
            //TODO(DB): why check the type name?
            if (metaItemTypeAnonymous) {
                // the datatype will be created
                lockMgr.acquireDataTypeWriteLock(locks, metaItemTypeDatabase, metaItemTypeDataverseName,
                        metaItemTypeName);
            } else {
                lockMgr.acquireDataTypeReadLock(locks, metaItemTypeDatabase, metaItemTypeDataverseName,
                        metaItemTypeName);
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
    public void createIndexBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName, String fullTextConfigName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetCreateIndexLock(locks, database, dataverseName, datasetName);
        if (!Strings.isNullOrEmpty(fullTextConfigName)) {
            lockMgr.acquireFullTextConfigReadLock(locks, database, dataverseName, fullTextConfigName);
        }
    }

    @Override
    public void dropIndexBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void createTypeBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String typeName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, database, dataverseName, typeName);
    }

    @Override
    public void dropDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void dropTypeBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String typeName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDataTypeWriteLock(locks, database, dataverseName, typeName);
    }

    @Override
    public void createLibraryBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String libraryName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireLibraryWriteLock(locks, database, dataverseName, libraryName);
    }

    @Override
    public void dropLibraryBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String libraryName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireLibraryWriteLock(locks, database, dataverseName, libraryName);
    }

    @Override
    public void createFunctionBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String functionName, String libraryDatabase,
            DataverseName libraryDataverseName, String libraryName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, database, dataverseName, functionName);
        if (libraryName != null) {
            lockIfDifferentNamespace(lockMgr, locks, database, dataverseName, libraryDatabase, libraryDataverseName);
            lockMgr.acquireLibraryReadLock(locks, libraryDatabase, libraryDataverseName, libraryName);
        }
    }

    @Override
    public void dropFunctionBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String functionName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFunctionWriteLock(locks, database, dataverseName, functionName);
    }

    @Override
    public void createFullTextFilterBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String fullTextFilterName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFullTextFilterWriteLock(locks, database, dataverseName, fullTextFilterName);
    }

    @Override
    public void dropFullTextFilterBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String fullTextFilterName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFullTextFilterWriteLock(locks, database, dataverseName, fullTextFilterName);
    }

    @Override
    public void createFullTextConfigBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String fullTextConfigName, ImmutableList<String> fullTextFilterNames)
            throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFullTextConfigWriteLock(locks, database, dataverseName, fullTextConfigName);

        // We should avoid sorting the original list, and the original list is immutable and cannot be sorted anyway
        List<String> fullTextFilterNamesMutable = new ArrayList<>(fullTextFilterNames);

        // sort the filters to guarantee locks are always fetched in the same order to avoid dead lock between filters
        Collections.sort(fullTextFilterNamesMutable);
        for (String filterName : fullTextFilterNamesMutable) {
            lockMgr.acquireFullTextFilterReadLock(locks, database, dataverseName, filterName);
        }
    }

    @Override
    public void dropFullTextConfigBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String configName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFullTextConfigWriteLock(locks, database, dataverseName, configName);
    }

    @Override
    public void createAdapterBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String adapterName, String libraryDatabase, DataverseName libraryDataverseName,
            String libraryName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireAdapterWriteLock(locks, database, dataverseName, adapterName);
        if (libraryName != null) {
            lockIfDifferentNamespace(lockMgr, locks, database, dataverseName, libraryDatabase, libraryDataverseName);
            lockMgr.acquireLibraryReadLock(locks, libraryDatabase, libraryDataverseName, libraryName);
        }
    }

    @Override
    public void dropAdapterBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String adapterName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireAdapterWriteLock(locks, database, dataverseName, adapterName);
    }

    @Override
    public void createSynonymBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String synonymName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireSynonymWriteLock(locks, database, dataverseName, synonymName);
    }

    @Override
    public void dropSynonymBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String synonymName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireSynonymWriteLock(locks, database, dataverseName, synonymName);
    }

    @Override
    public void modifyDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetModifyLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void insertDeleteUpsertBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetExclusiveModificationLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void dropFeedBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String feedName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, database, dataverseName, feedName);
    }

    @Override
    public void dropFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String policyName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, database, dataverseName, policyName);
    }

    @Override
    public void startFeedBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String feedName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, database, dataverseName, feedName);
    }

    @Override
    public void stopFeedBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String feedName) throws AlgebricksException {
        // TODO: dataset lock?
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, database, dataverseName, feedName);
    }

    @Override
    public void createFeedBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String feedName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityWriteLock(locks, database, dataverseName, feedName);
    }

    @Override
    public void connectFeedBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName, String feedName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, database, dataverseName, feedName);
        lockMgr.acquireDatasetReadLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void createFeedPolicyBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String policyName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireFeedPolicyWriteLock(locks, database, dataverseName, policyName);
    }

    @Override
    public void disconnectFeedBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName, String feedName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireActiveEntityReadLock(locks, database, dataverseName, feedName);
        lockMgr.acquireDatasetReadLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void analyzeDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetCreateIndexLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void analyzeDatasetDropBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetWriteLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void compactBegin(IMetadataLockManager lockMgr, LockList locks, String database, DataverseName dataverseName,
            String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetReadLock(locks, database, dataverseName, datasetName);
    }

    @Override
    public void refreshDatasetBegin(IMetadataLockManager lockMgr, LockList locks, String database,
            DataverseName dataverseName, String datasetName) throws AlgebricksException {
        lockMgr.acquireDatabaseReadLock(locks, database);
        lockMgr.acquireDataverseReadLock(locks, database, dataverseName);
        lockMgr.acquireDatasetExclusiveModificationLock(locks, database, dataverseName, datasetName);
    }

    private static void lockIfDifferentNamespace(IMetadataLockManager lockMgr, LockList locks, String lockedDatabase,
            DataverseName lockedDataverse, String toBeLockedDatabase, DataverseName toBeLockedDataverse)
            throws AlgebricksException {
        if (toBeLockedDatabase != null && toBeLockedDataverse != null) {
            if (!Objects.equals(lockedDatabase, toBeLockedDatabase)) {
                lockMgr.acquireDatabaseReadLock(locks, toBeLockedDatabase);
                lockMgr.acquireDataverseReadLock(locks, toBeLockedDatabase, toBeLockedDataverse);
            } else if (!Objects.equals(lockedDataverse, toBeLockedDataverse)) {
                lockMgr.acquireDataverseReadLock(locks, toBeLockedDatabase, toBeLockedDataverse);
            }
        }
    }

    private static void lockIfDifferentNamespace(IMetadataLockManager lockMgr, LockList locks, String lockedDatabase1,
            DataverseName lockedDataverse1, String lockedDatabase2, DataverseName lockedDataverse2,
            String toBeLockedDatabase, DataverseName toBeLockedDataverse) throws AlgebricksException {
        if (toBeLockedDatabase != null && toBeLockedDataverse != null) {
            if (!Objects.equals(lockedDatabase1, toBeLockedDatabase)) {
                if (!Objects.equals(lockedDatabase2, toBeLockedDatabase)) {
                    lockMgr.acquireDatabaseReadLock(locks, toBeLockedDatabase);
                    lockMgr.acquireDataverseReadLock(locks, toBeLockedDatabase, toBeLockedDataverse);
                } else if (!Objects.equals(lockedDataverse2, toBeLockedDataverse)) {
                    lockMgr.acquireDataverseReadLock(locks, toBeLockedDatabase, toBeLockedDataverse);
                }
            } else if (!Objects.equals(lockedDataverse1, toBeLockedDataverse)) {
                lockMgr.acquireDataverseReadLock(locks, toBeLockedDatabase, toBeLockedDataverse);
            }
        }
    }

    private static boolean sameNamespace(String database1, DataverseName dataverse1, String database2,
            DataverseName dataverse2) {
        return database1.equals(database2) && dataverse1.equals(dataverse2);
    }
}
