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
package org.apache.asterix.common.api;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.LockList;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public interface IMetadataLockManager {

    /**
     * Acquire read lock on the dataverse
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDataverseReadLock(LockList locks, DataverseName dataverseName) throws AlgebricksException;

    /**
     * Acquire write lock on the dataverse
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDataverseWriteLock(LockList locks, DataverseName dataverseName) throws AlgebricksException;

    /**
     * Acquire read lock on the dataset (for queries)
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetReadLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the dataset (for dataset create, dataset drop, and index drop)
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetWriteLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Acquire modify lock on the dataset (for inserts, upserts, deletes) Mutually exclusive with create index lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetModifyLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Acquire create index lock on the dataset (for index creation) Mutually exclusive with modify lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetCreateIndexLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Acquire exclusive modify lock on the dataset. only a single thread can acquire this lock and it is mutually
     * exclusive with modify locks and index build lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetExclusiveModificationLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the function
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param functionName
     *            the name of the function in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFunctionReadLock(LockList locks, DataverseName dataverseName, String functionName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the function
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param functionName
     *            the name of the function in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFunctionWriteLock(LockList locks, DataverseName dataverseName, String functionName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the library
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param libraryName
     *            the name of the library in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */

    void acquireLibraryReadLock(LockList locks, DataverseName dataverseName, String libraryName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the library
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param libraryName
     *            the name of the library in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireLibraryWriteLock(LockList locks, DataverseName dataverseName, String libraryName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the adapter
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param adapterName
     *            the name of the adapter in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */

    void acquireAdapterReadLock(LockList locks, DataverseName dataverseName, String adapterName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the adapter
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param adapterName
     *            the name of the adapter in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */

    void acquireAdapterWriteLock(LockList locks, DataverseName dataverseName, String adapterName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the node group
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param nodeGroupName
     *            the name of the node group
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireNodeGroupReadLock(LockList locks, String nodeGroupName) throws AlgebricksException;

    /**
     * Acquire write lock on the node group
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param nodeGroupName
     *            the name of the node group
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireNodeGroupWriteLock(LockList locks, String nodeGroupName) throws AlgebricksException;

    /**
     * Acquire read lock on the active entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param entityName
     *            the name of the active entity in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireActiveEntityReadLock(LockList locks, DataverseName dataverseName, String entityName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the active entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param entityName
     *            the name of the active entity in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireActiveEntityWriteLock(LockList locks, DataverseName dataverseName, String entityName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the feed policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param feedPolicyName
     *            the name of the feed policy in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFeedPolicyWriteLock(LockList locks, DataverseName dataverseName, String feedPolicyName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the feed policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param feedPolicyName
     *            the name of the feed policy in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFeedPolicyReadLock(LockList locks, DataverseName dataverseName, String feedPolicyName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the merge policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param mergePolicyName
     *            the name of the merge policy in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireMergePolicyReadLock(LockList locks, String mergePolicyName) throws AlgebricksException;

    /**
     * Acquire write lock on the merge policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param mergePolicyName
     *            the name of the merge policy in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireMergePolicyWriteLock(LockList locks, String mergePolicyName) throws AlgebricksException;

    /**
     * Acquire read lock on the data type
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datatypeName
     *            the name of the data type in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDataTypeReadLock(LockList locks, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the data type
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datatypeName
     *            the name of the data type in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDataTypeWriteLock(LockList locks, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the synonym
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param synonymName
     *            the name of the synonym in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireSynonymReadLock(LockList locks, DataverseName dataverseName, String synonymName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the synonym
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param synonymName
     *            the name of the synonym in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireSynonymWriteLock(LockList locks, DataverseName dataverseName, String synonymName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the extension entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param extension
     *            the extension key
     * @param dataverseName
     *            the dataverse name
     * @param extensionEntityName
     *            the name of the extension entity in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireExtensionEntityReadLock(LockList locks, String extension, DataverseName dataverseName,
            String extensionEntityName) throws AlgebricksException;

    /**
     * Acquire write lock on the extension entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param extension
     *            the extension key
     * @param dataverseName
     *            the dataverse name
     * @param extensionEntityName
     *            the name of the extension entity in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireExtensionEntityWriteLock(LockList locks, String extension, DataverseName dataverseName,
            String extensionEntityName) throws AlgebricksException;

    /**
     * Upgrade a previously acquired exclusive modification lock on the dataset to a write lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be upgraded
     */
    void upgradeDatasetLockToWrite(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    /**
     * Downgrade an upgraded dataset write lock to an exclusive modification lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param dataverseName
     *            the dataverse name
     * @param datasetName
     *            the name of the dataset in the given dataverse
     * @throws AlgebricksException
     *             if lock couldn't be downgraded
     */
    void downgradeDatasetLockToExclusiveModify(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;
}
