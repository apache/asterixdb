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
    void acquireDataverseReadLock(LockList locks, String dataverseName) throws AlgebricksException;

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
    void acquireDataverseWriteLock(LockList locks, String dataverseName) throws AlgebricksException;

    /**
     * Acquire read lock on the dataset (for queries)
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetReadLock(LockList locks, String datasetFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire write lock on the dataset (for dataset create, dataset drop, and index drop)
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetWriteLock(LockList locks, String datasetFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire modify lock on the dataset (for inserts, upserts, deletes) Mutually exclusive with create index lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetModifyLock(LockList locks, String datasetFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire create index lock on the dataset (for index creation) Mutually exclusive with modify lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetCreateIndexLock(LockList locks, String datasetFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire exclusive modify lock on the dataset. only a single thread can acquire this lock and it is mutually
     * exclusive with modify locks and index build lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDatasetExclusiveModificationLock(LockList locks, String datasetFullyQualifiedName)
            throws AlgebricksException;

    /**
     * Acquire read lock on the function
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param functionFullyQualifiedName
     *            the fully qualified name of the function
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFunctionReadLock(LockList locks, String functionFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire write lock on the function
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param functionFullyQualifiedName
     *            the fully qualified name of the function
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFunctionWriteLock(LockList locks, String functionFullyQualifiedName) throws AlgebricksException;

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
     * @param entityFullyQualifiedName
     *            the fully qualified name of the active entity
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireActiveEntityReadLock(LockList locks, String entityFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire write lock on the active entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param entityFullyQualifiedName
     *            the fully qualified name of the active entity
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireActiveEntityWriteLock(LockList locks, String entityFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire read lock on the feed policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param feedPolicyFullyQualifiedName
     *            the fully qualified name of the feed policy
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFeedPolicyWriteLock(LockList locks, String feedPolicyFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire write lock on the feed policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param feedPolicyFullyQualifiedName
     *            the fully qualified name of the feed policy
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireFeedPolicyReadLock(LockList locks, String feedPolicyFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire read lock on the merge policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param mergePolicyFullyQualifiedName
     *            the fully qualified name of the merge policy
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireMergePolicyReadLock(LockList locks, String mergePolicyFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire write lock on the merge policy
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param mergePolicyFullyQualifiedName
     *            the fully qualified name of the merge policy
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireMergePolicyWriteLock(LockList locks, String mergePolicyFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire read lock on the data type
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datatypeFullyQualifiedName
     *            the fully qualified name of the data type
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDataTypeReadLock(LockList locks, String datatypeFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire write lock on the data type
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datatypeFullyQualifiedName
     *            the fully qualified name of the data type
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireDataTypeWriteLock(LockList locks, String datatypeFullyQualifiedName) throws AlgebricksException;

    /**
     * Acquire read lock on the extension entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param extension
     *            the extension key
     * @param extensionEntityFullyQualifiedName
     *            the fully qualified name of the extension entity
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireExtensionReadLock(LockList locks, String extension, String extensionEntityFullyQualifiedName)
            throws AlgebricksException;

    /**
     * Acquire write lock on the extension entity
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param extension
     *            the extension key
     * @param extensionEntityFullyQualifiedName
     *            the fully qualified name of the extension entity
     * @throws AlgebricksException
     *             if lock couldn't be acquired
     */
    void acquireExtensionWriteLock(LockList locks, String extension, String extensionEntityFullyQualifiedName)
            throws AlgebricksException;

    /**
     * Upgrade a previously acquired exclusive modification lock on the dataset to a write lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be upgraded
     */
    void upgradeDatasetLockToWrite(LockList locks, String datasetFullyQualifiedName) throws AlgebricksException;

    /**
     * Downgrade an upgraded dataset write lock to an exclusive modification lock
     *
     * @param locks
     *            the lock list to add the new lock to
     * @param datasetFullyQualifiedName
     *            the fully qualified name of the dataset
     * @throws AlgebricksException
     *             if lock couldn't be downgraded
     */
    void downgradeDatasetLockToExclusiveModify(LockList locks, String datasetFullyQualifiedName)
            throws AlgebricksException;
}
