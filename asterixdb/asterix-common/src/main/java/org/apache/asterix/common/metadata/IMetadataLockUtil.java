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

package org.apache.asterix.common.metadata;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public interface IMetadataLockUtil {

    // Dataverse helpers

    void createDataverseBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName)
            throws AlgebricksException;

    void dropDataverseBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName)
            throws AlgebricksException;

    // Dataset helpers

    void createDatasetBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName, DataverseName itemTypeDataverseName, String itemTypeName, boolean itemTypeAnonymous,
            DataverseName metaItemTypeDataverseName, String metaItemTypeName, boolean metaItemTypeAnonymous,
            String nodeGroupName, String compactionPolicyName, boolean isDefaultCompactionPolicy,
            DatasetConfig.DatasetType datasetType, Object datasetDetails) throws AlgebricksException;

    void dropDatasetBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException;

    void modifyDatasetBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException;

    void refreshDatasetBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException;

    void compactBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException;

    void insertDeleteUpsertBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException;

    // Index helpers

    void createIndexBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException;

    void dropIndexBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName) throws AlgebricksException;

    // Type helpers

    void createTypeBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String typeName)
            throws AlgebricksException;

    void dropTypeBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String typeName)
            throws AlgebricksException;

    // Function helpers

    void createLibraryBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String libraryName) throws AlgebricksException;

    void dropLibraryBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String libraryName) throws AlgebricksException;

    // Function helpers

    void createFunctionBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String functionName, DataverseName libraryDataverseName, String libraryName) throws AlgebricksException;

    void dropFunctionBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String functionName) throws AlgebricksException;

    // Adapter helpers

    void createAdapterBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String adapterName, DataverseName libraryDataverseName, String libraryName) throws AlgebricksException;

    void dropAdapterBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String adapterName) throws AlgebricksException;

    // Synonym helpers

    void createSynonymBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String synonymName) throws AlgebricksException;

    void dropSynonymBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String synonymName) throws AlgebricksException;

    // Feed helpers

    void createFeedPolicyBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String policyName) throws AlgebricksException;

    void dropFeedPolicyBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String policyName) throws AlgebricksException;

    void createFeedBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String feedName)
            throws AlgebricksException;

    void dropFeedBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String feedName)
            throws AlgebricksException;

    void startFeedBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String feedName)
            throws AlgebricksException;

    void stopFeedBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName, String feedName)
            throws AlgebricksException;

    void connectFeedBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName, String feedName) throws AlgebricksException;

    void disconnectFeedBegin(IMetadataLockManager lockManager, LockList locks, DataverseName dataverseName,
            String datasetName, String feedName) throws AlgebricksException;
}