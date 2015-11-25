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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.asterix.metadata.entities.Dataverse;

public class MetadataLockManager {

    public static MetadataLockManager INSTANCE = new MetadataLockManager();
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> dataversesLocks;
    private final ConcurrentHashMap<String, DatasetLock> datasetsLocks;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> functionsLocks;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> nodeGroupsLocks;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> feedsLocks;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> feedPolicyLocks;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> compactionPolicyLocks;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> dataTypeLocks;

    private MetadataLockManager() {
        dataversesLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
        datasetsLocks = new ConcurrentHashMap<String, DatasetLock>();
        functionsLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
        nodeGroupsLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
        feedsLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
        feedPolicyLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
        compactionPolicyLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
        dataTypeLocks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
    }

    public void acquireDataverseReadLock(String dataverseName) {
        ReentrantReadWriteLock dvLock = dataversesLocks.get(dataverseName);
        if (dvLock == null) {
            dataversesLocks.putIfAbsent(dataverseName, new ReentrantReadWriteLock());
            dvLock = dataversesLocks.get(dataverseName);
        }
        dvLock.readLock().lock();
    }

    public void releaseDataverseReadLock(String dataverseName) {
        dataversesLocks.get(dataverseName).readLock().unlock();
    }

    public void acquireDataverseWriteLock(String dataverseName) {
        ReentrantReadWriteLock dvLock = dataversesLocks.get(dataverseName);
        if (dvLock == null) {
            dataversesLocks.putIfAbsent(dataverseName, new ReentrantReadWriteLock());
            dvLock = dataversesLocks.get(dataverseName);
        }
        dvLock.writeLock().lock();
    }

    public void releaseDataverseWriteLock(String dataverseName) {
        dataversesLocks.get(dataverseName).writeLock().unlock();
    }

    public void acquireDatasetReadLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        if (dsLock == null) {
            datasetsLocks.putIfAbsent(datasetName, new DatasetLock());
            dsLock = datasetsLocks.get(datasetName);
        }
        dsLock.acquireReadLock();
    }

    public void releaseDatasetReadLock(String datasetName) {
        datasetsLocks.get(datasetName).releaseReadLock();
    }

    public void acquireDatasetWriteLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        if (dsLock == null) {
            datasetsLocks.putIfAbsent(datasetName, new DatasetLock());
            dsLock = datasetsLocks.get(datasetName);
        }
        dsLock.acquireWriteLock();
    }

    public void releaseDatasetWriteLock(String datasetName) {
        datasetsLocks.get(datasetName).releaseWriteLock();
    }

    public void acquireDatasetModifyLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        if (dsLock == null) {
            datasetsLocks.putIfAbsent(datasetName, new DatasetLock());
            dsLock = datasetsLocks.get(datasetName);
        }
        dsLock.acquireReadLock();
        dsLock.acquireReadModifyLock();
    }

    public void releaseDatasetModifyLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        dsLock.releaseReadModifyLock();
        dsLock.releaseReadLock();
    }

    public void acquireDatasetCreateIndexLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        if (dsLock == null) {
            datasetsLocks.putIfAbsent(datasetName, new DatasetLock());
            dsLock = datasetsLocks.get(datasetName);
        }
        dsLock.acquireReadLock();
        dsLock.acquireWriteModifyLock();
    }

    public void releaseDatasetCreateIndexLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        dsLock.releaseWriteModifyLock();
        dsLock.releaseReadLock();
    }

    public void acquireExternalDatasetRefreshLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        if (dsLock == null) {
            datasetsLocks.putIfAbsent(datasetName, new DatasetLock());
            dsLock = datasetsLocks.get(datasetName);
        }
        dsLock.acquireReadLock();
        dsLock.acquireRefreshLock();
    }

    public void releaseExternalDatasetRefreshLock(String datasetName) {
        DatasetLock dsLock = datasetsLocks.get(datasetName);
        dsLock.releaseRefreshLock();
        dsLock.releaseReadLock();
    }

    public void acquireFunctionReadLock(String functionName) {
        ReentrantReadWriteLock fLock = functionsLocks.get(functionName);
        if (fLock == null) {
            functionsLocks.putIfAbsent(functionName, new ReentrantReadWriteLock());
            fLock = functionsLocks.get(functionName);
        }
        fLock.readLock().lock();
    }

    public void releaseFunctionReadLock(String functionName) {
        functionsLocks.get(functionName).readLock().unlock();
    }

    public void acquireFunctionWriteLock(String functionName) {
        ReentrantReadWriteLock fLock = functionsLocks.get(functionName);
        if (fLock == null) {
            functionsLocks.putIfAbsent(functionName, new ReentrantReadWriteLock());
            fLock = functionsLocks.get(functionName);
        }
        fLock.writeLock().lock();
    }

    public void releaseFunctionWriteLock(String functionName) {
        functionsLocks.get(functionName).writeLock().unlock();
    }

    public void acquireNodeGroupReadLock(String nodeGroupName) {
        ReentrantReadWriteLock ngLock = nodeGroupsLocks.get(nodeGroupName);
        if (ngLock == null) {
            nodeGroupsLocks.putIfAbsent(nodeGroupName, new ReentrantReadWriteLock());
            ngLock = nodeGroupsLocks.get(nodeGroupName);
        }
        ngLock.readLock().lock();
    }

    public void releaseNodeGroupReadLock(String nodeGroupName) {
        nodeGroupsLocks.get(nodeGroupName).readLock().unlock();
    }

    public void acquireNodeGroupWriteLock(String nodeGroupName) {
        ReentrantReadWriteLock ngLock = nodeGroupsLocks.get(nodeGroupName);
        if (ngLock == null) {
            nodeGroupsLocks.putIfAbsent(nodeGroupName, new ReentrantReadWriteLock());
            ngLock = nodeGroupsLocks.get(nodeGroupName);
        }
        ngLock.writeLock().lock();
    }

    public void releaseNodeGroupWriteLock(String nodeGroupName) {
        nodeGroupsLocks.get(nodeGroupName).writeLock().unlock();
    }

    public void acquireFeedReadLock(String feedName) {
        ReentrantReadWriteLock fLock = feedsLocks.get(feedName);
        if (fLock == null) {
            feedsLocks.putIfAbsent(feedName, new ReentrantReadWriteLock());
            fLock = feedsLocks.get(feedName);
        }
        fLock.readLock().lock();
    }

    public void releaseFeedReadLock(String feedName) {
        feedsLocks.get(feedName).readLock().unlock();
    }

    public void acquireFeedWriteLock(String feedName) {
        ReentrantReadWriteLock fLock = feedsLocks.get(feedName);
        if (fLock == null) {
            feedsLocks.putIfAbsent(feedName, new ReentrantReadWriteLock());
            fLock = feedsLocks.get(feedName);
        }
        fLock.writeLock().lock();
    }

    public void releaseFeedWriteLock(String feedName) {
        feedsLocks.get(feedName).writeLock().unlock();
    }

    public void acquireFeedPolicyWriteLock(String policyName) {
        ReentrantReadWriteLock fLock = feedPolicyLocks.get(policyName);
        if (fLock == null) {
            feedPolicyLocks.putIfAbsent(policyName, new ReentrantReadWriteLock());
            fLock = feedPolicyLocks.get(policyName);
        }
        fLock.writeLock().lock();
    }

    public void releaseFeedPolicyWriteLock(String policyName) {
        feedPolicyLocks.get(policyName).writeLock().unlock();
    }

    public void acquireCompactionPolicyReadLock(String compactionPolicyName) {
        ReentrantReadWriteLock compactionPolicyLock = compactionPolicyLocks.get(compactionPolicyName);
        if (compactionPolicyLock == null) {
            compactionPolicyLocks.putIfAbsent(compactionPolicyName, new ReentrantReadWriteLock());
            compactionPolicyLock = compactionPolicyLocks.get(compactionPolicyName);
        }
        compactionPolicyLock.readLock().lock();
    }

    public void releaseCompactionPolicyReadLock(String compactionPolicyName) {
        compactionPolicyLocks.get(compactionPolicyName).readLock().unlock();
    }

    public void acquireCompactionPolicyWriteLock(String compactionPolicyName) {
        ReentrantReadWriteLock compactionPolicyLock = compactionPolicyLocks.get(compactionPolicyName);
        if (compactionPolicyLock == null) {
            compactionPolicyLocks.putIfAbsent(compactionPolicyName, new ReentrantReadWriteLock());
            compactionPolicyLock = compactionPolicyLocks.get(compactionPolicyName);
        }
        compactionPolicyLock.writeLock().lock();
    }

    public void releaseCompactionPolicyWriteLock(String compactionPolicyName) {
        compactionPolicyLocks.get(compactionPolicyName).writeLock().unlock();
    }

    public void acquireDataTypeReadLock(String dataTypeName) {
        ReentrantReadWriteLock dataTypeLock = dataTypeLocks.get(dataTypeName);
        if (dataTypeLock == null) {
            dataTypeLocks.putIfAbsent(dataTypeName, new ReentrantReadWriteLock());
            dataTypeLock = dataTypeLocks.get(dataTypeName);
        }
        dataTypeLock.readLock().lock();
    }

    public void releaseDataTypeReadLock(String dataTypeName) {
        dataTypeLocks.get(dataTypeName).readLock().unlock();
    }

    public void acquireDataTypeWriteLock(String dataTypeName) {
        ReentrantReadWriteLock dataTypeLock = dataTypeLocks.get(dataTypeName);
        if (dataTypeLock == null) {
            dataTypeLocks.putIfAbsent(dataTypeName, new ReentrantReadWriteLock());
            dataTypeLock = dataTypeLocks.get(dataTypeName);
        }
        dataTypeLock.writeLock().lock();
    }

    public void releaseDataTypeWriteLock(String dataTypeName) {
        dataTypeLocks.get(dataTypeName).writeLock().unlock();
    }

    public void createDatasetBegin(String dataverseName, String itemTypeFullyQualifiedName, String nodeGroupName,
            String compactionPolicyName, String datasetFullyQualifiedName, boolean isDefaultCompactionPolicy) {
        acquireDataverseReadLock(dataverseName);
        acquireDataTypeReadLock(itemTypeFullyQualifiedName);
        acquireNodeGroupReadLock(nodeGroupName);
        if (!isDefaultCompactionPolicy) {
            acquireCompactionPolicyReadLock(compactionPolicyName);
        }
        acquireDatasetWriteLock(datasetFullyQualifiedName);
    }

    public void createDatasetEnd(String dataverseName, String itemTypeFullyQualifiedName, String nodeGroupName,
            String compactionPolicyName, String datasetFullyQualifiedName, boolean isDefaultCompactionPolicy) {
        releaseDatasetWriteLock(datasetFullyQualifiedName);
        if (!isDefaultCompactionPolicy) {
            releaseCompactionPolicyReadLock(compactionPolicyName);
        }
        releaseNodeGroupReadLock(nodeGroupName);
        releaseDataTypeReadLock(itemTypeFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void createIndexBegin(String dataverseName, String datasetFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetCreateIndexLock(datasetFullyQualifiedName);
    }

    public void createIndexEnd(String dataverseName, String datasetFullyQualifiedName) {
        releaseDatasetCreateIndexLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void createTypeBegin(String dataverseName, String itemTypeFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDataTypeWriteLock(itemTypeFullyQualifiedName);
    }

    public void createTypeEnd(String dataverseName, String itemTypeFullyQualifiedName) {
        releaseDataTypeWriteLock(itemTypeFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void dropDatasetBegin(String dataverseName, String datasetFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetWriteLock(datasetFullyQualifiedName);
    }

    public void dropDatasetEnd(String dataverseName, String datasetFullyQualifiedName) {
        releaseDatasetWriteLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void dropIndexBegin(String dataverseName, String datasetFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetWriteLock(datasetFullyQualifiedName);
    }

    public void dropIndexEnd(String dataverseName, String datasetFullyQualifiedName) {
        releaseDatasetWriteLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void dropTypeBegin(String dataverseName, String dataTypeFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDataTypeWriteLock(dataTypeFullyQualifiedName);
    }

    public void dropTypeEnd(String dataverseName, String dataTypeFullyQualifiedName) {
        releaseDataTypeWriteLock(dataTypeFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void functionStatementBegin(String dataverseName, String functionFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireFunctionWriteLock(functionFullyQualifiedName);
    }

    public void functionStatementEnd(String dataverseName, String functionFullyQualifiedName) {
        releaseFunctionWriteLock(functionFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void modifyDatasetBegin(String dataverseName, String datasetFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetModifyLock(datasetFullyQualifiedName);
    }

    public void modifyDatasetEnd(String dataverseName, String datasetFullyQualifiedName) {
        releaseDatasetModifyLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void insertDeleteBegin(String dataverseName, String datasetFullyQualifiedName, List<String> dataverses,
            List<String> datasets) {
        dataverses.add(dataverseName);
        datasets.add(datasetFullyQualifiedName);
        Collections.sort(dataverses);
        Collections.sort(datasets);

        String previous = null;
        for (int i = 0; i < dataverses.size(); i++) {
            String current = dataverses.get(i);
            if (!current.equals(previous)) {
                acquireDataverseReadLock(current);
                previous = current;
            }
        }

        for (int i = 0; i < datasets.size(); i++) {
            String current = datasets.get(i);
            if (!current.equals(previous)) {
                if (current.equals(datasetFullyQualifiedName)) {
                    acquireDatasetModifyLock(current);
                } else {
                    acquireDatasetReadLock(current);
                }
                previous = current;
            }
        }
    }

    public void insertDeleteEnd(String dataverseName, String datasetFullyQualifiedName, List<String> dataverses,
            List<String> datasets) {
        String previous = null;
        for (int i = dataverses.size() - 1; i >= 0; i--) {
            String current = dataverses.get(i);
            if (!current.equals(previous)) {
                releaseDataverseReadLock(current);
                previous = current;
            }
        }
        for (int i = datasets.size() - 1; i >= 0; i--) {
            String current = datasets.get(i);
            if (!current.equals(previous)) {
                if (current.equals(datasetFullyQualifiedName)) {
                    releaseDatasetModifyLock(current);
                } else {
                    releaseDatasetReadLock(current);
                }
                previous = current;
            }
        }
    }

    public void dropFeedBegin(String dataverseName, String feedFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireFeedWriteLock(feedFullyQualifiedName);
    }

    public void dropFeedEnd(String dataverseName, String feedFullyQualifiedName) {
        releaseFeedWriteLock(feedFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void dropFeedPolicyBegin(String dataverseName, String policyName) {
        releaseFeedWriteLock(policyName);
        releaseDataverseReadLock(dataverseName);
    }

    public void dropFeedPolicyEnd(String dataverseName, String policyName) {
        releaseFeedWriteLock(policyName);
        releaseDataverseReadLock(dataverseName);
    }

    public void createFeedBegin(String dataverseName, String feedFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireFeedWriteLock(feedFullyQualifiedName);
    }

    public void createFeedEnd(String dataverseName, String feedFullyQualifiedName) {
        releaseFeedWriteLock(feedFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void connectFeedBegin(String dataverseName, String datasetFullyQualifiedName, String feedFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetReadLock(datasetFullyQualifiedName);
        acquireFeedReadLock(feedFullyQualifiedName);
    }

    public void connectFeedEnd(String dataverseName, String datasetFullyQualifiedName, String feedFullyQualifiedName) {
        releaseFeedReadLock(feedFullyQualifiedName);
        releaseDatasetReadLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void createFeedPolicyBegin(String dataverseName, String policyName) {
        acquireDataverseReadLock(dataverseName);
        acquireFeedPolicyWriteLock(policyName);
    }

    public void createFeedPolicyEnd(String dataverseName, String policyName) {
        releaseFeedPolicyWriteLock(policyName);
        releaseDataverseReadLock(dataverseName);
    }

    public void disconnectFeedBegin(String dataverseName, String datasetFullyQualifiedName,
            String feedFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetReadLock(datasetFullyQualifiedName);
        acquireFeedReadLock(feedFullyQualifiedName);
    }

    public void disconnectFeedEnd(String dataverseName, String datasetFullyQualifiedName, String feedFullyQualifiedName) {
        releaseFeedReadLock(feedFullyQualifiedName);
        releaseDatasetReadLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void subscribeFeedBegin(String dataverseName, String datasetFullyQualifiedName, String feedFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetReadLock(datasetFullyQualifiedName);
        acquireFeedReadLock(feedFullyQualifiedName);
    }

    public void subscribeFeedEnd(String dataverseName, String datasetFullyQualifiedName, String feedFullyQualifiedName) {
        releaseFeedReadLock(feedFullyQualifiedName);
        releaseDatasetReadLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void compactBegin(String dataverseName, String datasetFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireDatasetReadLock(datasetFullyQualifiedName);
    }

    public void compactEnd(String dataverseName, String datasetFullyQualifiedName) {
        releaseDatasetReadLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }

    public void queryBegin(Dataverse dataverse, List<String> dataverses, List<String> datasets) {
        if (dataverse != null) {
            dataverses.add(dataverse.getDataverseName());
        }
        Collections.sort(dataverses);
        Collections.sort(datasets);

        String previous = null;
        for (int i = 0; i < dataverses.size(); i++) {
            String current = dataverses.get(i);
            if (!current.equals(previous)) {
                acquireDataverseReadLock(current);
                previous = current;
            }
        }

        for (int i = 0; i < datasets.size(); i++) {
            String current = datasets.get(i);
            if (!current.equals(previous)) {
                acquireDatasetReadLock(current);
                previous = current;
            }
        }
    }

    public void queryEnd(List<String> dataverses, List<String> datasets) {
        String previous = null;
        for (int i = dataverses.size() - 1; i >= 0; i--) {
            String current = dataverses.get(i);
            if (!current.equals(previous)) {
                releaseDataverseReadLock(current);
                previous = current;
            }
        }
        for (int i = datasets.size() - 1; i >= 0; i--) {
            String current = datasets.get(i);
            if (!current.equals(previous)) {
                releaseDatasetReadLock(current);
                previous = current;
            }
        }
    }

    public void refreshDatasetBegin(String dataverseName, String datasetFullyQualifiedName) {
        acquireDataverseReadLock(dataverseName);
        acquireExternalDatasetRefreshLock(datasetFullyQualifiedName);
    }

    public void refreshDatasetEnd(String dataverseName, String datasetFullyQualifiedName) {
        releaseExternalDatasetRefreshLock(datasetFullyQualifiedName);
        releaseDataverseReadLock(dataverseName);
    }
}
