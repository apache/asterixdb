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
package org.apache.asterix.test.active;

import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.metadata.api.IActiveEntityController;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataLockUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TestUserActor extends Actor {

    private TestClusterControllerActor clusterController;
    private IMetadataLockManager lockManager;

    public TestUserActor(String name, MetadataProvider metadataProvider, TestClusterControllerActor clusterController) {
        super(name, metadataProvider);
        this.clusterController = clusterController;
        this.lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
    }

    public Action startActivity(IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = actionListener.getEntityId().getDataverse();
                String entityName = actionListener.getEntityId().getEntityName();
                try {
                    lockManager.acquireActiveEntityWriteLock(mdProvider.getLocks(), dataverseName + '.' + entityName);
                    List<Dataset> datasets = actionListener.getDatasets();
                    for (Dataset dataset : datasets) {
                        MetadataLockUtil.modifyDatasetBegin(lockManager, mdProvider.getLocks(), dataverseName,
                                DatasetUtil.getFullyQualifiedName(dataset));
                    }
                    actionListener.start(mdProvider);
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action stopActivity(IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = actionListener.getEntityId().getDataverse();
                String entityName = actionListener.getEntityId().getEntityName();
                try {
                    lockManager.acquireActiveEntityWriteLock(mdProvider.getLocks(), dataverseName + '.' + entityName);
                    List<Dataset> datasets = actionListener.getDatasets();
                    for (Dataset dataset : datasets) {
                        MetadataLockUtil.modifyDatasetBegin(lockManager, mdProvider.getLocks(), dataverseName,
                                DatasetUtil.getFullyQualifiedName(dataset));
                    }
                    actionListener.stop(mdProvider);
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action suspendActivity(IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = actionListener.getEntityId().getDataverse();
                String entityName = actionListener.getEntityId().getEntityName();
                List<Dataset> datasets = actionListener.getDatasets();
                try {
                    lockManager.acquireActiveEntityWriteLock(mdProvider.getLocks(), dataverseName + '.' + entityName);
                    for (Dataset dataset : datasets) {
                        lockManager.acquireDatasetExclusiveModificationLock(mdProvider.getLocks(),
                                DatasetUtil.getFullyQualifiedName(dataset));
                    }
                    actionListener.suspend(mdProvider);
                } catch (Exception e) {
                    // only release in case of failure
                    mdProvider.getLocks().reset();
                    throw e;
                }
            }
        };
        add(action);
        return action;
    }

    public Action resumeActivity(IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = actionListener.getEntityId().getDataverse();
                String entityName = actionListener.getEntityId().getEntityName();
                try {
                    lockManager.acquireActiveEntityWriteLock(mdProvider.getLocks(), dataverseName + '.' + entityName);
                    List<Dataset> datasets = actionListener.getDatasets();
                    for (Dataset dataset : datasets) {
                        lockManager.upgradeDatasetLockToWrite(mdProvider.getLocks(),
                                DatasetUtil.getFullyQualifiedName(dataset));
                        lockManager.downgradeDatasetLockToExclusiveModify(mdProvider.getLocks(),
                                DatasetUtil.getFullyQualifiedName(dataset));
                    }
                    actionListener.resume(mdProvider);
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action addDataset(Dataset dataset, IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String entityDataverseName = actionListener.getEntityId().getDataverse();
                String entityName = actionListener.getEntityId().getEntityName();
                try {
                    lockManager.acquireActiveEntityReadLock(mdProvider.getLocks(),
                            entityDataverseName + '.' + entityName);
                    lockManager.acquireDatasetWriteLock(mdProvider.getLocks(),
                            DatasetUtil.getFullyQualifiedName(dataset));
                    List<Dataset> datasets = clusterController.getAllDatasets();
                    if (datasets.contains(dataset)) {
                        throw new HyracksDataException("Dataset " + dataset + " already exists");
                    }
                    actionListener.add(dataset);
                    datasets.add(dataset);
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action dropDataset(Dataset dataset, IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String entityDataverseName = actionListener.getEntityId().getDataverse();
                String entityName = actionListener.getEntityId().getEntityName();
                try {
                    lockManager.acquireActiveEntityReadLock(mdProvider.getLocks(),
                            entityDataverseName + '.' + entityName); // we have to first read lock all active entities before deleting a dataset
                    lockManager.acquireDatasetWriteLock(mdProvider.getLocks(),
                            DatasetUtil.getFullyQualifiedName(dataset));
                    List<Dataset> datasets = clusterController.getAllDatasets();
                    if (!datasets.contains(dataset)) {
                        throw new HyracksDataException("Dataset " + dataset + " does not exist");
                    }
                    actionListener.remove(dataset);
                    datasets.remove(dataset);
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action addIndex(Dataset dataset, IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = dataset.getDataverseName();
                String datasetFullyQualifiedName = dataverseName + '.' + dataset.getDatasetName();
                String indexFullyQualifiedName = datasetFullyQualifiedName + ".index";
                try {
                    MetadataLockUtil.createIndexBegin(lockManager, mdProvider.getLocks(), dataverseName,
                            datasetFullyQualifiedName);
                    if (actionListener.isActive()) {
                        throw new RuntimeDataException(ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY,
                                indexFullyQualifiedName, actionListener.getEntityId(), actionListener.getState());
                    }
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action dropIndex(Dataset dataset, IActiveEntityController actionListener) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = dataset.getDataverseName();
                String datasetFullyQualifiedName = dataverseName + '.' + dataset.getDatasetName();
                try {
                    MetadataLockUtil.dropIndexBegin(lockManager, mdProvider.getLocks(), dataverseName,
                            datasetFullyQualifiedName);
                    if (actionListener.isActive()) {
                        throw new RuntimeDataException(
                                ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY,
                                datasetFullyQualifiedName + ".index", actionListener.getEntityId(),
                                actionListener.getState());
                    }
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action query(Dataset dataset, Semaphore semaphore) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                String dataverseName = dataset.getDataverseName();
                String datasetFullyQualifiedName = dataverseName + '.' + dataset.getDatasetName();
                try {
                    lockManager.acquireDataverseReadLock(mdProvider.getLocks(), dataverseName);
                    lockManager.acquireDatasetReadLock(mdProvider.getLocks(), datasetFullyQualifiedName);
                    if (!semaphore.tryAcquire()) {
                        semaphore.acquire();
                    }
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }

    public Action suspendAllActivities(ActiveNotificationHandler handler) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                handler.suspend(mdProvider);
            }
        };
        add(action);
        return action;
    }

    public Action resumeAllActivities(ActiveNotificationHandler handler) {
        Action action = new Action() {
            @Override
            protected void doExecute(MetadataProvider mdProvider) throws Exception {
                try {
                    handler.resume(mdProvider);
                } finally {
                    mdProvider.getLocks().reset();
                }
            }
        };
        add(action);
        return action;
    }
}
