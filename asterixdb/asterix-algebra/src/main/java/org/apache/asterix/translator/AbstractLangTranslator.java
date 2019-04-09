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
package org.apache.asterix.translator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.metadata.dataset.hints.DatasetHints;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for language translators. Contains the common validation logic for language
 * statements.
 */
public abstract class AbstractLangTranslator {

    private static final Logger LOGGER = LogManager.getLogger();

    public void validateOperation(ICcApplicationContext appCtx, Dataverse defaultDataverse, Statement stmt)
            throws AlgebricksException {

        final IClusterStateManager clusterStateManager = appCtx.getClusterStateManager();
        final IGlobalRecoveryManager globalRecoveryManager = appCtx.getGlobalRecoveryManager();
        if (!(clusterStateManager.getState().equals(ClusterState.ACTIVE)
                && globalRecoveryManager.isRecoveryCompleted())) {
            int maxWaitCycles = appCtx.getExternalProperties().getMaxWaitClusterActive();
            try {
                clusterStateManager.waitForState(ClusterState.ACTIVE, maxWaitCycles, TimeUnit.SECONDS);
            } catch (HyracksDataException e) {
                throw new AsterixException(e);
            } catch (InterruptedException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Thread interrupted while waiting for cluster to be " + ClusterState.ACTIVE);
                }
                Thread.currentThread().interrupt();
            }
            synchronized (clusterStateManager) {
                if (!clusterStateManager.getState().equals(ClusterState.ACTIVE)) {
                    ClusterPartition[] configuredPartitions = clusterStateManager.getClusterPartitons();
                    Set<String> inactiveNodes = new HashSet<>();
                    for (ClusterPartition cp : configuredPartitions) {
                        if (!cp.isActive()) {
                            inactiveNodes.add(cp.getNodeId());
                        }
                    }
                    throw AsterixException.create(ErrorCode.CLUSTER_STATE_UNUSABLE,
                            Arrays.toString(inactiveNodes.toArray()));
                } else {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Cluster is now " + ClusterState.ACTIVE);
                    }
                }
            }
        }

        if (!globalRecoveryManager.isRecoveryCompleted()) {
            int maxWaitCycles = appCtx.getExternalProperties().getMaxWaitClusterActive();
            int waitCycleCount = 0;
            try {
                while (!globalRecoveryManager.isRecoveryCompleted() && waitCycleCount < maxWaitCycles) {
                    Thread.sleep(1000);
                    waitCycleCount++;
                }
            } catch (InterruptedException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Thread interrupted while waiting for cluster to complete global recovery ");
                }
                Thread.currentThread().interrupt();
            }
            if (!globalRecoveryManager.isRecoveryCompleted()) {
                throw new AsterixException("Cluster Global recovery is not yet complete and the system is in "
                        + ClusterState.ACTIVE + " state");
            }
        }

        boolean invalidOperation = false;
        String message = null;
        String dataverse = defaultDataverse != null ? defaultDataverse.getDataverseName() : null;
        switch (stmt.getKind()) {
            case INSERT:
                InsertStatement insertStmt = (InsertStatement) stmt;
                if (insertStmt.getDataverseName() != null) {
                    dataverse = insertStmt.getDataverseName().getValue();
                }
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse);
                if (invalidOperation) {
                    message = "Insert operation is not permitted in dataverse "
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                }
                break;

            case DELETE:
                DeleteStatement deleteStmt = (DeleteStatement) stmt;
                if (deleteStmt.getDataverseName() != null) {
                    dataverse = deleteStmt.getDataverseName().getValue();
                }
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse);
                if (invalidOperation) {
                    message = "Delete operation is not permitted in dataverse "
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                }
                break;

            case DATAVERSE_DROP:
                DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                invalidOperation =
                        MetadataConstants.METADATA_DATAVERSE_NAME.equals(dvDropStmt.getDataverseName().getValue());
                if (invalidOperation) {
                    message = "Cannot drop dataverse:" + dvDropStmt.getDataverseName().getValue();
                }
                break;

            case DATASET_DROP:
                DropDatasetStatement dropStmt = (DropDatasetStatement) stmt;
                if (dropStmt.getDataverseName() != null) {
                    dataverse = dropStmt.getDataverseName().getValue();
                }
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverse);
                if (invalidOperation) {
                    message = "Cannot drop a dataset belonging to the dataverse:"
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                }
                break;
            case DATASET_DECL:
                DatasetDecl datasetStmt = (DatasetDecl) stmt;
                Map<String, String> hints = datasetStmt.getHints();
                if (hints != null && !hints.isEmpty()) {
                    StringBuilder errorMsgBuffer = new StringBuilder();
                    for (Entry<String, String> hint : hints.entrySet()) {
                        Pair<Boolean, String> validationResult =
                                DatasetHints.validate(appCtx, hint.getKey(), hint.getValue());
                        if (!validationResult.first) {
                            errorMsgBuffer.append("Dataset: ").append(datasetStmt.getName().getValue())
                                    .append(" error in processing hint: ").append(hint.getKey()).append(" ")
                                    .append(validationResult.second);
                            errorMsgBuffer.append(" \n");
                        }
                    }
                    invalidOperation = errorMsgBuffer.length() > 0;
                    if (invalidOperation) {
                        message = errorMsgBuffer.toString();
                    }
                }
                break;
            default:
                break;
        }

        if (invalidOperation) {
            throw new AsterixException("Invalid operation - " + message);
        }
    }
}
