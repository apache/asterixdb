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

import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.DatasetDecl;
import org.apache.asterix.aql.expression.DataverseDropStatement;
import org.apache.asterix.aql.expression.DeleteStatement;
import org.apache.asterix.aql.expression.DropStatement;
import org.apache.asterix.aql.expression.InsertStatement;
import org.apache.asterix.aql.expression.NodeGroupDropStatement;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.bootstrap.MetadataConstants;
import org.apache.asterix.metadata.dataset.hints.DatasetHints;
import org.apache.asterix.metadata.entities.AsterixBuiltinTypeMap;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Base class for AQL translators. Contains the common validation logic for AQL
 * statements.
 */
public abstract class AbstractAqlTranslator {

    protected static final Logger LOGGER = Logger.getLogger(AbstractAqlTranslator.class.getName());

    protected static final Map<String, BuiltinType> builtinTypeMap = AsterixBuiltinTypeMap.getBuiltinTypes();

    public void validateOperation(Dataverse defaultDataverse, Statement stmt) throws AsterixException {

        if (!(AsterixClusterProperties.INSTANCE.getState().equals(ClusterState.ACTIVE) && AsterixClusterProperties.INSTANCE
                .isGlobalRecoveryCompleted())) {
            int maxWaitCycles = AsterixAppContextInfo.getInstance().getExternalProperties().getMaxWaitClusterActive();
            int waitCycleCount = 0;
            try {
                while (!AsterixClusterProperties.INSTANCE.getState().equals(ClusterState.ACTIVE)
                        && waitCycleCount < maxWaitCycles) {
                    Thread.sleep(1000);
                    waitCycleCount++;
                }
            } catch (InterruptedException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Thread interrupted while waiting for cluster to be "
                            + ClusterState.ACTIVE);
                }
            }
            if (!AsterixClusterProperties.INSTANCE.getState().equals(ClusterState.ACTIVE)) {
                throw new AsterixException(" Asterix Cluster is in " + ClusterState.UNUSABLE
                        + " state." + "\n One or more Node Controllers have left or haven't joined yet.\n");
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Cluster is now " + ClusterState.ACTIVE);
                }
            }
        }

        if (AsterixClusterProperties.INSTANCE.getState().equals(ClusterState.UNUSABLE)) {
            throw new AsterixException(" Asterix Cluster is in " + ClusterState.UNUSABLE + " state."
                    + "\n One or more Node Controllers have left.\n");
        }

        if (!AsterixClusterProperties.INSTANCE.isGlobalRecoveryCompleted()) {
            int maxWaitCycles = AsterixAppContextInfo.getInstance().getExternalProperties().getMaxWaitClusterActive();
            int waitCycleCount = 0;
            try {
                while (!AsterixClusterProperties.INSTANCE.isGlobalRecoveryCompleted() && waitCycleCount < maxWaitCycles) {
                    Thread.sleep(1000);
                    waitCycleCount++;
                }
            } catch (InterruptedException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Thread interrupted while waiting for cluster to complete global recovery ");
                }
            }
            if (!AsterixClusterProperties.INSTANCE.isGlobalRecoveryCompleted()) {
                throw new AsterixException(" Asterix Cluster Global recovery is not yet complete and The system is in "
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

            case NODEGROUP_DROP:
                String nodegroupName = ((NodeGroupDropStatement) stmt).getNodeGroupName().getValue();
                invalidOperation = MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME.equals(nodegroupName);
                if (invalidOperation) {
                    message = "Cannot drop nodegroup:" + nodegroupName;
                }
                break;

            case DATAVERSE_DROP:
                DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                invalidOperation = MetadataConstants.METADATA_DATAVERSE_NAME.equals(dvDropStmt.getDataverseName()
                        .getValue());
                if (invalidOperation) {
                    message = "Cannot drop dataverse:" + dvDropStmt.getDataverseName().getValue();
                }
                break;

            case DATASET_DROP:
                DropStatement dropStmt = (DropStatement) stmt;
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
                    Pair<Boolean, String> validationResult = null;
                    StringBuffer errorMsgBuffer = new StringBuffer();
                    for (Entry<String, String> hint : hints.entrySet()) {
                        validationResult = DatasetHints.validate(hint.getKey(), hint.getValue());
                        if (!validationResult.first) {
                            errorMsgBuffer.append("Dataset: " + datasetStmt.getName().getValue()
                                    + " error in processing hint: " + hint.getKey() + " " + validationResult.second);
                            errorMsgBuffer.append(" \n");
                        }
                    }
                    invalidOperation = errorMsgBuffer.length() > 0;
                    if (invalidOperation) {
                        message = errorMsgBuffer.toString();
                    }
                }
                break;

        }

        if (invalidOperation) {
            throw new AsterixException("Invalid operation - " + message);
        }
    }
}
