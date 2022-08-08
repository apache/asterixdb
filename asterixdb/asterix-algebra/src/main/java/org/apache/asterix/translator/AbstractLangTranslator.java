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

import static org.apache.asterix.common.utils.IdentifierUtil.dataset;
import static org.apache.asterix.common.utils.IdentifierUtil.dataverse;

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
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.AnalyzeDropStatement;
import org.apache.asterix.lang.common.statement.AnalyzeStatement;
import org.apache.asterix.lang.common.statement.CreateAdapterStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateLibraryStatement;
import org.apache.asterix.lang.common.statement.CreateSynonymStatement;
import org.apache.asterix.lang.common.statement.CreateViewStatement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDropStatement;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.DropDatasetStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.IndexDropStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.common.statement.TypeDecl;
import org.apache.asterix.lang.common.statement.TypeDropStatement;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.metadata.dataset.hints.DatasetHints;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.commons.lang3.StringUtils;
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

    protected static final String INVALID_OPERATION_MESSAGE = "Invalid operation - %s";

    protected static final String BAD_DATAVERSE_DML_MESSAGE = "%s operation is not permitted in " + dataverse() + " %s";

    protected static final String BAD_DATAVERSE_DDL_MESSAGE = "Cannot %s " + dataverse() + ": %s";

    protected static final String BAD_DATAVERSE_OBJECT_DDL_MESSAGE =
            "Cannot %s a %s belonging to the " + dataverse() + ": %s";

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
        DataverseName dataverseName = defaultDataverse != null ? defaultDataverse.getDataverseName() : null;
        switch (stmt.getKind()) {
            case LOAD:
                LoadStatement loadStmt = (LoadStatement) stmt;
                if (loadStmt.getDataverseName() != null) {
                    dataverseName = loadStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_DML_MESSAGE, "Load", dataverseName);
                }
                break;

            case INSERT:
                InsertStatement insertStmt = (InsertStatement) stmt;
                if (insertStmt.getDataverseName() != null) {
                    dataverseName = insertStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_DML_MESSAGE, "Insert", dataverseName);
                }
                break;

            case UPSERT:
                UpsertStatement upsertStmt = (UpsertStatement) stmt;
                if (upsertStmt.getDataverseName() != null) {
                    dataverseName = upsertStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_DML_MESSAGE, "Upsert", dataverseName);
                }
                break;

            case DELETE:
                DeleteStatement deleteStmt = (DeleteStatement) stmt;
                if (deleteStmt.getDataverseName() != null) {
                    dataverseName = deleteStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_DML_MESSAGE, "Delete", dataverseName);
                }
                break;

            case CREATE_DATAVERSE:
                CreateDataverseStatement dvCreateStmt = (CreateDataverseStatement) stmt;
                dataverseName = dvCreateStmt.getDataverseName();
                invalidOperation = FunctionConstants.ASTERIX_DV.equals(dataverseName)
                        || FunctionConstants.ALGEBRICKS_DV.equals(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_DDL_MESSAGE, "create", dataverseName);
                }
                break;

            case DATAVERSE_DROP:
                DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                dataverseName = dvDropStmt.getDataverseName();
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_DDL_MESSAGE, "drop", dataverseName);
                }
                break;

            case DATASET_DECL:
                DatasetDecl dsCreateStmt = (DatasetDecl) stmt;
                if (dsCreateStmt.getDataverse() != null) {
                    dataverseName = dsCreateStmt.getDataverse();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", dataset(), dataverseName);
                }

                if (!invalidOperation) {
                    Map<String, String> hints = dsCreateStmt.getHints();
                    if (hints != null && !hints.isEmpty()) {
                        StringBuilder errorMsgBuffer = new StringBuilder();
                        for (Entry<String, String> hint : hints.entrySet()) {
                            Pair<Boolean, String> validationResult =
                                    DatasetHints.validate(appCtx, hint.getKey(), hint.getValue());
                            if (!validationResult.first) {
                                errorMsgBuffer.append(StringUtils.capitalize(dataset())).append(": ")
                                        .append(dsCreateStmt.getName().getValue()).append(" error in processing hint: ")
                                        .append(hint.getKey()).append(" ").append(validationResult.second);
                                errorMsgBuffer.append(" \n");
                            }
                        }
                        invalidOperation = errorMsgBuffer.length() > 0;
                        if (invalidOperation) {
                            message = errorMsgBuffer.toString();
                        }
                    }
                }
                break;

            case DATASET_DROP:
                DropDatasetStatement dsDropStmt = (DropDatasetStatement) stmt;
                if (dsDropStmt.getDataverseName() != null) {
                    dataverseName = dsDropStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "drop", dataset(), dataverseName);
                }
                break;

            case INDEX_DROP:
                IndexDropStatement idxDropStmt = (IndexDropStatement) stmt;
                if (idxDropStmt.getDataverseName() != null) {
                    dataverseName = idxDropStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "drop", "index", dataverseName);
                }
                break;

            case TYPE_DECL:
                TypeDecl typeCreateStmt = (TypeDecl) stmt;
                if (typeCreateStmt.getDataverseName() != null) {
                    dataverseName = typeCreateStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "type", dataverseName);
                }
                break;

            case TYPE_DROP:
                TypeDropStatement typeDropStmt = (TypeDropStatement) stmt;
                if (typeDropStmt.getDataverseName() != null) {
                    dataverseName = typeDropStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "drop", "type", dataverseName);
                }
                break;

            case CREATE_SYNONYM:
                CreateSynonymStatement synCreateStmt = (CreateSynonymStatement) stmt;
                if (synCreateStmt.getDataverseName() != null) {
                    dataverseName = synCreateStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "synonym", dataverseName);
                }
                break;

            case FUNCTION_DECL:
                FunctionDecl fnDeclStmt = (FunctionDecl) stmt;
                FunctionSignature fnDeclSignature = fnDeclStmt.getSignature();
                if (fnDeclSignature.getDataverseName() != null) {
                    dataverseName = fnDeclSignature.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "declare", "function", dataverseName);
                }
                break;

            case CREATE_FUNCTION:
                CreateFunctionStatement fnCreateStmt = (CreateFunctionStatement) stmt;
                FunctionSignature fnCreateSignature = fnCreateStmt.getFunctionSignature();
                if (fnCreateSignature.getDataverseName() != null) {
                    dataverseName = fnCreateSignature.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "function", dataverseName);
                }
                break;

            case CREATE_LIBRARY:
                CreateLibraryStatement libCreateStmt = (CreateLibraryStatement) stmt;
                if (libCreateStmt.getDataverseName() != null) {
                    dataverseName = libCreateStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "library", dataverseName);
                }
                break;

            case CREATE_ADAPTER:
                CreateAdapterStatement adCreateStmt = (CreateAdapterStatement) stmt;
                if (adCreateStmt.getDataverseName() != null) {
                    dataverseName = adCreateStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "adapter", dataverseName);
                }
                break;

            case CREATE_VIEW:
                CreateViewStatement viewCreateStmt = (CreateViewStatement) stmt;
                if (viewCreateStmt.getDataverseName() != null) {
                    dataverseName = viewCreateStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "view", dataverseName);
                }
                break;

            case CREATE_FEED:
                CreateFeedStatement feedCreateStmt = (CreateFeedStatement) stmt;
                if (feedCreateStmt.getDataverseName() != null) {
                    dataverseName = feedCreateStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "feed", dataverseName);
                }
                break;

            case CREATE_FEED_POLICY:
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "create", "ingestion policy",
                            dataverseName);
                }
                break;

            case ANALYZE:
                AnalyzeStatement analyzeStmt = (AnalyzeStatement) stmt;
                if (analyzeStmt.getDataverseName() != null) {
                    dataverseName = analyzeStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "analyze", dataset(), dataverseName);
                }
                break;
            case ANALYZE_DROP:
                AnalyzeDropStatement analyzeDropStmt = (AnalyzeDropStatement) stmt;
                if (analyzeDropStmt.getDataverseName() != null) {
                    dataverseName = analyzeDropStmt.getDataverseName();
                }
                invalidOperation = isMetadataDataverse(dataverseName);
                if (invalidOperation) {
                    message = String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, "analyze drop", dataset(), dataverseName);
                }
                break;
        }

        if (invalidOperation) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, stmt.getSourceLocation(),
                    String.format(INVALID_OPERATION_MESSAGE, message));
        }
    }

    protected static boolean isMetadataDataverse(DataverseName dataverseName) {
        return MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverseName);
    }
}
