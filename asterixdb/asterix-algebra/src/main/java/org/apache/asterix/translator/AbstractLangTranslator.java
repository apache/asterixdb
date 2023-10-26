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
import static org.apache.hyracks.api.exceptions.ErrorCode.TIMEOUT;

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
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.AnalyzeDropStatement;
import org.apache.asterix.lang.common.statement.AnalyzeStatement;
import org.apache.asterix.lang.common.statement.CreateAdapterStatement;
import org.apache.asterix.lang.common.statement.CreateDatabaseStatement;
import org.apache.asterix.lang.common.statement.CreateDataverseStatement;
import org.apache.asterix.lang.common.statement.CreateFeedStatement;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.CreateLibraryStatement;
import org.apache.asterix.lang.common.statement.CreateSynonymStatement;
import org.apache.asterix.lang.common.statement.CreateViewStatement;
import org.apache.asterix.lang.common.statement.DatabaseDropStatement;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
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

    protected static final String BAD_DATAVERSE_DML_MSG_DB =
            "%s operation is not permitted in " + dataverse() + " %s in database %s";

    protected static final String BAD_DATAVERSE_DDL_MESSAGE = "Cannot %s " + dataverse() + ": %s";

    protected static final String BAD_DATAVERSE_DDL_MSG_DB = "Cannot %s " + dataverse() + ": %s in database: %s";

    protected static final String BAD_DATAVERSE_OBJECT_DDL_MESSAGE =
            "Cannot %s a %s belonging to the " + dataverse() + ": %s";

    protected static final String BAD_DATAVERSE_OBJECT_DDL_MSG_DB =
            "Cannot %s a %s belonging to the " + dataverse() + ": %s in database: %s";

    public void validateOperation(ICcApplicationContext appCtx, Namespace activeNamespace, Statement stmt)
            throws AlgebricksException {

        final IClusterStateManager clusterStateManager = appCtx.getClusterStateManager();
        final IGlobalRecoveryManager globalRecoveryManager = appCtx.getGlobalRecoveryManager();
        if (!(clusterStateManager.getState().equals(ClusterState.ACTIVE)
                && globalRecoveryManager.isRecoveryCompleted())) {
            int maxWaitCycles = appCtx.getExternalProperties().getMaxWaitClusterActive();
            try {
                clusterStateManager.waitForState(ClusterState.ACTIVE, maxWaitCycles, TimeUnit.SECONDS);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e, TIMEOUT);
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

        boolean usingDb = appCtx.getNamespaceResolver().isUsingDatabase();
        boolean invalidOperation = false;
        String message = null;
        DataverseName dataverseName;
        Namespace namespace;
        switch (stmt.getKind()) {
            case LOAD:
                namespace = getStatementNamespace(((LoadStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatDmlMessage("Load", namespace, usingDb);
                }
                break;

            case INSERT:
                namespace = getStatementNamespace(((InsertStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatDmlMessage("Insert", namespace, usingDb);
                }
                break;

            case UPSERT:
                namespace = getStatementNamespace(((UpsertStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatDmlMessage("Upsert", namespace, usingDb);
                }
                break;

            case DELETE:
                namespace = getStatementNamespace(((DeleteStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatDmlMessage("Delete", namespace, usingDb);
                }
                break;

            case CREATE_DATABASE: {
                CreateDatabaseStatement dbCreateStmt = (CreateDatabaseStatement) stmt;
                String dbName = dbCreateStmt.getDatabaseName().getValue();
                invalidOperation = isSystemDatabase(dbName) || isDefaultDatabase(dbName) || isReservedDatabase(dbName);
                if (invalidOperation) {
                    message = String.format("Cannot create database: %s", dbName);
                }
                break;
            }

            case DATABASE_DROP: {
                DatabaseDropStatement dbDropStmt = (DatabaseDropStatement) stmt;
                String dbName = dbDropStmt.getDatabaseName().getValue();
                invalidOperation = isSystemDatabase(dbName) || isDefaultDatabase(dbName);
                if (invalidOperation) {
                    message = String.format("Cannot drop database: %s", dbName);
                }
                break;
            }

            case CREATE_DATAVERSE:
                CreateDataverseStatement dvCreateStmt = (CreateDataverseStatement) stmt;
                dataverseName = dvCreateStmt.getDataverseName();
                invalidOperation = FunctionConstants.ASTERIX_DV.equals(dataverseName)
                        || FunctionConstants.ALGEBRICKS_DV.equals(dataverseName) || isMetadataDataverse(dataverseName)
                        || isDefaultDataverse(dataverseName) || isSystemDatabase(dvCreateStmt.getDatabaseName());
                if (invalidOperation) {
                    message = formatDdlMessage("create", dataverseName, dvCreateStmt.getDatabaseName(), usingDb);
                }
                break;

            case DATAVERSE_DROP:
                DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                dataverseName = dvDropStmt.getDataverseName();
                invalidOperation = isMetadataDataverse(dataverseName) || isDefaultDataverse(dataverseName)
                        || isSystemDatabase(dvDropStmt.getDatabaseName());
                if (invalidOperation) {
                    message = formatDdlMessage("drop", dataverseName, dvDropStmt.getDatabaseName(), usingDb);
                }
                break;

            case DATASET_DECL:
                DatasetDecl dsCreateStmt = (DatasetDecl) stmt;
                namespace = getStatementNamespace(((DatasetDecl) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", dataset(), namespace, usingDb);
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
                namespace = getStatementNamespace(((DropDatasetStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("drop", dataset(), namespace, usingDb);
                }
                break;

            case INDEX_DROP:
                namespace = getStatementNamespace(((IndexDropStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("drop", "index", namespace, usingDb);
                }
                break;

            case TYPE_DECL:
                namespace = getStatementNamespace(((TypeDecl) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "type", namespace, usingDb);
                }
                break;

            case TYPE_DROP:
                namespace = getStatementNamespace(((TypeDropStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("drop", "type", namespace, usingDb);
                }
                break;

            case CREATE_SYNONYM:
                namespace = getStatementNamespace(((CreateSynonymStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "synonym", namespace, usingDb);
                }
                break;

            case FUNCTION_DECL:
                //TODO(DB): change to use namespace like others
                FunctionDecl fnDeclStmt = (FunctionDecl) stmt;
                FunctionSignature fnDeclSignature = fnDeclStmt.getSignature();
                if (fnDeclSignature.getDataverseName() != null) {
                    namespace = new Namespace(fnDeclSignature.getDatabaseName(), fnDeclSignature.getDataverseName());
                } else {
                    namespace = activeNamespace;
                }
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("declare", "function", namespace, usingDb);
                }
                break;

            case CREATE_FUNCTION:
                //TODO(DB): check it's not System database for all cases
                CreateFunctionStatement fnCreateStmt = (CreateFunctionStatement) stmt;
                FunctionSignature fnCreateSignature = fnCreateStmt.getFunctionSignature();
                if (fnCreateSignature.getDataverseName() != null) {
                    namespace =
                            new Namespace(fnCreateSignature.getDatabaseName(), fnCreateSignature.getDataverseName());
                } else {
                    namespace = activeNamespace;
                }
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "function", namespace, usingDb);
                }
                break;

            case CREATE_LIBRARY:
                namespace = getStatementNamespace(((CreateLibraryStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "library", namespace, usingDb);
                }
                break;

            case CREATE_ADAPTER:
                namespace = getStatementNamespace(((CreateAdapterStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "adapter", namespace, usingDb);
                }
                break;

            case CREATE_VIEW:
                namespace = getStatementNamespace(((CreateViewStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "view", namespace, usingDb);
                }
                break;

            case CREATE_FEED:
                namespace = getStatementNamespace(((CreateFeedStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "feed", namespace, usingDb);
                }
                break;

            case CREATE_FEED_POLICY:
                invalidOperation = isSystemNamespace(activeNamespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("create", "ingestion policy", activeNamespace, usingDb);
                }
                break;

            case ANALYZE:
                namespace = getStatementNamespace(((AnalyzeStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("analyze", dataset(), namespace, usingDb);
                }
                break;
            case ANALYZE_DROP:
                namespace = getStatementNamespace(((AnalyzeDropStatement) stmt).getNamespace(), activeNamespace);
                invalidOperation = isSystemNamespace(namespace);
                if (invalidOperation) {
                    message = formatObjectDdlMessage("analyze drop", dataset(), namespace, usingDb);
                }
                break;
        }

        if (invalidOperation) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, stmt.getSourceLocation(),
                    String.format(INVALID_OPERATION_MESSAGE, message));
        }
    }

    private static String formatDmlMessage(String operation, Namespace ns, boolean usingDb) {
        DataverseName dv = ns.getDataverseName();
        return usingDb ? String.format(BAD_DATAVERSE_DML_MSG_DB, operation, dv, ns.getDatabaseName())
                : String.format(BAD_DATAVERSE_DML_MESSAGE, operation, dv);
    }

    private static String formatDdlMessage(String operation, DataverseName dv, String db, boolean usingDb) {
        return usingDb ? String.format(BAD_DATAVERSE_DDL_MSG_DB, operation, dv, db)
                : String.format(BAD_DATAVERSE_DDL_MESSAGE, operation, dv);
    }

    protected static String formatObjectDdlMessage(String operation, String object, Namespace ns, boolean usingDb) {
        DataverseName dv = ns.getDataverseName();
        return usingDb ? String.format(BAD_DATAVERSE_OBJECT_DDL_MSG_DB, operation, object, dv, ns.getDatabaseName())
                : String.format(BAD_DATAVERSE_OBJECT_DDL_MESSAGE, operation, object, dv);
    }

    protected static Namespace getStatementNamespace(Namespace namespace, Namespace activeNamespace) {
        return namespace != null ? namespace : activeNamespace;
    }

    protected static boolean isSystemNamespace(Namespace ns) {
        return ns != null && (isSystemDatabase(ns.getDatabaseName()) || isMetadataDataverse(ns.getDataverseName()));
    }

    protected static boolean isSystemDatabase(String databaseName) {
        return MetadataConstants.SYSTEM_DATABASE.equals(databaseName);
    }

    protected static boolean isDefaultDatabase(String databaseName) {
        return MetadataConstants.DEFAULT_DATABASE.equals(databaseName);
    }

    protected static boolean isMetadataDataverse(DataverseName dataverseName) {
        return MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverseName);
    }

    protected static boolean isDefaultDataverse(DataverseName dataverseName) {
        return MetadataConstants.DEFAULT_DATAVERSE_NAME.equals(dataverseName);
    }

    protected static boolean isReservedDatabase(String databaseName) {
        return FunctionConstants.ASTERIX_DB.equals(databaseName)
                || AlgebricksBuiltinFunctions.ALGEBRICKS_DB.equals(databaseName)
                || MetadataConstants.METADATA_DATAVERSE_NAME.getCanonicalForm().equals(databaseName)
                || databaseName.startsWith(StorageConstants.PARTITION_DIR_PREFIX);
    }
}
