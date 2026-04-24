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
package org.apache.asterix.app.translator.handlers;

import static org.apache.asterix.app.translator.QueryTranslator.abort;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.crs.CRSCreateStatement;
import org.apache.asterix.lang.common.statement.crs.CRSDropStatement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.CoordinateReferenceSystem;
import org.apache.asterix.translator.SessionConfig;
import org.apache.sis.referencing.CRS;
import org.opengis.util.FactoryException;

public class CRSStatementHandler {

    private final Statement.Kind kind;
    private final MetadataProvider metadataProvider;
    private final Statement statement;
    private final SessionConfig sessionConfig;
    private final IMetadataLockUtil lockUtil;
    private final IMetadataLockManager lockManager;
    private final Namespace activeNamespace;

    public CRSStatementHandler(Statement.Kind kind, MetadataProvider metadataProvider, Statement statement,
            SessionConfig sessionConfig, IMetadataLockUtil lockUtil, IMetadataLockManager lockManager,
            Namespace activeNamespace) {
        this.kind = kind;
        this.metadataProvider = metadataProvider;
        this.statement = statement;
        this.sessionConfig = sessionConfig;
        this.lockUtil = lockUtil;
        this.lockManager = lockManager;
        this.activeNamespace = activeNamespace;
    }

    public void handle() throws Exception {
        switch (kind) {
            case CRS_CREATE:
                handleCreate();
                return;
            case CRS_DROP:
                handleDrop();
                return;
            default:
                throw new IllegalStateException("CRS statement handler handling non-CRS statement: " + kind);
        }
    }

    private void handleCreate() throws Exception {
        if (isCompileOnly()) {
            return;
        }
        CRSCreateStatement stmt = (CRSCreateStatement) statement;
        String databaseName = activeNamespace.getDatabaseName();
        DataverseName dataverseName = activeNamespace.getDataverseName();
        lockUtil.createCRSBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, stmt.getSrid());
        try {
            doHandleCreate(stmt, databaseName, dataverseName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void doHandleCreate(CRSCreateStatement stmt, String databaseName, DataverseName dataverseName)
            throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            if (MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName) == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, stmt.getSourceLocation(),
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            CoordinateReferenceSystem existing =
                    MetadataManager.INSTANCE.getCRS(mdTxnCtx, databaseName, dataverseName, stmt.getSrid());
            if (existing != null) {
                if (stmt.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.CRS_ALREADY_EXISTS, stmt.getSourceLocation(),
                            stmt.getSrid());
                }
            }
            try {
                CRS.fromWKT(stmt.getCrsWkt());
            } catch (FactoryException e) {
                throw new CompilationException(ErrorCode.INVALID_CRS_WKT, stmt.getSourceLocation(), stmt.getSrid(),
                        e.getMessage());
            }
            CoordinateReferenceSystem crs = new CoordinateReferenceSystem(databaseName, dataverseName, stmt.getSrid(),
                    stmt.getCrsName(), stmt.getCrsWkt());
            MetadataManager.INSTANCE.addCRS(mdTxnCtx, crs);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    private void handleDrop() throws Exception {
        if (isCompileOnly()) {
            return;
        }
        CRSDropStatement stmt = (CRSDropStatement) statement;
        String databaseName = activeNamespace.getDatabaseName();
        DataverseName dataverseName = activeNamespace.getDataverseName();
        lockUtil.dropCRSBegin(lockManager, metadataProvider.getLocks(), databaseName, dataverseName, stmt.getSrid());
        try {
            doHandleDrop(stmt, databaseName, dataverseName);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private void doHandleDrop(CRSDropStatement stmt, String databaseName, DataverseName dataverseName)
            throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            if (MetadataManager.INSTANCE.getDataverse(mdTxnCtx, databaseName, dataverseName) == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, stmt.getSourceLocation(),
                        MetadataUtil.dataverseName(databaseName, dataverseName, metadataProvider.isUsingDatabase()));
            }
            CoordinateReferenceSystem existing =
                    MetadataManager.INSTANCE.getCRS(mdTxnCtx, databaseName, dataverseName, stmt.getSrid());
            if (existing == null) {
                if (stmt.getIfExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return;
                } else {
                    throw new CompilationException(ErrorCode.CRS_NOT_FOUND, stmt.getSourceLocation(), stmt.getSrid());
                }
            }
            MetadataManager.INSTANCE.dropCRS(mdTxnCtx, databaseName, dataverseName, stmt.getSrid());
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    private boolean isCompileOnly() {
        return !sessionConfig.isExecuteQuery();
    }
}
