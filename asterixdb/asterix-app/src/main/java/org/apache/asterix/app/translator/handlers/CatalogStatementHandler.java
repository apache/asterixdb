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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.CatalogConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.IMetadataLockUtil;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.catalog.CatalogCreateStatement;
import org.apache.asterix.lang.common.statement.catalog.CatalogDropStatement;
import org.apache.asterix.lang.common.statement.catalog.CatalogStatement;
import org.apache.asterix.lang.common.statement.catalog.IcebergCatalogCreateStatement;
import org.apache.asterix.lang.common.statement.catalog.IcebergCatalogDetailsDecl;
import org.apache.asterix.lang.common.util.LangRecordParseUtil;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Catalog;
import org.apache.asterix.metadata.entities.EntityDetails;
import org.apache.asterix.metadata.entities.IcebergCatalog;
import org.apache.asterix.metadata.entities.IcebergCatalogDetails;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class CatalogStatementHandler {

    private final Statement.Kind kind;
    private final MetadataProvider metadataProvider;
    private final CatalogStatement statement;
    private final Creator creator;
    private final SessionConfig sessionConfig;
    private final IMetadataLockUtil lockUtil;
    private final IMetadataLockManager lockManager;

    public CatalogStatementHandler(Statement.Kind kind, MetadataProvider metadataProvider, Statement statement,
            Creator creator, SessionConfig sessionConfig, IMetadataLockUtil lockUtil,
            IMetadataLockManager lockManager) {
        this.kind = kind;
        this.metadataProvider = metadataProvider;
        this.statement = (CatalogStatement) statement;
        this.creator = creator;
        this.sessionConfig = sessionConfig;
        this.lockUtil = lockUtil;
        this.lockManager = lockManager;
    }

    public void handle() throws Exception {
        switch (kind) {
            case CATALOG_CREATE:
                handleCreate();
                return;
            case CATALOG_DROP:
                handleDrop();
                return;
            default:
                throw new IllegalStateException("Catalog statement handler handling non-catalog statement: " + kind);
        }
    }

    private void handleCreate() throws Exception {
        if (isCompileOnly()) {
            return;
        }

        String name = statement.getCatalogName();
        lockUtil.createCatalogBegin(lockManager, metadataProvider.getLocks(), name);
        try {
            doHandleCreate((CatalogCreateStatement) statement);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private boolean doHandleCreate(CatalogCreateStatement statement) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            String catalogName = statement.getCatalogName();
            Catalog catalog = MetadataManager.INSTANCE.getCatalog(mdTxnCtx, catalogName);
            if (catalog != null) {
                if (statement.getIfNotExists()) {
                    MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                    return false;
                } else {
                    throw new CompilationException(ErrorCode.CATALOG_EXISTS, statement.getSourceLocation(),
                            catalogName);
                }
            }

            validateCatalogType(statement.getCatalogType());
            validateCatalogProperties(statement, mdTxnCtx, metadataProvider);

            MetadataManager.INSTANCE.addCatalog(mdTxnCtx, getCatalog(statement));
            beforeAddTxnCommit(metadataProvider, creator, EntityDetails.newCatalog(catalogName));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    private void handleDrop() throws Exception {
        CatalogDropStatement dropStatement = (CatalogDropStatement) statement;
        String catalogName = dropStatement.getCatalogName();

        if (isCompileOnly()) {
            return;
        }
        lockUtil.dropCatalogBegin(lockManager, metadataProvider.getLocks(), catalogName);
        try {
            doDropCatalog(dropStatement);
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    private boolean doDropCatalog(CatalogDropStatement statement) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            String catalogName = statement.getCatalogName();
            MetadataManager.INSTANCE.dropCatalog(mdTxnCtx, catalogName);
            beforeDropTxnCommit(metadataProvider, creator, EntityDetails.newCatalog(catalogName));
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return true;
        } catch (Exception e) {
            abort(e, e, mdTxnCtx);
            throw e;
        }
    }

    private boolean isCompileOnly() {
        return !sessionConfig.isExecuteQuery();
    }

    protected void beforeAddTxnCommit(MetadataProvider metadataProvider, Creator creator, EntityDetails entityDetails)
            throws AlgebricksException {
        //no op
    }

    protected void beforeDropTxnCommit(MetadataProvider metadataProvider, Creator creator, EntityDetails entityDetails)
            throws AlgebricksException {
        //no op
    }

    protected void validateCatalogType(String catalogType) throws CompilationException {
        for (CatalogConfig.CatalogType type : CatalogConfig.CatalogType.values()) {
            if (type.name().equalsIgnoreCase(catalogType)) {
                return;
            }
        }
        throw new CompilationException(ErrorCode.UNSUPPORTED_CATALOG_TYPE, catalogType);
    }

    /**
     * Returns the proper catalog based on the type
     *
     * @param statement statement
     * @return the proper catalog depending on the catalog type
     */
    private Catalog getCatalog(CatalogCreateStatement statement) throws AlgebricksException {
        if (IcebergUtils.isIcebergCatalog(statement.getCatalogType())) {
            return createIcebergCatalog(statement);
        }
        throw new CompilationException(ErrorCode.UNSUPPORTED_CATALOG_TYPE, statement.getCatalogType());
    }

    /**
     * Creates an Iceberg catalog from the create statement
     *
     * @param statement statement
     * @return iceberg catalog
     * @throws AlgebricksException AlgebricksException
     */
    protected IcebergCatalog createIcebergCatalog(CatalogCreateStatement statement) throws AlgebricksException {
        IcebergCatalogCreateStatement icebergStatement = (IcebergCatalogCreateStatement) statement;
        IcebergCatalogDetailsDecl detailsDecl = icebergStatement.getCatalogDetailsDecl();
        Map<String, String> properties = createCatalogDetailsProperties(icebergStatement);
        IcebergCatalogDetails details = new IcebergCatalogDetails(detailsDecl.getAdapter(), properties);
        return new IcebergCatalog(statement.getCatalogName(), statement.getCatalogType(), details,
                MetadataUtil.PENDING_NO_OP, creator);
    }

    protected Map<String, String> createCatalogDetailsProperties(IcebergCatalogCreateStatement statement)
            throws AlgebricksException {
        AdmObjectNode withObjectNode = statement.getWithObjectNode();
        IcebergCatalogDetailsDecl icebergCatalogDetailsDecl = statement.getCatalogDetailsDecl();
        Map<String, String> properties = new HashMap<>(icebergCatalogDetailsDecl.getProperties());
        Map<String, String> withClauseProperties = ExternalDataUtils
                .convertStringArrayParamIntoNumberedParameters(withObjectNode, statement.getSourceLocation());
        properties.putAll(withClauseProperties);
        return properties;
    }

    private void validateCatalogProperties(CatalogCreateStatement statement, MetadataTransactionContext mdTxnCtx,
            MetadataProvider metadataProvider) throws AlgebricksException {
        String catalogType = statement.getCatalogType();
        if (IcebergUtils.isIcebergCatalog(catalogType)) {
            validateIcebergCatalogProperties(statement, mdTxnCtx, metadataProvider);
        }
    }

    protected void validateIcebergCatalogProperties(CatalogCreateStatement statement,
            MetadataTransactionContext mdTxnCtx, MetadataProvider metadataProvider) throws AlgebricksException {
        IcebergCatalogCreateStatement icebergStatement = (IcebergCatalogCreateStatement) statement;
        IcebergCatalogDetailsDecl details = icebergStatement.getCatalogDetailsDecl();
        Map<String, String> allProperties = new HashMap<>(details.getProperties());
        LangRecordParseUtil.recordToMap(allProperties, icebergStatement.getWithObjectNode());
        IcebergUtils.validateCatalogProperties(allProperties);
    }
}
