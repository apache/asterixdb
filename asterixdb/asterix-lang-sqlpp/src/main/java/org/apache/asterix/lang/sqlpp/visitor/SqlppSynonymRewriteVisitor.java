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

package org.apache.asterix.lang.sqlpp.visitor;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.LoadStatement;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppAstVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This class resolves dataset synonyms in load/insert/upsert/delete statements
 */
public class SqlppSynonymRewriteVisitor extends AbstractSqlppAstVisitor<Void, MetadataProvider> {

    public static final SqlppSynonymRewriteVisitor INSTANCE = new SqlppSynonymRewriteVisitor();

    private SqlppSynonymRewriteVisitor() {
    }

    @Override
    public Void visit(LoadStatement loadStmt, MetadataProvider metadataProvider) throws CompilationException {
        Quadruple<DataverseName, String, Boolean, String> dsName =
                resolveDatasetNameUsingSynonyms(metadataProvider, loadStmt.getDatabaseName(),
                        loadStmt.getDataverseName(), loadStmt.getDatasetName(), false, loadStmt.getSourceLocation());
        if (dsName != null) {
            loadStmt.setNamespace(new Namespace(dsName.getFourth(), dsName.getFirst()));
            loadStmt.setDatasetName(dsName.getSecond());
        }
        return null;
    }

    @Override
    public Void visit(InsertStatement insertStmt, MetadataProvider metadataProvider) throws CompilationException {
        Quadruple<DataverseName, String, Boolean, String> dsName = resolveDatasetNameUsingSynonyms(metadataProvider,
                insertStmt.getDatabaseName(), insertStmt.getDataverseName(), insertStmt.getDatasetName(), false,
                insertStmt.getSourceLocation());
        if (dsName != null) {
            insertStmt.setNamespace(new Namespace(dsName.getFourth(), dsName.getFirst()));
            insertStmt.setDatasetName(dsName.getSecond());
        }
        return null;
    }

    @Override
    public Void visit(DeleteStatement deleteStmt, MetadataProvider metadataProvider) throws CompilationException {
        Quadruple<DataverseName, String, Boolean, String> dsName = resolveDatasetNameUsingSynonyms(metadataProvider,
                deleteStmt.getDatabaseName(), deleteStmt.getDataverseName(), deleteStmt.getDatasetName(), false,
                deleteStmt.getSourceLocation());
        if (dsName != null) {
            deleteStmt.setNamespace(new Namespace(dsName.getFourth(), dsName.getFirst()));
            deleteStmt.setDatasetName(dsName.getSecond());
        }
        return null;
    }

    private Quadruple<DataverseName, String, Boolean, String> resolveDatasetNameUsingSynonyms(
            MetadataProvider metadataProvider, String databaseName, DataverseName dataverseName, String datasetName,
            boolean includingViews, SourceLocation sourceLoc) throws CompilationException {
        try {
            return metadataProvider.resolveDatasetNameUsingSynonyms(databaseName, dataverseName, datasetName,
                    includingViews);
        } catch (AlgebricksException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, sourceLoc, e.getMessage());
        }
    }
}
