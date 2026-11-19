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

import java.util.Collections;

import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.LongIntegerLiteral;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppAstVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;

/**
 * Builds the query of an UPDATE statement, reading the collection it targets in the active namespace. Must run after
 * {@link SqlppSynonymRewriteVisitor}, so that the query reads the collection a synonym names rather than the synonym
 * itself.
 */
public class SqlppUpdateRewriteVisitor extends AbstractSqlppAstVisitor<Void, MetadataProvider> {

    public static final SqlppUpdateRewriteVisitor INSTANCE = new SqlppUpdateRewriteVisitor();

    private SqlppUpdateRewriteVisitor() {
    }

    @Override
    public Void visit(UpdateStatement updateStmt, MetadataProvider metadataProvider) {
        Namespace namespace = updateStmt.getNamespace();
        if (namespace == null) {
            // later stages read the statement's namespace directly, so they must see the resolved one
            namespace = metadataProvider.getDefaultNamespace();
            updateStmt.setNamespace(namespace);
        }
        CallExpr datasetCall = FunctionUtil.makeDatasetCallExpr(namespace.getDatabaseName(),
                namespace.getDataverseName(), updateStmt.getDatasetName());
        datasetCall.setSourceLocation(updateStmt.getSourceLocation());

        VariableExpr aliasVar = updateStmt.getVar();
        FromTerm fromTerm = new FromTerm(datasetCall, aliasVar, null, null);
        fromTerm.setSourceLocation(aliasVar.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(aliasVar.getSourceLocation());

        WhereClause whereClause = null;
        if (updateStmt.getCondition() != null) {
            whereClause = new WhereClause(updateStmt.getCondition());
            whereClause.setSourceLocation(updateStmt.getCondition().getSourceLocation());
        }

        // the change sequence is seeded with [alias], so its result is a one-element list holding the new record
        IndexAccessor newRecordExpr = new IndexAccessor(updateStmt.getChangeSeq(), IndexAccessor.IndexKind.ELEMENT,
                new LiteralExpr(new LongIntegerLiteral(0L)));
        newRecordExpr.setSourceLocation(updateStmt.getSourceLocation());
        SelectElement selectElement = new SelectElement(newRecordExpr);
        selectElement.setSourceLocation(updateStmt.getSourceLocation());
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(updateStmt.getSourceLocation());

        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause,
                whereClause != null ? Collections.singletonList(whereClause) : null, null, null);
        selectBlock.setSourceLocation(aliasVar.getSourceLocation());
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(aliasVar.getSourceLocation());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, false);
        selectExpression.setSourceLocation(aliasVar.getSourceLocation());

        Query query = new Query(false, false, true, selectExpression, 0);
        query.setBody(selectExpression);
        query.setSourceLocation(updateStmt.getSourceLocation());
        updateStmt.setQuery(query);
        return null;
    }
}
