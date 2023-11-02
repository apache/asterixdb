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
package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.Collections;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.CopyToStatement;
import org.apache.asterix.lang.common.statement.Query;
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

public class SqlppCopyToRewriteVisitor extends AbstractSqlppAstVisitor<Void, MetadataProvider> {
    public static final SqlppCopyToRewriteVisitor INSTANCE = new SqlppCopyToRewriteVisitor();

    @Override
    public Void visit(CopyToStatement stmtCopy, MetadataProvider metadataProvider) throws CompilationException {
        if (stmtCopy.getNamespace() == null) {
            stmtCopy.setNamespace(metadataProvider.getDefaultNamespace());
        }
        if (stmtCopy.getQuery() == null) {
            setQuery(stmtCopy);
        }
        return null;
    }

    private void setQuery(CopyToStatement stmtCopy) {
        DataverseName dataverseName = stmtCopy.getNamespace().getDataverseName();
        String datasetName = stmtCopy.getDatasetName();
        CallExpr callExpression =
                FunctionUtil.makeDatasetCallExpr(stmtCopy.getNamespace().getDatabaseName(), dataverseName, datasetName);
        callExpression.setSourceLocation(stmtCopy.getSourceLocation());

        // From clause.
        VariableExpr var = stmtCopy.getSourceVariable();
        FromTerm fromTerm = new FromTerm(callExpression, var, null, null);
        fromTerm.setSourceLocation(var.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(var.getSourceLocation());

        // Select clause.
        VariableExpr returnExpr = new VariableExpr(var.getVar());
        returnExpr.setIsNewVar(false);
        returnExpr.setSourceLocation(var.getSourceLocation());
        SelectElement selectElement = new SelectElement(returnExpr);
        selectElement.setSourceLocation(stmtCopy.getSourceLocation());
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(stmtCopy.getSourceLocation());

        // Construct the select expression.
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null);
        selectBlock.setSourceLocation(var.getSourceLocation());
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(var.getSourceLocation());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, false);
        selectExpression.setSourceLocation(var.getSourceLocation());
        Query query = new Query(false, false, selectExpression, 0);
        query.setBody(selectExpression);
        query.setSourceLocation(stmtCopy.getSourceLocation());

        // return the copy statement.
        stmtCopy.setQuery(query);
    }
}
