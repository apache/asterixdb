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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppAstVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * This class rewrites delete statement to contain a query that specifying
 * what to delete.
 */
public class SqlppDeleteRewriteVisitor extends AbstractSqlppAstVisitor<Void, Void> {

    @Override
    public Void visit(DeleteStatement deleteStmt, Void visitArg) {
        List<Expression> arguments = new ArrayList<>();
        Identifier dataverseName = deleteStmt.getDataverseName();
        Identifier datasetName = deleteStmt.getDatasetName();
        String arg = dataverseName == null ? datasetName.getValue()
                : dataverseName.getValue() + "." + datasetName.getValue();
        LiteralExpr argumentLiteral = new LiteralExpr(new StringLiteral(arg));
        arguments.add(argumentLiteral);
        CallExpr callExpression = new CallExpr(new FunctionSignature(BuiltinFunctions.DATASET), arguments);
        callExpression.setSourceLocation(deleteStmt.getSourceLocation());

        // From clause.
        VariableExpr var = deleteStmt.getVariableExpr();
        FromTerm fromTerm = new FromTerm(callExpression, var, null, null);
        fromTerm.setSourceLocation(var.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(var.getSourceLocation());

        // Where clause.
        WhereClause whereClause = null;
        Expression condition = deleteStmt.getCondition();
        if (condition != null) {
            whereClause = new WhereClause(condition);
            whereClause.setSourceLocation(condition.getSourceLocation());
        }

        // Select clause.
        VariableExpr returnExpr = new VariableExpr(var.getVar());
        returnExpr.setIsNewVar(false);
        returnExpr.setSourceLocation(var.getSourceLocation());
        SelectElement selectElement = new SelectElement(returnExpr);
        selectElement.setSourceLocation(deleteStmt.getSourceLocation());
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(deleteStmt.getSourceLocation());

        // Construct the select expression.
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause,
                whereClause != null ? Collections.singletonList(whereClause) : null, null, null);
        selectBlock.setSourceLocation(var.getSourceLocation());
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(var.getSourceLocation());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, false);
        selectExpression.setSourceLocation(var.getSourceLocation());
        Query query = new Query(false, false, selectExpression, 0);
        query.setBody(selectExpression);
        query.setSourceLocation(deleteStmt.getSourceLocation());

        // return the delete statement.
        deleteStmt.setQuery(query);
        return null;
    }
}
