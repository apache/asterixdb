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
import java.util.List;

import org.apache.asterix.common.functions.FunctionConstants;
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
import org.mortbay.util.SingletonList;

/**
 * This class rewrites delete statement to contain a query that specifying
 * what to delete.
 */
public class SqlppDeleteRewriteVisitor extends AbstractSqlppAstVisitor<Void, Void> {

    @Override
    public Void visit(DeleteStatement deleteStmt, Void visitArg) {
        List<Expression> arguments = new ArrayList<Expression>();
        Identifier dataverseName = deleteStmt.getDataverseName();
        Identifier datasetName = deleteStmt.getDatasetName();
        String arg = dataverseName == null ? datasetName.getValue()
                : dataverseName.getValue() + "." + datasetName.getValue();
        LiteralExpr argumentLiteral = new LiteralExpr(new StringLiteral(arg));
        arguments.add(argumentLiteral);
        CallExpr callExpression = new CallExpr(new FunctionSignature(FunctionConstants.ASTERIX_NS, "dataset", 1),
                arguments);

        // From clause.
        VariableExpr var = deleteStmt.getVariableExpr();
        FromTerm fromTerm = new FromTerm(callExpression, var, null, null);
        @SuppressWarnings("unchecked")
        FromClause fromClause = new FromClause(SingletonList.newSingletonList(fromTerm));

        // Where clause.
        WhereClause whereClause = null;
        Expression condition = deleteStmt.getCondition();
        if (condition != null) {
            whereClause = new WhereClause(condition);
        }

        // Select clause.
        VariableExpr returnExpr = new VariableExpr(var.getVar());
        returnExpr.setIsNewVar(false);
        SelectElement selectElement = new SelectElement(returnExpr);
        SelectClause selectClause = new SelectClause(selectElement, null, false);

        // Construct the select expression.
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, whereClause, null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, false);
        Query query = new Query();
        query.setBody(selectExpression);

        // return the delete statement.
        deleteStmt.setQuery(query);
        return null;
    }

}
