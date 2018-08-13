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
package org.apache.asterix.lang.aql.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.visitor.base.AbstractAqlAstVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.DeleteStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.om.functions.BuiltinFunctions;

public class AqlDeleteRewriteVisitor extends AbstractAqlAstVisitor<Void, Void> {

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

        List<Clause> clauseList = new ArrayList<>();
        VariableExpr var = deleteStmt.getVariableExpr();
        Clause forClause = new ForClause(var, callExpression);
        clauseList.add(forClause);
        Clause whereClause = null;
        Expression condition = deleteStmt.getCondition();
        if (condition != null) {
            whereClause = new WhereClause(condition);
            clauseList.add(whereClause);
        }
        VariableExpr returnExpr = new VariableExpr(var.getVar());
        returnExpr.setIsNewVar(false);
        FLWOGRExpression flowgr = new FLWOGRExpression(clauseList, returnExpr);
        Query query = new Query(false);
        query.setBody(flowgr);
        deleteStmt.setQuery(query);
        return null;
    }

}
