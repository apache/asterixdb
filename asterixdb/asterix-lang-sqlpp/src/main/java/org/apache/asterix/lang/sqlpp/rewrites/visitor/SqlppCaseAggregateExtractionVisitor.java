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

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Extracts SQL-92 aggregate functions from CASE/IF expressions into LET clauses,
 * so they can be pushed into GROUPBY subplans by the optimizer.
 */
public final class SqlppCaseAggregateExtractionVisitor extends AbstractSqlppExpressionExtractionVisitor {

    public SqlppCaseAggregateExtractionVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    protected void visitLetWhereClauses(List<AbstractClause> letWhereList, ILangExpression arg,
            List<Pair<Expression, VarIdentifier>> extractionList) {
        // do not perform the extraction
    }

    @Override
    protected void visitGroupByClause(GroupbyClause groupbyClause, ILangExpression arg,
            List<Pair<Expression, VarIdentifier>> extractionList, List<AbstractClause> letWhereList) {
        // do not perform the extraction
    }

    @Override
    public Expression visit(CaseExpression caseExpr, ILangExpression arg) throws CompilationException {
        Expression resultExpr = super.visit(caseExpr, arg);
        resultExpr.accept(new Sql92AggregateExtractionVisitor(), arg);
        return resultExpr;
    }

    @Override
    public Expression visit(IfExpr ifExpr, ILangExpression arg) throws CompilationException {
        Expression resultExpr = super.visit(ifExpr, arg);
        resultExpr.accept(new Sql92AggregateExtractionVisitor(), arg);
        return resultExpr;
    }

    @Override
    void handleUnsupportedClause(FromClause clause) throws CompilationException {
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_USE_OF_IDENTIFIER, clause.getSourceLocation(),
                BuiltinFunctions.SWITCH_CASE.getName());
    }

    private final class Sql92AggregateExtractionVisitor extends AbstractSqlppSimpleExpressionVisitor {

        @Override
        public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
            CallExpr resultExpr = (CallExpr) super.visit(callExpr, arg);
            if (FunctionMapUtil.isSql92AggregateFunction(resultExpr.getFunctionSignature())) {
                StackElement stackElement = stack.peek();
                if (stackElement != null && stackElement.getSelectBlock().hasGroupbyClause()) {
                    VarIdentifier v = stackElement.addPendingLetClause(resultExpr);
                    VariableExpr vExpr = new VariableExpr(v);
                    vExpr.setSourceLocation(callExpr.getSourceLocation());
                    return vExpr;
                }
            }
            return resultExpr;
        }

        @Override
        public Expression visit(SelectExpression selectExpression, ILangExpression arg) {
            // don't visit sub-queries
            return selectExpression;
        }
    }
}
