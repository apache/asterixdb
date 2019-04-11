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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Base class for visitors that extract expressions into LET clauses.
 * Subclasses should call {@link #extractExpressions(List, int)} to perform the extraction.
 */
abstract class AbstractSqlppExpressionExtractionVisitor extends AbstractSqlppSimpleExpressionVisitor {

    protected final LangRewritingContext context;

    private final Deque<List<Pair<Expression, VarIdentifier>>> stack = new ArrayDeque<>();

    AbstractSqlppExpressionExtractionVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        List<Pair<Expression, VarIdentifier>> extractionList = new ArrayList<>();
        stack.push(extractionList);

        if (selectBlock.hasFromClause()) {
            FromClause clause = selectBlock.getFromClause();
            clause.accept(this, arg);
            if (!extractionList.isEmpty()) {
                handleUnsupportedClause(clause, extractionList);
            }
        }
        List<AbstractClause> letWhereList = selectBlock.getLetWhereList();
        if (!letWhereList.isEmpty()) {
            visitLetWhereClauses(letWhereList, extractionList, arg);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
            introduceLetClauses(extractionList, letWhereList);
        }
        List<AbstractClause> letHavingListAfterGby = selectBlock.getLetHavingListAfterGroupby();
        if (!letHavingListAfterGby.isEmpty()) {
            visitLetWhereClauses(letHavingListAfterGby, extractionList, arg);
        }
        selectBlock.getSelectClause().accept(this, arg);
        introduceLetClauses(extractionList, selectBlock.hasGroupbyClause() ? letHavingListAfterGby : letWhereList);

        stack.pop();
        return null;
    }

    private void visitLetWhereClauses(List<AbstractClause> clauseList,
            List<Pair<Expression, VarIdentifier>> extractionList, ILangExpression arg) throws CompilationException {
        List<AbstractClause> newClauseList = new ArrayList<>(clauseList.size());
        for (AbstractClause letWhereClause : clauseList) {
            letWhereClause.accept(this, arg);
            introduceLetClauses(extractionList, newClauseList);
            newClauseList.add(letWhereClause);
        }
        if (newClauseList.size() > clauseList.size()) {
            clauseList.clear();
            clauseList.addAll(newClauseList);
        }
    }

    private void introduceLetClauses(List<Pair<Expression, VarIdentifier>> fromBindingList,
            List<AbstractClause> toLetWhereList) {
        for (Pair<Expression, VarIdentifier> p : fromBindingList) {
            Expression bindExpr = p.first;
            VarIdentifier var = p.second;
            VariableExpr varExpr = new VariableExpr(var);
            varExpr.setSourceLocation(bindExpr.getSourceLocation());
            toLetWhereList.add(new LetClause(varExpr, bindExpr));
        }
        fromBindingList.clear();
    }

    List<Expression> extractExpressions(List<Expression> exprList, int limit) {
        List<Pair<Expression, VarIdentifier>> outLetList = stack.peek();
        if (outLetList == null) {
            return null;
        }
        int n = exprList.size();
        List<Expression> newExprList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Expression expr = exprList.get(i);
            Expression newExpr;
            if (i < limit && isExtractableExpression(expr)) {
                VarIdentifier v = context.newVariable();
                VariableExpr vExpr = new VariableExpr(v);
                vExpr.setSourceLocation(expr.getSourceLocation());
                outLetList.add(new Pair<>(expr, v));
                newExpr = vExpr;
            } else {
                newExpr = expr;
            }
            newExprList.add(newExpr);
        }
        return newExprList;
    }

    abstract boolean isExtractableExpression(Expression expr);

    abstract void handleUnsupportedClause(FromClause clause, List<Pair<Expression, VarIdentifier>> extractionList)
            throws CompilationException;
}
