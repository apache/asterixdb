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
import org.apache.asterix.common.exceptions.ErrorCode;
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
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Base class for visitors that extract expressions into LET clauses.
 * Subclasses should call {@link #extractExpressions(List, int, SourceLocation)} to perform the extraction.
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
        List<LetClause> letList = selectBlock.getLetList();
        if (selectBlock.hasLetClauses()) {
            visitLetClauses(letList, extractionList, arg);
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, arg);
            introduceLetClauses(extractionList, letList);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
            introduceLetClauses(extractionList, letList);
        }
        List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
        if (selectBlock.hasLetClausesAfterGroupby()) {
            visitLetClauses(letListAfterGby, extractionList, arg);
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(this, arg);
            introduceLetClauses(extractionList, letListAfterGby);
        }
        selectBlock.getSelectClause().accept(this, arg);
        introduceLetClauses(extractionList, selectBlock.hasGroupbyClause() ? letListAfterGby : letList);

        stack.pop();
        return null;
    }

    private void visitLetClauses(List<LetClause> letList, List<Pair<Expression, VarIdentifier>> extractionList,
            ILangExpression arg) throws CompilationException {
        List<LetClause> newLetList = new ArrayList<>(letList.size());
        for (LetClause letClause : letList) {
            letClause.accept(this, arg);
            introduceLetClauses(extractionList, newLetList);
            newLetList.add(letClause);
        }
        if (newLetList.size() > letList.size()) {
            letList.clear();
            letList.addAll(newLetList);
        }
    }

    private void introduceLetClauses(List<Pair<Expression, VarIdentifier>> fromBindingList, List<LetClause> toLetList) {
        for (Pair<Expression, VarIdentifier> p : fromBindingList) {
            Expression bindExpr = p.first;
            VarIdentifier var = p.second;
            VariableExpr varExpr = new VariableExpr(var);
            varExpr.setSourceLocation(bindExpr.getSourceLocation());
            toLetList.add(new LetClause(varExpr, bindExpr));
            context.addExcludedForFieldAccessVar(var);
        }
        fromBindingList.clear();
    }

    List<Expression> extractExpressions(List<Expression> exprList, int limit, SourceLocation sourceLocation)
            throws CompilationException {
        List<Pair<Expression, VarIdentifier>> outLetList = stack.peek();
        if (outLetList == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLocation);
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
