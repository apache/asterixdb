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
 */
abstract class AbstractSqlppExpressionExtractionVisitor extends AbstractSqlppSimpleExpressionVisitor {

    protected final LangRewritingContext context;

    protected final Deque<StackElement> stack = new ArrayDeque<>();

    AbstractSqlppExpressionExtractionVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        StackElement stackElement = new StackElement(selectBlock);
        stack.push(stackElement);

        if (selectBlock.hasFromClause()) {
            FromClause clause = selectBlock.getFromClause();
            clause.accept(this, arg);
            if (!stackElement.extractionList.isEmpty()) {
                handleUnsupportedClause(clause);
            }
        }
        List<AbstractClause> letWhereList = selectBlock.getLetWhereList();
        if (!letWhereList.isEmpty()) {
            visitLetWhereClauses(letWhereList, stackElement.extractionList, arg);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
            introduceLetClauses(stackElement.extractionList, letWhereList);
        }
        List<AbstractClause> letHavingListAfterGby = selectBlock.getLetHavingListAfterGroupby();
        if (!letHavingListAfterGby.isEmpty()) {
            visitLetWhereClauses(letHavingListAfterGby, stackElement.extractionList, arg);
        }
        selectBlock.getSelectClause().accept(this, arg);
        introduceLetClauses(stackElement.extractionList,
                selectBlock.hasGroupbyClause() ? letHavingListAfterGby : letWhereList);

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

    abstract void handleUnsupportedClause(FromClause clause) throws CompilationException;

    protected final class StackElement {

        private final SelectBlock selectBlock;

        private final List<Pair<Expression, VarIdentifier>> extractionList;

        private StackElement(SelectBlock selectBlock) {
            this.selectBlock = selectBlock;
            this.extractionList = new ArrayList<>();
        }

        public SelectBlock getSelectBlock() {
            return selectBlock;
        }

        public VarIdentifier addPendingLetClause(Expression expression) {
            VarIdentifier letVar = context.newVariable();
            extractionList.add(new Pair<>(expression, letVar));
            return letVar;
        }
    }
}
