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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.CheckSql92AggregateVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * A pre-processor that
 * <ul>
 *     <li>adds the group variable as well as its group field
 *         list into the AST. e.g., GROUP AS eis(e AS e, i AS i, s AS s)</li>
 *     <li>adds group by clause if select block contains SQL-92 agregate function but there's no group by clause</li>
 * </ul>
 */
public class SqlppGroupByVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    public SqlppGroupByVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        if (selectBlock.hasFromClause()) {
            if (selectBlock.hasGroupbyClause()) {
                rewriteSelectWithGroupBy(selectBlock, arg);
            } else {
                rewriteSelectWithoutGroupBy(selectBlock);
            }
        }
        return super.visit(selectBlock, arg);
    }

    private void rewriteSelectWithGroupBy(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        GroupbyClause gbyClause = selectBlock.getGroupbyClause();

        // Sets the group variable.
        if (!gbyClause.hasGroupVar()) {
            VariableExpr groupVar = new VariableExpr(context.newVariable());
            groupVar.setSourceLocation(gbyClause.getSourceLocation());
            gbyClause.setGroupVar(groupVar);
        }

        // Sets the field list for the group variable.
        List<Pair<Expression, Identifier>> groupFieldList;
        if (gbyClause.hasGroupFieldList()) {
            groupFieldList = new ArrayList<>();
            for (Pair<Expression, Identifier> groupField : gbyClause.getGroupFieldList()) {
                Expression newFieldExpr = groupField.first.accept(this, arg);
                groupFieldList.add(new Pair<>(newFieldExpr, groupField.second));
            }
        } else {
            groupFieldList = createGroupFieldList(selectBlock);
        }
        gbyClause.setGroupFieldList(groupFieldList);
    }

    private void rewriteSelectWithoutGroupBy(SelectBlock selectBlock) throws CompilationException {
        if (hasSql92Aggregate(selectBlock)) {
            // Adds an implicit group-by clause for SQL-92 global aggregate.
            List<GbyVariableExpressionPair> gbyPairList = new ArrayList<>();
            List<GbyVariableExpressionPair> decorPairList = new ArrayList<>();
            VariableExpr groupVar = new VariableExpr(context.newVariable());
            groupVar.setSourceLocation(selectBlock.getSourceLocation());
            List<Pair<Expression, Identifier>> groupFieldList = createGroupFieldList(selectBlock);
            GroupbyClause gbyClause = new GroupbyClause(gbyPairList, decorPairList, new HashMap<>(), groupVar,
                    groupFieldList, false, true);
            gbyClause.setSourceLocation(selectBlock.getSourceLocation());
            selectBlock.setGroupbyClause(gbyClause);
        }
    }

    private boolean hasSql92Aggregate(SelectBlock selectBlock) throws CompilationException {
        SelectClause selectClause = selectBlock.getSelectClause();
        if (selectClause.selectRegular()) {
            return isSql92Aggregate(selectClause.getSelectRegular(), selectBlock);
        } else if (selectClause.selectElement()) {
            return isSql92Aggregate(selectClause.getSelectElement(), selectBlock);
        } else {
            throw new IllegalStateException();
        }
    }

    private boolean isSql92Aggregate(ILangExpression expr, SelectBlock selectBlock) throws CompilationException {
        CheckSql92AggregateVisitor visitor = new CheckSql92AggregateVisitor();
        return expr.accept(visitor, selectBlock);
    }

    private List<Pair<Expression, Identifier>> createGroupFieldList(SelectBlock selectBlock) {
        List<Pair<Expression, Identifier>> groupFieldList = new ArrayList<>();
        addToFieldList(groupFieldList, SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause()));
        addToFieldList(groupFieldList, SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetWhereList()));
        return groupFieldList;
    }

    private void addToFieldList(List<Pair<Expression, Identifier>> outFieldList, List<VariableExpr> varList) {
        for (VariableExpr varExpr : varList) {
            SqlppVariableUtil.addToFieldVariableList(varExpr, outFieldList);
        }
    }
}
