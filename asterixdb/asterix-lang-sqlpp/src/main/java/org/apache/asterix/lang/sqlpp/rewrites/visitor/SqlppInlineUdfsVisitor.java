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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.AbstractInlineUdfsVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.SqlppCloneAndSubstituteVariablesVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class SqlppInlineUdfsVisitor extends AbstractInlineUdfsVisitor implements ISqlppVisitor<Boolean, Void> {

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     * @param usedUDFs,
     *            user defined functions used by this query.
     * @param usedViews,
     *            views used by this query.
     */
    public SqlppInlineUdfsVisitor(LangRewritingContext context, Map<FunctionSignature, FunctionDecl> usedUDFs,
            Map<DatasetFullyQualifiedName, ViewDecl> usedViews) {
        super(context, usedUDFs, usedViews, new SqlppCloneAndSubstituteVariablesVisitor(context));
    }

    @Override
    protected Expression generateQueryExpression(List<LetClause> letClauses, Expression returnExpr)
            throws CompilationException {
        Map<Expression, Expression> varExprMap = extractLetBindingVariableExpressionMappings(letClauses);
        return SqlppRewriteUtil.substituteExpression(returnExpr, varExprMap, context);
    }

    @Override
    public Boolean visit(FromClause fromClause, Void arg) throws CompilationException {
        boolean changed = false;
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            changed |= fromTerm.accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, Void arg) throws CompilationException {
        boolean changed = false;
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(fromTerm.getLeftExpression());
        fromTerm.setLeftExpression(p.second);
        changed |= p.first;
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            changed |= correlateClause.accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visit(JoinClause joinClause, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p1 = inlineUdfsAndViewsInExpr(joinClause.getRightExpression());
        joinClause.setRightExpression(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsAndViewsInExpr(joinClause.getConditionExpression());
        joinClause.setConditionExpression(p2.second);
        return p1.first || p2.first;
    }

    @Override
    public Boolean visit(NestClause nestClause, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p1 = inlineUdfsAndViewsInExpr(nestClause.getRightExpression());
        nestClause.setRightExpression(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsAndViewsInExpr(nestClause.getConditionExpression());
        nestClause.setConditionExpression(p2.second);
        return p1.first || p2.first;
    }

    @Override
    public Boolean visit(Projection projection, Void arg) throws CompilationException {
        if (!projection.hasExpression()) {
            return false;
        }
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(projection.getExpression());
        projection.setExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, Void arg) throws CompilationException {
        boolean changed = false;
        if (selectBlock.hasFromClause()) {
            changed |= selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
                changed |= letWhereClause.accept(this, arg);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            changed |= selectBlock.getGroupbyClause().accept(this, arg);
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
                changed |= letHavingClause.accept(this, arg);
            }
        }
        changed |= selectBlock.getSelectClause().accept(this, arg);
        return changed;
    }

    @Override
    public Boolean visit(SelectClause selectClause, Void arg) throws CompilationException {
        boolean changed = false;
        if (selectClause.selectElement()) {
            changed |= selectClause.getSelectElement().accept(this, arg);
        } else {
            changed |= selectClause.getSelectRegular().accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectElement selectElement, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(selectElement.getExpression());
        selectElement.setExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, Void arg) throws CompilationException {
        boolean changed = false;
        for (Projection projection : selectRegular.getProjections()) {
            changed |= projection.accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, Void arg) throws CompilationException {
        boolean changed = false;
        changed |= selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            changed |= right.getSetOperationRightInput().accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectExpression selectExpression, Void arg) throws CompilationException {
        boolean changed = false;
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                changed |= letClause.accept(this, arg);
            }
        }
        changed |= selectExpression.getSelectSetOperation().accept(this, arg);
        if (selectExpression.hasOrderby()) {
            changed |= selectExpression.getOrderbyClause().accept(this, arg);
        }
        if (selectExpression.hasLimit()) {
            changed |= selectExpression.getLimitClause().accept(this, arg);
        }
        return changed;
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(unnestClause.getRightExpression());
        unnestClause.setRightExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(HavingClause havingClause, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(havingClause.getFilterExpression());
        havingClause.setFilterExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(CaseExpression caseExpr, Void arg) throws CompilationException {
        Pair<Boolean, Expression> result = inlineUdfsAndViewsInExpr(caseExpr.getConditionExpr());
        caseExpr.setConditionExpr(result.second);
        boolean inlined = result.first;

        Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(caseExpr.getWhenExprs());
        inlined = inlined || inlinedList.first;
        caseExpr.setWhenExprs(inlinedList.second);

        inlinedList = inlineUdfsInExprList(caseExpr.getThenExprs());
        inlined = inlined || inlinedList.first;
        caseExpr.setThenExprs(inlinedList.second);

        result = inlineUdfsAndViewsInExpr(caseExpr.getElseExpr());
        caseExpr.setElseExpr(result.second);
        return inlined || result.first;
    }

    @Override
    public Boolean visit(WindowExpression winExpr, Void arg) throws CompilationException {
        boolean inlined = false;
        if (winExpr.hasPartitionList()) {
            Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(winExpr.getPartitionList());
            winExpr.setPartitionList(inlinedList.second);
            inlined = inlinedList.first;
        }
        if (winExpr.hasOrderByList()) {
            Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(winExpr.getOrderbyList());
            winExpr.setOrderbyList(inlinedList.second);
            inlined |= inlinedList.first;
        }
        if (winExpr.hasFrameStartExpr()) {
            Pair<Boolean, Expression> inlinedExpr = inlineUdfsAndViewsInExpr(winExpr.getFrameStartExpr());
            winExpr.setFrameStartExpr(inlinedExpr.second);
            inlined |= inlinedExpr.first;
        }
        if (winExpr.hasFrameEndExpr()) {
            Pair<Boolean, Expression> inlinedExpr = inlineUdfsAndViewsInExpr(winExpr.getFrameEndExpr());
            winExpr.setFrameEndExpr(inlinedExpr.second);
            inlined |= inlinedExpr.first;
        }
        if (winExpr.hasWindowFieldList()) {
            Pair<Boolean, List<Pair<Expression, Identifier>>> inlinedList =
                    inlineUdfsInFieldList(winExpr.getWindowFieldList());
            winExpr.setWindowFieldList(inlinedList.second);
            inlined |= inlinedList.first;
        }
        if (winExpr.hasAggregateFilterExpr()) {
            Pair<Boolean, Expression> inlinedExpr = inlineUdfsAndViewsInExpr(winExpr.getAggregateFilterExpr());
            winExpr.setAggregateFilterExpr(inlinedExpr.second);
            inlined |= inlinedExpr.first;
        }
        Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(winExpr.getExprList());
        winExpr.setExprList(inlinedList.second);
        inlined |= inlinedList.first;
        return inlined;
    }

    @Override
    public Boolean visit(ListSliceExpression expression, Void arg) throws CompilationException {
        Pair<Boolean, Expression> expressionResult = inlineUdfsAndViewsInExpr(expression.getExpr());
        expression.setExpr(expressionResult.second);
        boolean inlined = expressionResult.first;

        Pair<Boolean, Expression> startIndexExpressResult =
                inlineUdfsAndViewsInExpr(expression.getStartIndexExpression());
        expression.setStartIndexExpression(startIndexExpressResult.second);
        inlined |= startIndexExpressResult.first;

        // End index expression can be null (optional)
        if (expression.hasEndExpression()) {
            Pair<Boolean, Expression> endIndexExpressionResult =
                    inlineUdfsAndViewsInExpr(expression.getEndIndexExpression());
            expression.setEndIndexExpression(endIndexExpressionResult.second);
            inlined |= endIndexExpressionResult.first;
        }

        return inlined;
    }

    private Map<Expression, Expression> extractLetBindingVariableExpressionMappings(List<LetClause> letClauses)
            throws CompilationException {
        Map<Expression, Expression> varExprMap = new HashMap<>();
        for (LetClause lc : letClauses) {
            // inline let variables one by one iteratively.
            lc.setBindingExpr(SqlppRewriteUtil.substituteExpression(lc.getBindingExpr(), varExprMap, context));
            varExprMap.put(lc.getVarExpr(), lc.getBindingExpr());
        }
        return varExprMap;
    }
}
