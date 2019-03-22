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
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
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
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class SqlppInlineUdfsVisitor extends AbstractInlineUdfsVisitor
        implements ISqlppVisitor<Boolean, List<FunctionDecl>> {

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     * @param rewriterFactory,
     *            a rewrite factory for rewriting user-defined functions.
     * @param declaredFunctions,
     *            a list of declared functions associated with the query.
     * @param metadataProvider,
     *            providing the definition of created (i.e., stored) user-defined functions.
     */
    public SqlppInlineUdfsVisitor(LangRewritingContext context, IRewriterFactory rewriterFactory,
            List<FunctionDecl> declaredFunctions, MetadataProvider metadataProvider) {
        super(context, rewriterFactory, declaredFunctions, metadataProvider,
                new SqlppCloneAndSubstituteVariablesVisitor(context));
    }

    @Override
    protected Expression generateQueryExpression(List<LetClause> letClauses, Expression returnExpr)
            throws CompilationException {
        Map<Expression, Expression> varExprMap = extractLetBindingVariableExpressionMappings(letClauses);
        return SqlppRewriteUtil.substituteExpression(returnExpr, varExprMap, context);
    }

    @Override
    public Boolean visit(FromClause fromClause, List<FunctionDecl> func) throws CompilationException {
        boolean changed = false;
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            changed |= fromTerm.accept(this, func);
        }
        return changed;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, List<FunctionDecl> func) throws CompilationException {
        boolean changed = false;
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fromTerm.getLeftExpression(), func);
        fromTerm.setLeftExpression(p.second);
        changed |= p.first;
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            changed |= correlateClause.accept(this, func);
        }
        return changed;
    }

    @Override
    public Boolean visit(JoinClause joinClause, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(joinClause.getRightExpression(), funcs);
        joinClause.setRightExpression(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(joinClause.getConditionExpression(), funcs);
        joinClause.setConditionExpression(p2.second);
        return p1.first || p2.first;
    }

    @Override
    public Boolean visit(NestClause nestClause, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(nestClause.getRightExpression(), funcs);
        nestClause.setRightExpression(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(nestClause.getConditionExpression(), funcs);
        nestClause.setConditionExpression(p2.second);
        return p1.first || p2.first;
    }

    @Override
    public Boolean visit(Projection projection, List<FunctionDecl> funcs) throws CompilationException {
        if (projection.star()) {
            return false;
        }
        Pair<Boolean, Expression> p = inlineUdfsInExpr(projection.getExpression(), funcs);
        projection.setExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, List<FunctionDecl> funcs) throws CompilationException {
        boolean changed = false;
        if (selectBlock.hasFromClause()) {
            changed |= selectBlock.getFromClause().accept(this, funcs);
        }
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
                changed |= letWhereClause.accept(this, funcs);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            changed |= selectBlock.getGroupbyClause().accept(this, funcs);
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
                changed |= letHavingClause.accept(this, funcs);
            }
        }
        changed |= selectBlock.getSelectClause().accept(this, funcs);
        return changed;
    }

    @Override
    public Boolean visit(SelectClause selectClause, List<FunctionDecl> funcs) throws CompilationException {
        boolean changed = false;
        if (selectClause.selectElement()) {
            changed |= selectClause.getSelectElement().accept(this, funcs);
        } else {
            changed |= selectClause.getSelectRegular().accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectElement selectElement, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(selectElement.getExpression(), funcs);
        selectElement.setExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, List<FunctionDecl> funcs) throws CompilationException {
        boolean changed = false;
        for (Projection projection : selectRegular.getProjections()) {
            changed |= projection.accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, List<FunctionDecl> funcs) throws CompilationException {
        boolean changed = false;
        changed |= selectSetOperation.getLeftInput().accept(this, funcs);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            changed |= right.getSetOperationRightInput().accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(SelectExpression selectExpression, List<FunctionDecl> funcs) throws CompilationException {
        boolean changed = false;
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                changed |= letClause.accept(this, funcs);
            }
        }
        changed |= selectExpression.getSelectSetOperation().accept(this, funcs);
        if (selectExpression.hasOrderby()) {
            changed |= selectExpression.getOrderbyClause().accept(this, funcs);
        }
        if (selectExpression.hasLimit()) {
            changed |= selectExpression.getLimitClause().accept(this, funcs);
        }
        return changed;
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(unnestClause.getRightExpression(), funcs);
        unnestClause.setRightExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(HavingClause havingClause, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(havingClause.getFilterExpression(), funcs);
        havingClause.setFilterExpression(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(CaseExpression caseExpr, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> result = inlineUdfsInExpr(caseExpr.getConditionExpr(), funcs);
        caseExpr.setConditionExpr(result.second);
        boolean inlined = result.first;

        Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(caseExpr.getWhenExprs(), funcs);
        inlined = inlined || inlinedList.first;
        caseExpr.setWhenExprs(inlinedList.second);

        inlinedList = inlineUdfsInExprList(caseExpr.getThenExprs(), funcs);
        inlined = inlined || inlinedList.first;
        caseExpr.setThenExprs(inlinedList.second);

        result = inlineUdfsInExpr(caseExpr.getElseExpr(), funcs);
        caseExpr.setElseExpr(result.second);
        return inlined || result.first;
    }

    @Override
    public Boolean visit(WindowExpression winExpr, List<FunctionDecl> funcs) throws CompilationException {
        boolean inlined = false;
        if (winExpr.hasPartitionList()) {
            Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(winExpr.getPartitionList(), funcs);
            winExpr.setPartitionList(inlinedList.second);
            inlined = inlinedList.first;
        }
        if (winExpr.hasOrderByList()) {
            Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(winExpr.getOrderbyList(), funcs);
            winExpr.setOrderbyList(inlinedList.second);
            inlined |= inlinedList.first;
        }
        if (winExpr.hasFrameStartExpr()) {
            Pair<Boolean, Expression> inlinedExpr = inlineUdfsInExpr(winExpr.getFrameStartExpr(), funcs);
            winExpr.setFrameStartExpr(inlinedExpr.second);
            inlined |= inlinedExpr.first;
        }
        if (winExpr.hasFrameEndExpr()) {
            Pair<Boolean, Expression> inlinedExpr = inlineUdfsInExpr(winExpr.getFrameEndExpr(), funcs);
            winExpr.setFrameEndExpr(inlinedExpr.second);
            inlined |= inlinedExpr.first;
        }
        if (winExpr.hasWindowFieldList()) {
            Pair<Boolean, List<Pair<Expression, Identifier>>> inlinedList =
                    inlineUdfsInFieldList(winExpr.getWindowFieldList(), funcs);
            winExpr.setWindowFieldList(inlinedList.second);
            inlined |= inlinedList.first;
        }
        Pair<Boolean, List<Expression>> inlinedList = inlineUdfsInExprList(winExpr.getExprList(), funcs);
        winExpr.setExprList(inlinedList.second);
        inlined |= inlinedList.first;
        return inlined;
    }

    @Override
    public Boolean visit(ListSliceExpression expression, List<FunctionDecl> funcs) throws CompilationException {
        Pair<Boolean, Expression> expressionResult = inlineUdfsInExpr(expression.getExpr(), funcs);
        expression.setExpr(expressionResult.second);
        boolean inlined = expressionResult.first;

        Pair<Boolean, Expression> startIndexExpressResult =
                inlineUdfsInExpr(expression.getStartIndexExpression(), funcs);
        expression.setStartIndexExpression(startIndexExpressResult.second);
        inlined |= startIndexExpressResult.first;

        // End index expression can be null (optional)
        if (expression.hasEndExpression()) {
            Pair<Boolean, Expression> endIndexExpressionResult =
                    inlineUdfsInExpr(expression.getEndIndexExpression(), funcs);
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
