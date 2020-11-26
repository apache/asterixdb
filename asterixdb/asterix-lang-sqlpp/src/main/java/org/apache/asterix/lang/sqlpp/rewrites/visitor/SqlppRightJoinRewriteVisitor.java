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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.annotation.ExcludeFromSelectStarAnnotation;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateWithConditionClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Rewrites RIGHT OUTER JOIN into LEFT OUTER JOIN as follows:
 * <p>
 * Single preceding clause:
 * <pre>
 * FROM x RIGHT OUTER JOIN y
 * -->
 * FROM y LEFT OUTER JOIN x
 * </pre>
 * Multiple preceding clauses:
 * <pre>
 * FROM x JOIN y RIGHT OUTER JOIN z
 * -->
 * FROM z LEFT OUTER JOIN ( FROM x JOIN y SELECT ... )
 * </pre>
 * Note, clauses preceding to the RIGHT JOIN clause should not have any non-external free variables.
 * If they do then this rewriting fails and the query would fail later during logical plan generation.
 * E.g.
 * <pre>
 * FROM x
 * LET v = ( FROM x.nested RIGHT OUTER JOIN y ... )
 * ...
 * </pre>
 */
public final class SqlppRightJoinRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    private final Collection<VarIdentifier> externalVars;

    public SqlppRightJoinRewriteVisitor(LangRewritingContext context, Collection<VarIdentifier> externalVars) {
        this.context = context;
        this.externalVars = externalVars;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        super.visit(selectBlock, arg);

        if (selectBlock.hasFromClause()) {
            // we only support a FromClause with a single FromTerm for now
            List<FromTerm> fromTerms = selectBlock.getFromClause().getFromTerms();
            if (fromTerms.size() == 1) {
                FromTerm fromTerm = fromTerms.get(0);
                if (canRewriteFromTerm(fromTerm)) {
                    Pair<FromTerm, List<LetClause>> newFromLetPair = rewriteFromTerm(fromTerm);
                    fromTerms.set(0, newFromLetPair.first);
                    selectBlock.getLetWhereList().addAll(0, newFromLetPair.second);
                }
            }
        }

        return null;
    }

    private boolean canRewriteFromTerm(FromTerm fromTerm) throws CompilationException {
        boolean hasRightOuterJoin = false;
        List<AbstractBinaryCorrelateClause> clauseList = fromTerm.getCorrelateClauses();
        for (AbstractBinaryCorrelateClause correlateClause : clauseList) {
            if (correlateClause.getClauseType() == Clause.ClauseType.JOIN_CLAUSE) {
                JoinClause joinClause = (JoinClause) correlateClause;
                if (joinClause.getJoinType() == JoinType.RIGHTOUTER) {
                    hasRightOuterJoin = true;
                    break;
                }
            }
        }
        if (!hasRightOuterJoin) {
            // nothing to do
            return false;
        }

        Set<VariableExpr> fromExprFreeVars = SqlppVariableUtil.getFreeVariables(fromTerm.getLeftExpression());
        for (VariableExpr freeVarExpr : fromExprFreeVars) {
            if (!externalVars.contains(freeVarExpr.getVar())) {
                // cannot rewrite. will fail later
                return false;
            }
        }

        return true;
    }

    private Pair<FromTerm, List<LetClause>> rewriteFromTerm(FromTerm fromTerm) throws CompilationException {
        Map<VariableExpr, Expression> substMapInner = new HashMap<>();
        Map<VariableExpr, Expression> substMapOuterTmp = new HashMap<>();
        Map<VariableExpr, Expression> substMapOuterFinal = new LinkedHashMap<>();

        Expression fromExpr = fromTerm.getLeftExpression();
        VariableExpr fromVar = fromTerm.getLeftVariable();
        VariableExpr fromPosVar = fromTerm.getPositionalVariable();

        List<AbstractBinaryCorrelateClause> correlateClauses = fromTerm.getCorrelateClauses();
        for (int i = 0; i < correlateClauses.size(); i++) {
            AbstractBinaryCorrelateClause correlateClause = correlateClauses.get(i);
            if (correlateClause.getClauseType() == Clause.ClauseType.JOIN_CLAUSE
                    && ((JoinClause) correlateClause).getJoinType() == JoinType.RIGHTOUTER) {
                JoinClause joinClause = (JoinClause) correlateClause;
                SourceLocation joinClauseSourceLoc = joinClause.getSourceLocation();

                Expression rightExpr = joinClause.getRightExpression();
                VariableExpr rightVar = joinClause.getRightVariable();
                VariableExpr rightPosVar = joinClause.getPositionalVariable();
                Expression condExpr = joinClause.getConditionExpression();
                if (i == 0) {
                    JoinClause newJoinClause =
                            new JoinClause(JoinType.LEFTOUTER, fromExpr, fromVar, fromPosVar, condExpr);
                    newJoinClause.setSourceLocation(joinClauseSourceLoc);

                    fromExpr = rightExpr;
                    fromVar = rightVar;
                    fromPosVar = rightPosVar;
                    correlateClauses.set(i, newJoinClause);
                } else {
                    VarIdentifier newRightVar = context.newVariable();

                    substMapOuterTmp.clear();
                    substMapInner.clear();
                    List<Projection> projectList = new ArrayList<>();

                    SourceLocation fromVarSourceLoc = fromVar.getSourceLocation();
                    VarIdentifier newFromVar = context.newVariable();
                    String newFromVarFieldName = generateFieldName();
                    projectList.add(createProjection(newVariableExpr(newFromVar, fromVarSourceLoc), newFromVarFieldName,
                            fromVarSourceLoc));
                    substMapOuterTmp.put(fromVar, SqlppRewriteUtil
                            .getFieldByName(newVariableExpr(newRightVar, fromVarSourceLoc), newFromVarFieldName));
                    substMapInner.put(fromVar, newVariableExpr(newFromVar, fromVarSourceLoc));

                    VarIdentifier newFromPosVar = null;
                    if (fromPosVar != null) {
                        SourceLocation fromPosVarSourceLoc = fromPosVar.getSourceLocation();
                        newFromPosVar = context.newVariable();
                        String newFromPosVarFieldName = generateFieldName();
                        projectList.add(createProjection(newVariableExpr(newFromPosVar, fromPosVarSourceLoc),
                                newFromPosVarFieldName, fromPosVarSourceLoc));
                        substMapOuterTmp.put(fromPosVar, SqlppRewriteUtil.getFieldByName(
                                newVariableExpr(newRightVar, fromPosVarSourceLoc), newFromPosVarFieldName));
                        substMapInner.put(fromPosVar, newVariableExpr(newFromPosVar, fromPosVarSourceLoc));
                    }

                    List<AbstractBinaryCorrelateClause> newPrecedingClauseList = new ArrayList<>(i);
                    for (int j = 0; j < i; j++) {
                        AbstractBinaryCorrelateClause precedingClause = correlateClauses.get(j);
                        SourceLocation precedingClauseSourceLoc = precedingClause.getSourceLocation();

                        VariableExpr precedingClauseRightVar = precedingClause.getRightVariable();
                        SourceLocation precedingClauseRightVarSourceLoc = precedingClauseRightVar.getSourceLocation();
                        VarIdentifier newPrecedingClauseRightVar = context.newVariable();
                        String newPrecedingClauseRightVarFieldName = generateFieldName();
                        projectList.add(createProjection(
                                newVariableExpr(newPrecedingClauseRightVar, precedingClauseRightVarSourceLoc),
                                newPrecedingClauseRightVarFieldName, precedingClauseRightVarSourceLoc));
                        substMapOuterTmp.put(precedingClauseRightVar,
                                SqlppRewriteUtil.getFieldByName(
                                        newVariableExpr(newRightVar, precedingClauseRightVarSourceLoc),
                                        newPrecedingClauseRightVarFieldName));
                        substMapInner.put(precedingClauseRightVar,
                                newVariableExpr(newPrecedingClauseRightVar, precedingClauseRightVarSourceLoc));

                        VariableExpr precedingClauseRightPosVar = precedingClause.getPositionalVariable();
                        SourceLocation precedingClauseRightPosVarSourceLoc = null;
                        VarIdentifier newPrecedingClauseRightPosVar = null;
                        if (precedingClauseRightPosVar != null) {
                            precedingClauseRightPosVarSourceLoc = precedingClauseRightPosVar.getSourceLocation();
                            newPrecedingClauseRightPosVar = context.newVariable();
                            String newPrecedingClauseRightPosVarFieldName = generateFieldName();
                            projectList.add(createProjection(
                                    newVariableExpr(newPrecedingClauseRightPosVar, precedingClauseRightPosVarSourceLoc),
                                    newPrecedingClauseRightPosVarFieldName, precedingClauseRightPosVarSourceLoc));
                            substMapOuterTmp.put(precedingClauseRightPosVar,
                                    SqlppRewriteUtil.getFieldByName(
                                            newVariableExpr(newRightVar, precedingClauseRightPosVarSourceLoc),
                                            newPrecedingClauseRightPosVarFieldName));
                            substMapInner.put(precedingClauseRightPosVar, newVariableExpr(newPrecedingClauseRightPosVar,
                                    precedingClauseRightPosVarSourceLoc));
                        }

                        AbstractBinaryCorrelateClause newPrecedingClause;
                        switch (precedingClause.getClauseType()) {
                            case JOIN_CLAUSE:
                                JoinClause joinPrecedingClause = (JoinClause) precedingClause;
                                Expression newCondExpr = (Expression) SqlppRewriteUtil
                                        .deepCopy(joinPrecedingClause.getConditionExpression());
                                SqlppRewriteUtil.substituteExpression(newCondExpr, substMapInner, context);
                                newPrecedingClause = new JoinClause(joinPrecedingClause.getJoinType(),
                                        joinPrecedingClause.getRightExpression(),
                                        newVariableExpr(newPrecedingClauseRightVar, precedingClauseRightVarSourceLoc),
                                        newPrecedingClauseRightPosVar != null
                                                ? newVariableExpr(newPrecedingClauseRightPosVar,
                                                        precedingClauseRightPosVarSourceLoc)
                                                : null,
                                        newCondExpr);
                                newPrecedingClause.setSourceLocation(precedingClauseSourceLoc);
                                break;
                            case UNNEST_CLAUSE:
                                UnnestClause unnestPrecedingClause = (UnnestClause) precedingClause;
                                Expression newRightExpr = (Expression) SqlppRewriteUtil
                                        .deepCopy(unnestPrecedingClause.getRightExpression());
                                SqlppRewriteUtil.substituteExpression(newRightExpr, substMapInner, context);
                                newPrecedingClause = new UnnestClause(unnestPrecedingClause.getUnnestType(),
                                        newRightExpr,
                                        newVariableExpr(newPrecedingClauseRightVar, precedingClauseRightVarSourceLoc),
                                        newPrecedingClauseRightPosVar != null
                                                ? newVariableExpr(newPrecedingClauseRightPosVar,
                                                        precedingClauseRightPosVarSourceLoc)
                                                : null);
                                newPrecedingClause.setSourceLocation(precedingClauseSourceLoc);
                                break;
                            default:
                                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                        String.valueOf(precedingClause.getClauseType()));
                        }
                        newPrecedingClauseList.add(newPrecedingClause);
                    }

                    Expression newRightExpr = createRightSelectExpression(fromExpr, newFromVar, newFromPosVar,
                            newPrecedingClauseList, projectList, joinClauseSourceLoc);

                    VariableExpr newRightVarExpr = newVariableExpr(newRightVar, rightVar.getSourceLocation());
                    newRightVarExpr.addHint(ExcludeFromSelectStarAnnotation.INSTANCE);

                    // compose substitution maps
                    for (VariableExpr varExpr : substMapOuterFinal.keySet()) {
                        Expression substExpr = substMapOuterFinal.get(varExpr);
                        Expression newSubstExpr =
                                SqlppRewriteUtil.substituteExpression(substExpr, substMapOuterTmp, context);
                        substMapOuterFinal.put(varExpr, newSubstExpr);
                    }
                    substMapOuterFinal.putAll(substMapOuterTmp);

                    Expression newCondExpr = SqlppRewriteUtil.substituteExpression(
                            (Expression) SqlppRewriteUtil.deepCopy(condExpr), substMapOuterFinal, context);

                    JoinClause newJoinClause =
                            new JoinClause(JoinType.LEFTOUTER, newRightExpr, newRightVarExpr, null, newCondExpr);
                    newJoinClause.setSourceLocation(joinClauseSourceLoc);

                    fromExpr = rightExpr;
                    fromVar = rightVar;
                    fromPosVar = rightPosVar;

                    correlateClauses.subList(0, i).clear();
                    correlateClauses.set(0, newJoinClause);

                    i = 0;
                }
            } else if (!substMapOuterFinal.isEmpty()) {
                switch (correlateClause.getClauseType()) {
                    case JOIN_CLAUSE:
                        AbstractBinaryCorrelateWithConditionClause correlateConditionClause =
                                (AbstractBinaryCorrelateWithConditionClause) correlateClause;
                        Expression condExpr = correlateConditionClause.getConditionExpression();
                        Expression newCondExpr = SqlppRewriteUtil.substituteExpression(
                                (Expression) SqlppRewriteUtil.deepCopy(condExpr), substMapOuterFinal, context);
                        correlateConditionClause.setConditionExpression(newCondExpr);
                        // fall thru to UNNEST_CLAUSE
                    case UNNEST_CLAUSE:
                        Expression rightExpr = correlateClause.getRightExpression();
                        Expression newRightExpr = SqlppRewriteUtil.substituteExpression(
                                (Expression) SqlppRewriteUtil.deepCopy(rightExpr), substMapOuterFinal, context);
                        correlateClause.setRightExpression(newRightExpr);
                        break;
                    default:
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                correlateClause.getSourceLocation(), String.valueOf(correlateClause.getClauseType()));
                }

            }
        }

        FromTerm newFromTerm = new FromTerm(fromExpr, fromVar, fromPosVar, correlateClauses);
        newFromTerm.setSourceLocation(fromTerm.getSourceLocation());

        List<LetClause> newLetClauses = new ArrayList<>(substMapOuterFinal.size());
        for (Map.Entry<VariableExpr, Expression> me : substMapOuterFinal.entrySet()) {
            VariableExpr newVarExpr = (VariableExpr) SqlppRewriteUtil.deepCopy(me.getKey());
            Expression newValueExpr = (Expression) SqlppRewriteUtil.deepCopy(me.getValue());
            LetClause newLetClause = new LetClause(newVarExpr, newValueExpr);
            newLetClause.setSourceLocation(newVarExpr.getSourceLocation());
            newLetClauses.add(newLetClause);
        }

        return new Pair<>(newFromTerm, newLetClauses);
    }

    private Expression createRightSelectExpression(Expression fromExpr, VarIdentifier fromVar, VarIdentifier fromPosVar,
            List<AbstractBinaryCorrelateClause> correlateClauseList, List<Projection> projectList,
            SourceLocation sourceLoc) {
        FromTerm newFromTerm = new FromTerm(fromExpr, newVariableExpr(fromVar, sourceLoc),
                fromPosVar != null ? newVariableExpr(fromPosVar, sourceLoc) : null, correlateClauseList);
        List<FromTerm> newFromTermList = new ArrayList<>(1);
        newFromTermList.add(newFromTerm);
        FromClause newFromClause = new FromClause(newFromTermList);
        newFromClause.setSourceLocation(sourceLoc);
        SelectClause newSelectClause = new SelectClause(null, new SelectRegular(projectList), false);
        newSelectClause.setSourceLocation(sourceLoc);
        SelectBlock newSelectBlock = new SelectBlock(newSelectClause, newFromClause, null, null, null);
        newSelectBlock.setSourceLocation(sourceLoc);
        SelectSetOperation newSelectSetOp = new SelectSetOperation(new SetOperationInput(newSelectBlock, null), null);
        newSelectSetOp.setSourceLocation(sourceLoc);
        SelectExpression newSelectExpr = new SelectExpression(null, newSelectSetOp, null, null, true);
        newSelectExpr.setSourceLocation(sourceLoc);
        return newSelectExpr;
    }

    private VariableExpr newVariableExpr(VarIdentifier newFromVar, SourceLocation sourceLoc) {
        VariableExpr varExpr = new VariableExpr(newFromVar);
        varExpr.setSourceLocation(sourceLoc);
        return varExpr;
    }

    private Projection createProjection(VariableExpr var, String fieldName, SourceLocation sourceLoc) {
        Projection projection = new Projection(newVariableExpr(var.getVar(), null), fieldName, false, false);
        projection.setSourceLocation(sourceLoc);
        return projection;
    }

    private String generateFieldName() {
        return SqlppVariableUtil.variableNameToDisplayedFieldName(context.newVariable().getValue());
    }
}
