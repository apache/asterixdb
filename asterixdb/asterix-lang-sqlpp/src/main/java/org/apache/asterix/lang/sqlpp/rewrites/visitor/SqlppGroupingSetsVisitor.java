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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.LongIntegerLiteral;
import org.apache.asterix.lang.common.literal.NullLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Rewrites GROUP BY clauses with multiple grouping sets into UNION ALL.
 * Also rewrites valid GROUPING(...) operations into constants.
 */
public final class SqlppGroupingSetsVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    private final List<List<GbyVariableExpressionPair>> tmpGroupingSets = new ArrayList<>(1);

    private final List<GbyVariableExpressionPair> tmpDecorPairList = new ArrayList<>();

    private final Set<VariableExpr> tmpAllGroupingSetsVars = new LinkedHashSet<>();

    private final Set<VariableExpr> tmpCurrentGroupingSetVars = new LinkedHashSet<>();

    public SqlppGroupingSetsVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(SelectSetOperation setOp, ILangExpression arg) throws CompilationException {
        super.visit(setOp, arg);

        SetOperationInput setOpInputLeft = setOp.getLeftInput();
        SelectBlock selectBlockLeft = setOpInputLeft.getSelectBlock();
        if (selectBlockLeft != null && selectBlockLeft.hasGroupbyClause()) {
            setOpInputLeft.setSelectBlock(rewriteSelectBlock(selectBlockLeft));
        }
        if (setOp.hasRightInputs()) {
            for (SetOperationRight setOpRight : setOp.getRightInputs()) {
                SetOperationInput setOpInputRight = setOpRight.getSetOperationRightInput();
                SelectBlock selectBlockRight = setOpInputRight.getSelectBlock();
                if (selectBlockRight != null && selectBlockRight.hasGroupbyClause()) {
                    setOpInputRight.setSelectBlock(rewriteSelectBlock(selectBlockRight));
                }
            }
        }

        return null;
    }

    private SelectBlock rewriteSelectBlock(SelectBlock selectBlock) throws CompilationException {
        return selectBlock.getGroupbyClause().getGbyPairList().size() <= 1 ? rewriteZeroOrOneGroupingSet(selectBlock)
                : rewriteMultipleGroupingSets(selectBlock);
    }

    private SelectBlock rewriteZeroOrOneGroupingSet(SelectBlock selectBlock) throws CompilationException {
        // no UNION ALL, we only need to rewrite GROUPING(..) operations
        GroupbyClause gby = selectBlock.getGroupbyClause();
        List<List<GbyVariableExpressionPair>> groupingSets = gby.getGbyPairList();
        if (groupingSets.size() > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, gby.getSourceLocation(), "");
        }
        tmpAllGroupingSetsVars.clear();
        getAllGroupingSetsVars(groupingSets, tmpAllGroupingSetsVars);
        rewriteGroupingOperations(selectBlock, tmpAllGroupingSetsVars, tmpAllGroupingSetsVars);
        return selectBlock;
    }

    private SelectBlock rewriteMultipleGroupingSets(SelectBlock selectBlock) throws CompilationException {
        GroupbyClause gby = selectBlock.getGroupbyClause();
        List<List<GbyVariableExpressionPair>> groupingSets = gby.getGbyPairList();
        if (groupingSets.size() <= 1 || !gby.getDecorPairList().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, gby.getSourceLocation(), "");
        }

        tmpAllGroupingSetsVars.clear();
        getAllGroupingSetsVars(groupingSets, tmpAllGroupingSetsVars);

        int nGroupingSets = groupingSets.size();
        List<SetOperationRight> newSetOpRightInputs = new ArrayList<>(nGroupingSets - 1);

        // process 2nd and following grouping sets

        for (int i = 1; i < nGroupingSets; i++) {
            List<GbyVariableExpressionPair> groupingSet = groupingSets.get(i);

            tmpCurrentGroupingSetVars.clear();
            getGroupingSetVars(groupingSet, tmpCurrentGroupingSetVars);

            tmpGroupingSets.clear();
            tmpGroupingSets.add(groupingSet);
            gby.setGbyPairList(tmpGroupingSets); // will be cloned by deepCopy() below

            tmpDecorPairList.clear();
            computeDecorVars(tmpAllGroupingSetsVars, tmpCurrentGroupingSetVars, tmpDecorPairList);
            gby.setDecorPairList(tmpDecorPairList); // will be cloned by deepCopy() below

            SelectBlock newSelectBlock = (SelectBlock) SqlppRewriteUtil.deepCopy(selectBlock);

            rewriteGroupingOperations(newSelectBlock, tmpAllGroupingSetsVars, tmpCurrentGroupingSetVars);

            SetOperationRight newSetOpRight =
                    new SetOperationRight(SetOpType.UNION, false, new SetOperationInput(newSelectBlock, null));
            newSetOpRightInputs.add(newSetOpRight);
        }

        // process 1st grouping set

        List<GbyVariableExpressionPair> groupingSet = groupingSets.get(0);
        gby.setGbyPairList(Collections.singletonList(groupingSet));

        tmpCurrentGroupingSetVars.clear();
        getGroupingSetVars(groupingSet, tmpCurrentGroupingSetVars);

        List<GbyVariableExpressionPair> newDecorPairList = new ArrayList<>();
        computeDecorVars(tmpAllGroupingSetsVars, tmpCurrentGroupingSetVars, newDecorPairList);
        gby.setDecorPairList(newDecorPairList);

        rewriteGroupingOperations(selectBlock, tmpAllGroupingSetsVars, tmpCurrentGroupingSetVars);

        SetOperationInput newSetOpInput = new SetOperationInput(selectBlock, null);

        SelectSetOperation newSetOp = new SelectSetOperation(newSetOpInput, newSetOpRightInputs);
        newSetOp.setSourceLocation(selectBlock.getSourceLocation());

        SelectExpression newSelectExpr = new SelectExpression(null, newSetOp, null, null, true);
        newSelectExpr.setSourceLocation(selectBlock.getSourceLocation());

        return SetOperationVisitor.createSelectBlock(newSelectExpr, context);
    }

    /**
     * Valid GROUPING() operations can only be in LET clauses after GROUP BY.
     * This is guaranteed by {@link SqlppGroupByVisitor}.
     * These operations a rewritten into constants by this method.
     * The remaining GROUPING() operations are invalid and will lead to a compile-time failure later
     * because there's no runtime for GROUPING() function.
     */
    private void rewriteGroupingOperations(SelectBlock selectBlock, Set<VariableExpr> allGroupingSetsVars,
            Set<VariableExpr> currentGroupingSetVars) throws CompilationException {
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            for (Clause clause : selectBlock.getLetHavingListAfterGroupby()) {
                if (clause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                    LetClause letClause = (LetClause) clause;
                    Expression letExpr = letClause.getBindingExpr();
                    if (SqlppGroupByVisitor.isGroupingOperation(letExpr)) {
                        Expression newLetExpr = rewriteGroupingOperation((CallExpr) letExpr, allGroupingSetsVars,
                                currentGroupingSetVars);
                        letClause.setBindingExpr(newLetExpr);
                    }
                }
            }
        }
    }

    private Expression rewriteGroupingOperation(CallExpr callExpr, Set<VariableExpr> allGroupingSetsVars,
            Set<VariableExpr> currentGroupingSetVars) throws CompilationException {
        List<Expression> argList = callExpr.getExprList();
        if (argList.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, callExpr.getSourceLocation(),
                    BuiltinFunctions.GROUPING.getName());
        }
        long result = 0;
        for (Expression argExpr : argList) {
            int v;
            if (argExpr.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                VariableExpr varExpr = (VariableExpr) argExpr;
                if (currentGroupingSetVars.contains(varExpr)) {
                    v = 0;
                } else if (allGroupingSetsVars.contains(varExpr)) {
                    v = 1;
                } else {
                    throw new CompilationException(ErrorCode.COMPILATION_GROUPING_OPERATION_INVALID_ARG,
                            argExpr.getSourceLocation());
                }
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_GROUPING_OPERATION_INVALID_ARG,
                        argExpr.getSourceLocation());
            }
            result = (result << 1) + v;
        }

        LiteralExpr resultExpr = new LiteralExpr(new LongIntegerLiteral(result));
        resultExpr.setSourceLocation(callExpr.getSourceLocation());
        return resultExpr;
    }

    private static void getAllGroupingSetsVars(List<List<GbyVariableExpressionPair>> gbyList,
            Set<VariableExpr> outVars) {
        for (List<GbyVariableExpressionPair> gbyPairList : gbyList) {
            getGroupingSetVars(gbyPairList, outVars);
        }
    }

    private static void getGroupingSetVars(List<GbyVariableExpressionPair> groupingSet,
            Collection<VariableExpr> outVars) {
        for (GbyVariableExpressionPair gbyPair : groupingSet) {
            outVars.add(gbyPair.getVar());
        }
    }

    private static void computeDecorVars(Set<VariableExpr> allGroupingSetsVars,
            Set<VariableExpr> currentGroupingSetVars, List<GbyVariableExpressionPair> outDecorPairList) {
        for (VariableExpr var : allGroupingSetsVars) {
            if (!currentGroupingSetVars.contains(var)) {
                LiteralExpr nullExpr = new LiteralExpr(NullLiteral.INSTANCE);
                nullExpr.setSourceLocation(var.getSourceLocation());
                VariableExpr newDecorVarExpr = new VariableExpr(var.getVar());
                newDecorVarExpr.setSourceLocation(var.getSourceLocation());
                outDecorPairList.add(new GbyVariableExpressionPair(newDecorVarExpr, nullExpr));
            }
        }
    }
}
