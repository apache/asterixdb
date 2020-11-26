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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
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
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteExpressionVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Rewrites GROUP BY clauses with multiple grouping sets into UNION ALL.
 * Also rewrites valid GROUPING(...) operations into constants.
 */
public final class SqlppGroupingSetsVisitor extends AbstractSqlppExpressionScopingVisitor {

    private final List<List<GbyVariableExpressionPair>> tmpGroupingSets = new ArrayList<>(1);

    private final List<GbyVariableExpressionPair> tmpDecorPairList = new ArrayList<>();

    private final Set<VariableExpr> tmpAllGroupingSetsVars = new LinkedHashSet<>();

    private final Set<VariableExpr> tmpCurrentGroupingSetVars = new LinkedHashSet<>();

    public SqlppGroupingSetsVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectSetOperation setOp, ILangExpression arg) throws CompilationException {
        super.visit(setOp, arg);

        SetOperationInput setOpInputLeft = setOp.getLeftInput();
        SelectBlock selectBlockLeft = setOpInputLeft.getSelectBlock();

        if (selectBlockLeft != null && selectBlockLeft.hasGroupbyClause()) {
            SelectExpression selectExpression = (SelectExpression) arg;
            boolean hasPostSetOpClauses = selectExpression.hasOrderby() || selectExpression.hasLimit();
            Map<VariableExpr, String> extraProjections = Collections.emptyMap();
            if (hasPostSetOpClauses && !setOp.hasRightInputs() && isMultiGroupingSets(selectBlockLeft)) {
                // We need to make sure that ORDER BY/LIMIT free vars are still available after UNION ALL rewriting.
                // To do that we expose these variables by adding them to SELECT so they can be used after UNION ALL.
                // Then we rewrite ORDERBY/LIMIT to replace these free vars with field accessors on top of SELECT.
                // Then we remove these extra projections from the final output.
                extraProjections = rewritePostSetOpClauses(selectExpression, selectBlockLeft);
            }

            SelectBlock newSelectBlockLeft = rewriteSelectBlock(selectBlockLeft, extraProjections);
            setOpInputLeft.setSelectBlock(newSelectBlockLeft);
        }

        if (setOp.hasRightInputs()) {
            for (SetOperationRight setOpRight : setOp.getRightInputs()) {
                SetOperationInput setOpInputRight = setOpRight.getSetOperationRightInput();
                SelectBlock selectBlockRight = setOpInputRight.getSelectBlock();
                if (selectBlockRight != null && selectBlockRight.hasGroupbyClause()) {
                    SelectBlock newSelectBlockRight = rewriteSelectBlock(selectBlockRight, Collections.emptyMap());
                    setOpInputRight.setSelectBlock(newSelectBlockRight);
                }
            }
        }

        return null;
    }

    private Map<VariableExpr, String> rewritePostSetOpClauses(SelectExpression selectExpression,
            SelectBlock selectBlockLeft) throws CompilationException {
        // 1. Get free variables in ORDERBY/LIMIT
        Set<VariableExpr> freeVarsPostSetOp = getFreeVarsPostSetOp(selectExpression);
        if (freeVarsPostSetOp.isEmpty()) {
            return Collections.emptyMap();
        }

        // 2. Compute substitution map for ORDERBY/LIMIT to replace free vars with field accessors
        Pair<Map<VariableExpr, VariableExpr>, Map<VariableExpr, String>> p =
                computePostSetOpSubstMap(selectBlockLeft, freeVarsPostSetOp);
        Map<VariableExpr, VariableExpr> freeVarsPostSetOpSubstMap = p.first;
        Map<VariableExpr, String> extraProjections = p.second;

        // 3. Rewrite ORDERBY/LIMIT using this substitution
        SqlppSubstituteExpressionVisitor substVisitor =
                new SqlppSubstituteExpressionVisitor(context, freeVarsPostSetOpSubstMap);
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(substVisitor, selectExpression);
        }
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(substVisitor, selectExpression);
        }

        // 4. return extra projections that should be added to SELECT
        return extraProjections;
    }

    private Pair<Map<VariableExpr, VariableExpr>, Map<VariableExpr, String>> computePostSetOpSubstMap(
            SelectBlock selectBlockLeft, Set<VariableExpr> freeVarsPostSetOp) throws CompilationException {
        SelectClause selectClause = selectBlockLeft.getSelectClause();
        if (selectClause.selectRegular()) {
            return computePostSetOpSubstMapForSelectRegular(freeVarsPostSetOp, selectBlockLeft);
        } else if (selectClause.selectElement()) {
            return computePostSetOpSubstMapForSelectElement(freeVarsPostSetOp);
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectBlockLeft.getSourceLocation(),
                    "");
        }
    }

    private Pair<Map<VariableExpr, VariableExpr>, Map<VariableExpr, String>> computePostSetOpSubstMapForSelectRegular(
            Set<VariableExpr> freeVarsPostSetOp, SelectBlock selectBlock) throws CompilationException {
        // For regular SELECT we need to add ORDERBY/LIMIT free variables to the projection list of the SELECT clause,
        // so they can be accessed using field-access-by-name after SELECT.
        // Some of these variables might be already projected by SELECT, so we need to account for that.
        // We currently do not support (fail) SELECT v.* because in this case we cannot statically compute
        // the schema of the record produced by the SELECT and therefore cannot guarantee that the field
        // names we generate will not conflict with the existing fields in the SELECT output.
        // The added projections will be later removed by the outer query.

        Map<VariableExpr, String> extraProjections = Collections.emptyMap();
        Map<VariableExpr, VariableExpr> freeVarsPostSetOpSubstMap = new HashMap<>();

        List<Projection> projectionList = selectBlock.getSelectClause().getSelectRegular().getProjections();
        List<VariableExpr> gbyBindingVars = null, postGbyBindingVars = null;
        for (VariableExpr freeVarPostSetOp : freeVarsPostSetOp) {
            String projectionName = null;
            for (int i = projectionList.size() - 1; i >= 0; i--) {
                Projection projection = projectionList.get(i);
                if (projection.varStar()) {
                    throw new CompilationException(ErrorCode.UNSUPPORTED_GBY_OBY_SELECT_COMBO,
                            selectBlock.getSourceLocation());
                } else if (projection.star()) {
                    if (gbyBindingVars == null) {
                        gbyBindingVars = SqlppVariableUtil.getBindingVariables(selectBlock.getGroupbyClause());
                    }
                    if (postGbyBindingVars == null) {
                        postGbyBindingVars = selectBlock.hasLetHavingClausesAfterGroupby()
                                ? SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetHavingListAfterGroupby())
                                : Collections.emptyList();
                    }
                    if (gbyBindingVars.contains(freeVarPostSetOp) || postGbyBindingVars.contains(freeVarPostSetOp)) {
                        projectionName = SqlppVariableUtil
                                .variableNameToDisplayedFieldName(freeVarPostSetOp.getVar().getValue());
                        break;
                    }
                } else if (projection.hasName()) {
                    if (projection.getExpression().equals(freeVarPostSetOp)) {
                        projectionName = projection.getName();
                        break;
                    }
                } else {
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectBlock.getSourceLocation(),
                            "");
                }
            }

            boolean newProjection = false;
            if (projectionName == null) {
                newProjection = true;
                projectionName = generateProjectionName(projectionList);
            }

            VariableExpr substExpr = new VariableExpr(SqlppVariableUtil.toInternalVariableIdentifier(projectionName));
            substExpr.setSourceLocation(freeVarPostSetOp.getSourceLocation());
            freeVarsPostSetOpSubstMap.put(freeVarPostSetOp, substExpr);

            if (newProjection) {
                if (extraProjections.isEmpty()) {
                    extraProjections = new LinkedHashMap<>();
                }
                extraProjections.put(freeVarPostSetOp, projectionName);
            }
        }

        return new Pair<>(freeVarsPostSetOpSubstMap, extraProjections);
    }

    private Pair<Map<VariableExpr, VariableExpr>, Map<VariableExpr, String>> computePostSetOpSubstMapForSelectElement(
            Set<VariableExpr> freeVarsPostSetOp) {
        // For SELECT VALUE we first need to convert it to the regular SELECT, so we can then
        // add ORDERBY/LIMIT free variables to the projection list of this regular SELECT clause.
        // They can be accessed using field-access-by-name after SELECT.
        // SELECT VALUE to regular conversion will be reversed later.
        Map<VariableExpr, VariableExpr> freeVarsPostSetOpSubstMap = new HashMap<>();
        Map<VariableExpr, String> extraProjections = new LinkedHashMap<>();
        for (VariableExpr freeVarPostSelect : freeVarsPostSetOp) {
            String projectionName =
                    SqlppVariableUtil.variableNameToDisplayedFieldName(context.newVariable().getValue());
            VariableExpr substExpr = new VariableExpr(SqlppVariableUtil.toInternalVariableIdentifier(projectionName));
            substExpr.setSourceLocation(freeVarPostSelect.getSourceLocation());
            freeVarsPostSetOpSubstMap.put(freeVarPostSelect, substExpr);
            extraProjections.put(freeVarPostSelect, projectionName);
        }

        return new Pair<>(freeVarsPostSetOpSubstMap, extraProjections);
    }

    private Set<VariableExpr> getFreeVarsPostSetOp(SelectExpression selectExpression) throws CompilationException {
        if (selectExpression.hasOrderby() || selectExpression.hasLimit()) {
            Set<VariableExpr> freeVars = new ListSet<>();
            FreeVariableVisitor freeVarsVisitor = new FreeVariableVisitor();
            if (selectExpression.hasOrderby()) {
                selectExpression.getOrderbyClause().accept(freeVarsVisitor, freeVars);
            }
            if (selectExpression.hasLimit()) {
                selectExpression.getLimitClause().accept(freeVarsVisitor, freeVars);
            }
            return freeVars;
        } else {
            return Collections.emptySet();
        }
    }

    private boolean isMultiGroupingSets(SelectBlock selectBlock) {
        return selectBlock.getGroupbyClause().getGbyPairList().size() > 1;
    }

    private SelectBlock rewriteSelectBlock(SelectBlock selectBlock, Map<VariableExpr, String> extraProjections)
            throws CompilationException {
        if (isMultiGroupingSets(selectBlock)) {
            return rewriteMultipleGroupingSets(selectBlock, extraProjections);
        } else {
            if (!extraProjections.isEmpty()) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectBlock.getSourceLocation(),
                        extraProjections.toString());
            }
            return rewriteZeroOrOneGroupingSet(selectBlock);
        }
    }

    private SelectBlock rewriteZeroOrOneGroupingSet(SelectBlock selectBlock) throws CompilationException {
        // no UNION ALL if there's only one grouping set (or zero), but we still need to rewrite GROUPING(..) operations
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

    private SelectBlock rewriteMultipleGroupingSets(SelectBlock selectBlock, Map<VariableExpr, String> extraProjections)
            throws CompilationException {
        GroupbyClause gby = selectBlock.getGroupbyClause();
        List<List<GbyVariableExpressionPair>> groupingSets = gby.getGbyPairList();
        int nGroupingSets = groupingSets.size();
        if (nGroupingSets <= 1 || !gby.getDecorPairList().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, gby.getSourceLocation(), "");
        }

        SelectClause selectClause = selectBlock.getSelectClause();
        boolean distinct = selectClause.distinct();
        boolean selectElement = selectClause.selectElement();
        boolean selectElementConvertToRegular = selectElement && !extraProjections.isEmpty();

        // If we're converting SELECT VALUE to regular SELECT then we need to generate a field name
        // that'll hold SELECT VALUE expression after the conversion.
        // SELECT VALUE expr -> SELECT expr AS main_projection_name
        String selectElementMainProjectionName = selectElementConvertToRegular
                ? SqlppVariableUtil.variableNameToDisplayedFieldName(context.newVariable().getValue()) : null;

        tmpAllGroupingSetsVars.clear();
        getAllGroupingSetsVars(groupingSets, tmpAllGroupingSetsVars);

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
            computeDecorVars(tmpCurrentGroupingSetVars, tmpAllGroupingSetsVars, tmpDecorPairList);
            gby.setDecorPairList(tmpDecorPairList); // will be cloned by deepCopy() below

            SelectBlock newSelectBlock = (SelectBlock) SqlppRewriteUtil.deepCopy(selectBlock);
            rewriteGroupingOperations(newSelectBlock, tmpCurrentGroupingSetVars, tmpAllGroupingSetsVars);
            rewriteSelectClause(newSelectBlock, extraProjections, selectElementMainProjectionName);

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
        computeDecorVars(tmpCurrentGroupingSetVars, tmpAllGroupingSetsVars, newDecorPairList);
        gby.setDecorPairList(newDecorPairList);

        rewriteGroupingOperations(selectBlock, tmpCurrentGroupingSetVars, tmpAllGroupingSetsVars);
        rewriteSelectClause(selectBlock, extraProjections, selectElementMainProjectionName);

        SetOperationInput newSetOpInput = new SetOperationInput(selectBlock, null);

        SelectSetOperation newSetOp = new SelectSetOperation(newSetOpInput, newSetOpRightInputs);
        newSetOp.setSourceLocation(selectBlock.getSourceLocation());

        SelectExpression newSelectExpr = new SelectExpression(null, newSetOp, null, null, true);
        newSelectExpr.setSourceLocation(selectBlock.getSourceLocation());

        return selectElement
                ? SetOperationVisitor.createSelectBlock(newSelectExpr, distinct,
                        selectElementConvertToRegular ? SqlppRewriteUtil::getFieldByName : null,
                        selectElementConvertToRegular ? selectElementMainProjectionName : null, context)
                : SetOperationVisitor.createSelectBlock(newSelectExpr, distinct,
                        SqlppGroupingSetsVisitor::removeFieldsByName, extraProjections.values(), context);
    }

    private static void rewriteSelectClause(SelectBlock selectBlock, Map<VariableExpr, String> extraProjections,
            String selectElementMainProjectionName) throws CompilationException {
        SelectClause selectClause = selectBlock.getSelectClause();
        // unset distinct. we'll have it in the outer select if needed
        selectClause.setDistinct(false);
        if (!extraProjections.isEmpty()) {
            if (selectClause.selectElement()) {
                selectClause = convertSelectElementToRegular(selectClause, selectElementMainProjectionName);
            }
            insertExtraProjections(selectClause, extraProjections);
        }
        selectBlock.setSelectClause(selectClause);
    }

    private static SelectClause convertSelectElementToRegular(SelectClause selectClause, String mainProjectionName)
            throws CompilationException {
        if (!selectClause.selectElement() || selectClause.distinct()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectClause.getSourceLocation(), "");
        }
        SelectElement selectElement = selectClause.getSelectElement();
        List<Projection> projectionList = new ArrayList<>(1);
        projectionList.add(new Projection(selectElement.getExpression(), mainProjectionName, false, false));
        SelectRegular newSelectRegular = new SelectRegular(projectionList);
        newSelectRegular.setSourceLocation(selectElement.getSourceLocation());
        SelectClause newSelectClause = new SelectClause(null, newSelectRegular, selectClause.distinct());
        newSelectClause.setSourceLocation(selectClause.getSourceLocation());
        return newSelectClause;
    }

    private static void insertExtraProjections(SelectClause selectClause, Map<VariableExpr, String> projections)
            throws CompilationException {
        if (!selectClause.selectRegular()) {
            // "SELECT VALUE expr" should've been rewritten earlier into "SELECT expr AS $n"
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, selectClause.getSourceLocation(), "");
        }
        SelectRegular selectRegular = selectClause.getSelectRegular();
        for (Map.Entry<VariableExpr, String> me : projections.entrySet()) {
            Projection newProjection =
                    new Projection((VariableExpr) SqlppRewriteUtil.deepCopy(me.getKey()), me.getValue(), false, false);
            selectRegular.getProjections().add(newProjection);
        }
    }

    private static Expression removeFieldsByName(Expression inExpr, Collection<String> fieldNames) {
        //TODO(dmitry): need to enhance RECORD_REMOVE to accept an array of field names the should be removed.
        //Until that's implemented we can only remove them one by one.
        Expression resultExpr = inExpr;
        for (String fieldName : fieldNames) {
            LiteralExpr fieldNameExpr = new LiteralExpr(new StringLiteral(fieldName));
            fieldNameExpr.setSourceLocation(resultExpr.getSourceLocation());
            List<Expression> argList = new ArrayList<>(2);
            argList.add(resultExpr);
            argList.add(fieldNameExpr);
            CallExpr callExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_REMOVE), argList);
            callExpr.setSourceLocation(resultExpr.getSourceLocation());
            resultExpr = callExpr;
        }
        return resultExpr;
    }

    private String generateProjectionName(List<Projection> projectionList) {
        String projectionName;
        do {
            projectionName = SqlppVariableUtil.variableNameToDisplayedFieldName(context.newVariable().getValue());
        } while (findProjectionByName(projectionList, projectionName) != null);
        return projectionName;
    }

    private static Projection findProjectionByName(List<Projection> projectionList, String name) {
        for (Projection projection : projectionList) {
            if (projection.hasName() && projection.getName().equals(name)) {
                return projection;
            }
        }
        return null;
    }

    /**
     * Valid GROUPING() operations can only be in LET clauses after GROUP BY.
     * This is guaranteed by {@link SqlppGroupByVisitor}.
     * These operations a rewritten into constants by this method.
     * The remaining GROUPING() operations are invalid and will lead to a compile-time failure later
     * because there's no runtime for GROUPING() function.
     */
    private static void rewriteGroupingOperations(SelectBlock selectBlock, Set<VariableExpr> currentGroupingSetVars,
            Set<VariableExpr> allGroupingSetsVars) throws CompilationException {
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            for (Clause clause : selectBlock.getLetHavingListAfterGroupby()) {
                if (clause.getClauseType() == Clause.ClauseType.LET_CLAUSE) {
                    LetClause letClause = (LetClause) clause;
                    Expression letExpr = letClause.getBindingExpr();
                    if (SqlppGroupByVisitor.isGroupingOperation(letExpr)) {
                        Expression newLetExpr = rewriteGroupingOperation((CallExpr) letExpr, currentGroupingSetVars,
                                allGroupingSetsVars);
                        letClause.setBindingExpr(newLetExpr);
                    }
                }
            }
        }
    }

    private static Expression rewriteGroupingOperation(CallExpr callExpr, Set<VariableExpr> currentGroupingSetVars,
            Set<VariableExpr> allGroupingSetsVars) throws CompilationException {
        List<Expression> argList = callExpr.getExprList();
        if (argList.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, callExpr.getSourceLocation(),
                    BuiltinFunctions.GROUPING.getName());
        }
        if (callExpr.hasAggregateFilterExpr()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_USE_OF_FILTER_CLAUSE,
                    callExpr.getSourceLocation());
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

    private static void computeDecorVars(Set<VariableExpr> currentGroupingSetVars,
            Set<VariableExpr> allGroupingSetsVars, List<GbyVariableExpressionPair> outDecorPairList) {
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
