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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.asterix.metadata.utils.PushdownUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.BTreeAccessMethod;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

public class AdvisorConditionParser {
    private static class ExprRef {
        private final ILogicalExpression expr;
        private final ILogicalOperator op;

        public ExprRef(Mutable<ILogicalExpression> expr, ILogicalOperator op) {
            this.expr = expr.get().cloneExpression();
            this.op = op;
        }

        public ILogicalExpression getExpr() {
            return expr;
        }

        public ILogicalOperator getOp() {
            return op;
        }
    }

    public static ScanFilter parseScanNode(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        List<ScanFilterCondition> primitiveFilterConditions = extractPrimitiveConditions(op, context);
        List<UnnestFilterCondition> unnestFilterConditions = extractUnnestConditions(op, context);
        return new ScanFilter(primitiveFilterConditions, unnestFilterConditions);
    }

    private static void accumlateSubplanOps(ILogicalOperator op, List<SubplanOperator> subplanOps) {
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            subplanOps.add((SubplanOperator) op);
        }
    }

    private static void accumulateFilterExprRefsFromOp(ILogicalOperator op, List<ExprRef> filterExprRefs) {
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator selectOp = (SelectOperator) op;
            ILogicalExpression condition = selectOp.getCondition().getValue();
            List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
            if (condition.splitIntoConjuncts(conjs)) {
                filterExprRefs.addAll(conjs.stream().map(expr -> new ExprRef(expr, selectOp)).toList());
            } else {
                filterExprRefs.add(new ExprRef(selectOp.getCondition(), selectOp));
            }
        }
    }

    private static void preOrderVisitNestedOps(ILogicalOperator op, Function<ILogicalOperator, Void> func) {
        func.apply(op);
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            SubplanOperator subplan = (SubplanOperator) op;
            subplan.getNestedPlans().forEach(plan -> {
                for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                    preOrderVisitNestedOps(root.getValue(), func);
                }
            });
        }
        for (Mutable<ILogicalOperator> root : op.getInputs()) {
            preOrderVisitNestedOps(root.getValue(), func);
        }
    }

    private static List<UnnestFilterCondition> extractUnnestConditions(ILogicalOperator op,
            IOptimizationContext context) throws AlgebricksException {
        List<ExprRef> filterExprRefs = new ArrayList<>();
        preOrderVisitNestedOps(op, (logicalOperator) -> {
            accumulateFilterExprRefsFromOp(logicalOperator, filterExprRefs);
            return null;
        });
        filterExprRefs.removeIf(exprRef -> !(exprRef.getExpr() instanceof AbstractFunctionCallExpression));

        Function<ILogicalOperator, Void> replaceFunc = (logicalOperator) -> {
            if (logicalOperator.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                replaceExprsWithAssign((AssignOperator) logicalOperator, filterExprRefs);
            } else if (logicalOperator.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                replaceExprsWithUnnest((UnnestOperator) logicalOperator, filterExprRefs);
            }
            return null;
        };
        preOrderVisitNestedOps(op, replaceFunc);

        List<UnnestFilterCondition> unnestFilterConditions = new ArrayList<>();

        for (ExprRef exprRef : filterExprRefs) {
            IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(exprRef.getOp(), context);
            UnnestFilterCondition unnestFilterCondition = parseUnnestCondition(exprRef.getExpr(), typeEnv);
            if (unnestFilterCondition != null) {
                unnestFilterConditions.add(unnestFilterCondition);
            }
        }

        List<SubplanOperator> subplanOps = new ArrayList<>();
        preOrderVisitNestedOps(op, (logicalOperator) -> {
            accumlateSubplanOps(logicalOperator, subplanOps);
            return null;
        });

        for (SubplanOperator subplanOp : subplanOps) {
            for (Mutable<ILogicalOperator> root : subplanOp.getNestedPlans().getFirst().getRoots()) {
                ILogicalOperator aggLogOp = root.get();
                if (aggLogOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                    continue;
                }
                AggregateOperator aggOp = (AggregateOperator) aggLogOp;
                for (ExprRef exprRef : filterExprRefs) {
                    if (!aggOp.hasInputs() || aggOp.getInputs().get(0).get() != exprRef.getOp()) {
                        continue;
                    }
                    ILogicalExpression expr = exprRef.getExpr();
                    if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                        continue;
                    }
                    ScalarFunctionCallExpression notFunction = (ScalarFunctionCallExpression) expr;
                    if (!notFunction.getFunctionIdentifier().equals(BuiltinFunctions.NOT)) {
                        continue;
                    }
                    ScalarFunctionCallExpression ifMissingOrNullFunction =
                            (ScalarFunctionCallExpression) notFunction.getArguments().get(0).getValue();
                    if (!ifMissingOrNullFunction.getFunctionIdentifier().equals(BuiltinFunctions.IF_MISSING_OR_NULL)) {
                        continue;
                    }

                    ILogicalExpression actualFilterExpr = ifMissingOrNullFunction.getArguments().get(0).getValue();
                    IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(exprRef.getOp(), context);

                    List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
                    if (!actualFilterExpr.splitIntoConjuncts(conjs)) {
                        conjs.add(new MutableObject<>(actualFilterExpr));
                    }

                    for (Mutable<ILogicalExpression> conjExpr : conjs) {
                        UnnestFilterCondition unnestFilterCondition = parseUnnestCondition(conjExpr.get(), typeEnv);
                        if (unnestFilterCondition != null) {
                            unnestFilterConditions.add(unnestFilterCondition);
                        }
                    }
                }
            }
        }
        return unnestFilterConditions;
    }

    public static List<ScanFilterCondition> extractPrimitiveConditions(ILogicalOperator op,
            IOptimizationContext context) throws AlgebricksException {
        List<ExprRef> filterExprRefs = new ArrayList<>();
        ILogicalOperator tempOp = op;
        do {
            accumulateFilterExprRefsFromOp(tempOp, filterExprRefs);
            tempOp = tempOp.getInputs().getFirst().getValue();
        } while (tempOp.hasInputs());
        filterExprRefs.removeIf(exprRef -> !(exprRef.getExpr() instanceof AbstractFunctionCallExpression));
        tempOp = op;
        do {
            if (tempOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                replaceExprsWithAssign((AssignOperator) tempOp, filterExprRefs);
            }
            tempOp = tempOp.getInputs().getFirst().getValue();
        } while (tempOp.hasInputs());
        List<ScanFilterCondition> primitiveFilterConditions = new ArrayList<>();
        for (ExprRef exprRef : filterExprRefs) {
            IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(exprRef.getOp(), context);
            ScanFilterCondition primitiveFilterCondition = parseScanCondition(exprRef.getExpr(), typeEnv);
            if (primitiveFilterCondition != null) {
                primitiveFilterConditions.add(primitiveFilterCondition);
            }
        }
        return primitiveFilterConditions;
    }

    private static ScanFilterCondition parseScanCondition(ILogicalExpression logicalExpression,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        if (!(logicalExpression instanceof AbstractFunctionCallExpression expr)) {
            return null;
        }
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        if (!BTreeAccessMethod.INSTANCE.getOptimizableFunctions().contains(new Pair<>(fi, false))) {
            return null;
        }
        ConstantExpression constantExpression = null;
        Pair<LogicalVariable, List<String>> accessPath = null;
        if (expr.getArguments().get(1).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constantExpression = (ConstantExpression) expr.getArguments().get(1).getValue();
            accessPath = parseAccessPath(expr.getArguments().get(0).getValue(), typeEnv);
        } else if (expr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constantExpression = (ConstantExpression) expr.getArguments().get(0).getValue();
            accessPath = parseAccessPath(expr.getArguments().get(1).getValue(), typeEnv);
        }
        if (accessPath == null) {
            return null;
        }
        return new ScanFilterCondition(fi, accessPath.getFirst(), accessPath.getSecond(), constantExpression);
    }

    private record UnnestExprRef(LogicalVariable scanVar, List<List<String>> unnestList, List<String> projectList) {
    }

    private static UnnestFilterCondition parseUnnestCondition(ILogicalExpression logicalExpression,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        if (!(logicalExpression instanceof AbstractFunctionCallExpression expr)) {
            return null;
        }
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        if (!BTreeAccessMethod.INSTANCE.getOptimizableFunctions().contains(new Pair<>(fi, false))) {
            return null;
        }
        ConstantExpression constantExpression = null;
        UnnestExprRef unnestExprRef = null;
        if (expr.getArguments().get(1).get().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constantExpression = (ConstantExpression) expr.getArguments().get(1).get();
            unnestExprRef = parseUnnestAccessPath(expr.getArguments().get(0).get(), typeEnv);
        } else if (expr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            constantExpression = (ConstantExpression) expr.getArguments().get(0).getValue();
            unnestExprRef = parseUnnestAccessPath(expr.getArguments().get(1).getValue(), typeEnv);
        }
        if (unnestExprRef == null) {
            return null;
        }
        return new UnnestFilterCondition(fi, unnestExprRef.unnestList(), unnestExprRef.projectList(),
                constantExpression);
    }

    private static UnnestExprRef parseUnnestAccessPath(ILogicalExpression expr, IVariableTypeEnvironment typeEnv)
            throws AlgebricksException {
        List<List<String>> fieldNamesList = new LinkedList<>();
        List<String> lastFieldNameList = new LinkedList<>();
        boolean hasSeenScanCollection = false, isLastScanCollection = false;

        while (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }

            AbstractFunctionCallExpression functionCallExpr = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fi = functionCallExpr.getFunctionIdentifier();
            if (fi == BuiltinFunctions.FIELD_ACCESS_BY_NAME || fi == BuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
                String fieldName = PushdownUtil.getFieldName(functionCallExpr, typeEnv);
                lastFieldNameList.addFirst(fieldName);
                if (isLastScanCollection) {
                    isLastScanCollection = false;
                }
            } else if (fi == BuiltinFunctions.SCAN_COLLECTION) {
                if (isLastScanCollection) {
                    return null;
                }
                fieldNamesList.addFirst(lastFieldNameList);
                lastFieldNameList = new LinkedList<>();
                isLastScanCollection = true;
                hasSeenScanCollection = true;
            } else {
                return null;
            }
            expr = functionCallExpr.getArguments().getFirst().getValue();
        }

        if (!hasSeenScanCollection) {
            return null;
        }

        fieldNamesList.addFirst(lastFieldNameList);
        List<String> projectList = fieldNamesList.getLast();
        fieldNamesList.removeLast();

        VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
        LogicalVariable var = varRef.getVariableReference();
        return new UnnestExprRef(var, fieldNamesList, projectList);
    }

    private static Pair<LogicalVariable, List<String>> parseAccessPath(ILogicalExpression expr,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        List<String> fieldNames = new LinkedList<>();
        while (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            AbstractFunctionCallExpression functionCallExpr = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fi = functionCallExpr.getFunctionIdentifier();
            if (fi != BuiltinFunctions.FIELD_ACCESS_BY_NAME && fi != BuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
                return null;
            }
            String fieldName = PushdownUtil.getFieldName(functionCallExpr, typeEnv);
            fieldNames.addFirst(fieldName);
            expr = functionCallExpr.getArguments().getFirst().get();
        }
        if (fieldNames.isEmpty()) {
            return null;
        }
        VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
        LogicalVariable var = varRef.getVariableReference();
        return new Pair<>(var, fieldNames);
    }

    public static JoinFilter parseJoinNode(AbstractBinaryJoinOperator joinOp, IOptimizationContext context)
            throws AlgebricksException {
        List<ExprRef> joinExprs = new ArrayList<>();
        ILogicalExpression joinExpression = joinOp.getCondition().get();
        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        if (joinExpression.splitIntoConjuncts(conjs)) {
            joinExprs.addAll(conjs.stream().map(expr -> new ExprRef(expr, joinOp)).toList());
        } else {
            joinExprs.add(new ExprRef(joinOp.getCondition(), joinOp));
        }
        traverseAndReplace(joinOp, joinExprs);
        List<JoinFilterCondition> joinConditions = new ArrayList<>();
        for (ExprRef joinExprRef : joinExprs) {
            IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(joinExprRef.getOp(), context);
            JoinFilterCondition joinCondition = parseJoinCondition(joinExprRef.getExpr(), typeEnv);
            if (joinCondition != null) {
                joinConditions.add(joinCondition);
            }
        }
        return new JoinFilter(joinConditions);
    }

    private static void traverseAndReplace(AbstractLogicalOperator op, List<ExprRef> exprs) {
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            replaceExprsWithAssign((AssignOperator) op, exprs);
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            traverseAndReplace((AbstractLogicalOperator) input.getValue(), exprs);
        }
    }

    private static void replaceExprsWithAssign(AssignOperator assignOp, List<ExprRef> exprRefs) {
        for (ExprRef exprRef : exprRefs) {
            if (exprRef.getExpr().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                continue;
            }
            for (int i = 0; i < assignOp.getVariables().size(); i++) {
                OperatorManipulationUtil.replaceVarWithExpr((AbstractFunctionCallExpression) exprRef.getExpr(),
                        assignOp.getVariables().get(i), assignOp.getExpressions().get(i).getValue());
            }
        }
    }

    private static void replaceExprsWithUnnest(UnnestOperator unnestOp, List<ExprRef> exprRefs) {
        for (ExprRef exprRef : exprRefs) {
            if (exprRef.getExpr().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                continue;
            }
            for (int i = 0; i < unnestOp.getVariables().size(); i++) {
                OperatorManipulationUtil.replaceVarWithExpr((AbstractFunctionCallExpression) exprRef.getExpr(),
                        unnestOp.getVariables().get(i), unnestOp.getExpressionRef().getValue());
            }
        }
    }

    public static JoinFilterCondition parseJoinCondition(ILogicalExpression logicalExpression,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        if (!(logicalExpression instanceof AbstractFunctionCallExpression expr)) {
            return null;
        }
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        if (!BTreeAccessMethod.INSTANCE.getOptimizableFunctions().contains(new Pair<>(fi, false))) {
            return null;
        }
        if (expr.getArguments().size() != 2) {
            return null;
        }
        ILogicalExpression lhs = expr.getArguments().get(0).get();
        ILogicalExpression rhs = expr.getArguments().get(1).get();
        Pair<LogicalVariable, List<String>> lhsAccessPath = parseAccessPath(lhs, typeEnv);
        if (lhsAccessPath == null) {
            return null;
        }
        Pair<LogicalVariable, List<String>> rhsAccessPath = parseAccessPath(rhs, typeEnv);
        if (rhsAccessPath == null) {
            return null;
        }
        return new JoinFilterCondition(fi, lhsAccessPath.getFirst(), lhsAccessPath.getSecond(),
                rhsAccessPath.getFirst(), rhsAccessPath.getSecond());
    }
}
