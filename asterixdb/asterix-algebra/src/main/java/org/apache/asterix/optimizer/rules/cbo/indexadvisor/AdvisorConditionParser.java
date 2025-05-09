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

import org.apache.asterix.metadata.utils.PushdownUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.BTreeAccessMethod;
import org.apache.commons.lang3.mutable.Mutable;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

public class AdvisorConditionParser {

    public static ScanFilter parseScanNode(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {

        List<Mutable<ILogicalExpression>> filterExprs = new ArrayList<>();
        IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(op, context);
        ILogicalOperator tempOp = op;
        do {
            if (tempOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selectOp = (SelectOperator) tempOp;
                ILogicalExpression condition = selectOp.getCondition().getValue();
                List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
                if (condition.splitIntoConjuncts(conjs)) {
                    filterExprs.addAll(conjs);
                } else {
                    filterExprs.add(selectOp.getCondition());
                }
            }
            tempOp = tempOp.getInputs().getFirst().getValue();
        } while (tempOp.hasInputs());

        filterExprs = OperatorManipulationUtil.cloneExpressions(filterExprs);

        filterExprs.removeIf(expr -> !(expr.getValue() instanceof AbstractFunctionCallExpression));

        tempOp = op;
        do {
            if (tempOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                replaceExprsWithAssign((AssignOperator) tempOp, filterExprs);
            }
            tempOp = tempOp.getInputs().getFirst().getValue();
        } while (tempOp.hasInputs());

        List<ScanFilterCondition> filterConditions = new ArrayList<>();

        for (Mutable<ILogicalExpression> filterExpr : filterExprs) {
            ScanFilterCondition filterCondition = parseCondition(filterExpr.getValue(), typeEnv);
            if (filterCondition != null) {
                filterConditions.add(filterCondition);
            }
        }

        return new ScanFilter(filterConditions);
    }

    private static ScanFilterCondition parseCondition(ILogicalExpression logicalExpression,
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

        return new ScanFilterCondition(fi, accessPath.getSecond(), constantExpression);
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

            expr = functionCallExpr.getArguments().getFirst().getValue();
        }

        VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
        LogicalVariable var = varRef.getVariableReference();
        return new Pair<>(var, fieldNames);
    }

    public static JoinFilter parseJoinNode(AbstractBinaryJoinOperator joinOp, IOptimizationContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> joinExprs = new ArrayList<>();
        IVariableTypeEnvironment typeEnv = PushdownUtil.getTypeEnv(joinOp, context);

        ILogicalExpression joinExpression = joinOp.getCondition().getValue();
        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        if (joinExpression.splitIntoConjuncts(conjs)) {
            joinExprs.addAll(conjs);
        } else {
            joinExprs.add(joinOp.getCondition());
        }
        joinExprs = OperatorManipulationUtil.cloneExpressions(joinExprs);
        traverseAndReplace(joinOp, joinExprs);

        List<JoinFilterCondition> joinConditions = new ArrayList<>();

        for (Mutable<ILogicalExpression> joinExpr : joinExprs) {
            JoinFilterCondition joinCondition = parseJoinCondition(joinExpr.getValue(), typeEnv);
            if (joinCondition != null) {
                joinConditions.add(joinCondition);
            }
        }

        return new JoinFilter(joinConditions);
    }

    private static void traverseAndReplace(AbstractLogicalOperator op, List<Mutable<ILogicalExpression>> exprs) {

        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            replaceExprsWithAssign((AssignOperator) op, exprs);
        }

        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            traverseAndReplace((AbstractLogicalOperator) input.getValue(), exprs);
        }

    }

    public static void replaceExprsWithAssign(AssignOperator assignOp, List<Mutable<ILogicalExpression>> exprs) {
        for (Mutable<ILogicalExpression> filterExpr : exprs) {
            if (filterExpr.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                continue;
            }
            for (int i = 0; i < assignOp.getVariables().size(); i++) {
                OperatorManipulationUtil.replaceVarWithExpr((AbstractFunctionCallExpression) filterExpr.getValue(),
                        assignOp.getVariables().get(i), assignOp.getExpressions().get(i).getValue());
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

        ILogicalExpression lhs = expr.getArguments().get(0).getValue();
        ILogicalExpression rhs = expr.getArguments().get(1).getValue();

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
