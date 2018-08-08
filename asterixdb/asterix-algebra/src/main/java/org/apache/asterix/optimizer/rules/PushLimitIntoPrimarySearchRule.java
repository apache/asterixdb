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
package org.apache.asterix.optimizer.rules;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;

/**
 * Pattern:
 * SCAN or UNNEST_MAP -> ((ASSIGN)* -> (SELECT))? -> (EXCHANGE)? -> LIMIT
 * We push both SELECT condition and LIMIT to SCAN or UNNEST_MAP
 *
 */
public class PushLimitIntoPrimarySearchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.LIMIT) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);

        Integer outputLimit = PushLimitIntoOrderByRule.getOutputLimit((LimitOperator) op);
        if (outputLimit == null) {
            // we cannot push if limit is not constant
            return false;
        }

        Mutable<ILogicalOperator> childOp = op.getInputs().get(0);
        if (childOp.getValue().getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
            childOp = childOp.getValue().getInputs().get(0);
        }
        boolean changed;
        if (childOp.getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
            changed = rewriteSelect(childOp, outputLimit, context);
        } else {
            changed = setLimitForScanOrUnnestMap(childOp.getValue(), outputLimit);
        }
        if (changed) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        return changed;
    }

    private boolean rewriteSelect(Mutable<ILogicalOperator> op, int outputLimit, IOptimizationContext context)
            throws AlgebricksException {
        SelectOperator select = (SelectOperator) op.getValue();
        ILogicalExpression selectCondition = select.getCondition().getValue();
        Set<LogicalVariable> selectedVariables = new HashSet<>();
        selectCondition.getUsedVariables(selectedVariables);

        MutableObject<ILogicalExpression> selectConditionRef = new MutableObject<>(selectCondition.cloneExpression());

        // If the select condition uses variables from assigns then inline those variables into it
        ILogicalOperator child = select.getInputs().get(0).getValue();
        InlineVariablesRule.InlineVariablesVisitor inlineVisitor = null;
        Map<LogicalVariable, ILogicalExpression> varAssignRhs = null;
        for (; child.getOperatorTag() == LogicalOperatorTag.ASSIGN; child = child.getInputs().get(0).getValue()) {
            if (varAssignRhs == null) {
                varAssignRhs = new HashMap<>();
            } else {
                varAssignRhs.clear();
            }
            AssignOperator assignOp = (AssignOperator) child;
            extractInlinableVariablesFromAssign(assignOp, selectedVariables, varAssignRhs);
            if (!varAssignRhs.isEmpty()) {
                if (inlineVisitor == null) {
                    inlineVisitor = new InlineVariablesRule.InlineVariablesVisitor(varAssignRhs);
                    inlineVisitor.setContext(context);
                    inlineVisitor.setOperator(select);
                }
                if (!inlineVisitor.transform(selectConditionRef)) {
                    break;
                }
                selectedVariables.clear();
                selectConditionRef.getValue().getUsedVariables(selectedVariables);
            }
        }

        boolean changed = false;
        switch (child.getOperatorTag()) {
            case DATASOURCESCAN:
                DataSourceScanOperator scan = (DataSourceScanOperator) child;
                if (isScanPushable(scan, selectedVariables)) {
                    scan.setSelectCondition(selectConditionRef);
                    scan.setOutputLimit(outputLimit);
                    changed = true;
                }
                break;
            case UNNEST_MAP:
                UnnestMapOperator unnestMap = (UnnestMapOperator) child;
                if (isUnnestMapPushable(unnestMap, selectedVariables)) {
                    unnestMap.setSelectCondition(selectConditionRef);
                    unnestMap.setOutputLimit(outputLimit);
                    changed = true;
                }
                break;
        }

        if (changed) {
            // SELECT is not needed
            op.setValue(op.getValue().getInputs().get(0).getValue());
        }
        return changed;
    }

    private boolean setLimitForScanOrUnnestMap(ILogicalOperator op, int outputLimit) {
        if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scan = (DataSourceScanOperator) op;
            if (isScanPushable(scan, Collections.emptySet())) {
                scan.setOutputLimit(outputLimit);
                return true;
            }
        } else if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnestMap = (UnnestMapOperator) op;
            if (isUnnestMapPushable(unnestMap, Collections.emptySet())) {
                unnestMap.setOutputLimit(outputLimit);
                return true;
            }
        }
        return false;
    }

    private boolean isUnnestMapPushable(UnnestMapOperator op, Set<LogicalVariable> selectedVariables) {
        if (op.getOutputLimit() >= 0) {
            // already pushed
            return false;
        }
        ILogicalExpression unnestExpr = op.getExpressionRef().getValue();
        if (op.propagatesInput() || unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
        if (!f.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
            return false;
        }
        AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
        jobGenParams.readFromFuncArgs(f.getArguments());
        if (!jobGenParams.isPrimaryIndex()) {
            return false;
        }
        if (!op.getScanVariables().containsAll(selectedVariables)) {
            return false;
        }
        return true;
    }

    private boolean isScanPushable(DataSourceScanOperator op, Set<LogicalVariable> selectedVariables) {
        if (op.getOutputLimit() >= 0) {
            return false;
        }
        if (!op.getInputs().isEmpty()
                && op.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return false;
        }
        if (((DataSource) op.getDataSource()).getDatasourceType() != DataSource.Type.INTERNAL_DATASET) {
            return false;
        }
        if (!op.getScanVariables().containsAll(selectedVariables)) {
            return false;
        }
        return true;
    }

    private void extractInlinableVariablesFromAssign(AssignOperator assignOp, Set<LogicalVariable> includeVariables,
            Map<LogicalVariable, ILogicalExpression> outVarExprs) {
        List<LogicalVariable> vars = assignOp.getVariables();
        List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();
        for (int i = 0, ln = vars.size(); i < ln; i++) {
            LogicalVariable var = vars.get(i);
            if (includeVariables.contains(var)) {
                ILogicalExpression expr = exprs.get(i).getValue();
                if (expr.isFunctional()) {
                    outVarExprs.put(var, expr);
                }
            }
        }
    }
}
