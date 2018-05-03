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
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pattern:
 * SCAN or UNNEST_MAP -> (SELECT)? -> (EXCHANGE)? -> LIMIT
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

        Long outputLimit = getOutputLimit((LimitOperator) op);
        if (outputLimit == null) {
            // we cannot push if limit is not constant
            return false;
        }

        Mutable<ILogicalOperator> childOp = op.getInputs().get(0);
        if (childOp.getValue().getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
            childOp = childOp.getValue().getInputs().get(0);
        }
        boolean changed = false;
        if (childOp.getValue().getOperatorTag() == LogicalOperatorTag.SELECT) {
            changed = rewriteSelect(childOp, outputLimit);
        } else {
            changed = setLimitForScanOrUnnestMap(childOp.getValue(), outputLimit);
        }
        if (changed) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        return changed;
    }

    private Long getOutputLimit(LimitOperator limit) {
        if (limit.getMaxObjects().getValue().getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        long outputLimit = AccessMethodUtils.getInt64Constant(limit.getMaxObjects());
        if (limit.getOffset() != null && limit.getOffset().getValue() != null) {
            if (limit.getOffset().getValue().getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            outputLimit += AccessMethodUtils.getInt64Constant(limit.getOffset());
        }
        return outputLimit;
    }

    private boolean rewriteSelect(Mutable<ILogicalOperator> op, long outputLimit) throws AlgebricksException {
        SelectOperator select = (SelectOperator) op.getValue();
        Set<LogicalVariable> selectedVariables = new HashSet<>();
        select.getCondition().getValue().getUsedVariables(selectedVariables);
        ILogicalOperator child = select.getInputs().get(0).getValue();
        boolean changed = false;
        if (child.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scan = (DataSourceScanOperator) child;
            if (isScanPushable(scan, selectedVariables)) {
                scan.setSelectCondition(select.getCondition());
                scan.setOutputLimit(outputLimit);
                changed = true;
            }
        } else if (child.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnestMap = (UnnestMapOperator) child;
            if (isUnnestMapPushable(unnestMap, selectedVariables)) {
                unnestMap.setSelectCondition(select.getCondition());
                unnestMap.setOutputLimit(outputLimit);
                changed = true;
            }
        }
        if (changed) {
            // SELECT is not needed
            op.setValue(child);
        }
        return changed;
    }

    private boolean setLimitForScanOrUnnestMap(ILogicalOperator op, long outputLimit) throws AlgebricksException {
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

}
