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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.asterix.optimizer.rules.cbo.AbstractLeafInput;
import org.apache.asterix.optimizer.rules.cbo.OperatorUtils;
import org.apache.asterix.optimizer.rules.cbo.RangeMapUtil;
import org.apache.asterix.optimizer.rules.cbo.RealLeafInput;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * <pre>
 *     This rule attaches a static RangeMap to an {@link OrderOperator} to enable full parallel sort when computed at compile-time
 *     computed based on samples.
 *     It skips this step if:
 *       - A RangeMap already exists,
 *       - Parallel sort is disabled, or
 *       - The ORDER input subtree contains scans {@link org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator} without sample indexes.
 *       - A limit operator or running aggregate operator is
 *       present in the parents of the order operator.
 *       - The ORDER operator has execution mode is not  PARTITIONED.
 *
 * Pre-conditions:
 *     - ORDER operator exists in the plan.
 *
 * Post-requirements:
 *     - {@link org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule}
 * </pre>
 */

public class AttachCompileTimeRangeMapRule implements IAlgebraicRewriteRule {
    private final List<AbstractLogicalOperator> parents = new ArrayList<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        parents.add((AbstractLogicalOperator) opRef.getValue());
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        parents.remove(parents.size() - 1);
        AbstractLogicalOperator orderOp = (AbstractLogicalOperator) opRef.getValue();

        if (parallelSortNotApplicable(orderOp, context)) {
            return false;
        }
        // compute and attach the range map
        return RangeMapUtil.attachRangeMapIfNeeded(orderOp, context);
    }

    private boolean parallelSortNotApplicable(AbstractLogicalOperator orderOp, IOptimizationContext context)
            throws AlgebricksException {
        // only care about ORDER; everything else is "not applicable"
        if (orderOp.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return true;
        }

        // already has a static range or range map computation failed,  don't try again
        if (orderOp.getAnnotations().containsKey(OperatorAnnotations.USE_STATIC_RANGE)) {
            return true;
        }

        // only consider range map for partitioned sorts
        if (orderOp.getExecutionMode() != AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            return true;
        }

        // TODO(janhavi): skip rule if interval merge join is present in the subtree and range hint is provided (imp)
        // otherwise interval functions should also use compile time range map
        if (hasIntervalMergeJoin(orderOp)) {
            return true;
        }

        // disable when sort output is consumed by limit & running agg op (result should be unpartitioned for limit)
        AbstractLogicalOperator parent;
        for (int i = parents.size() - 1; i >= 0; i--) {
            parent = parents.get(i);
            if (parent.getOperatorTag() == LogicalOperatorTag.LIMIT && ((LimitOperator) parent).isTopmostLimitOp()) {
                // No plan change: do not compute a range map if a topmost LIMIT is present in the parent chain.
                return true;
            }
            if (parent.getOperatorTag() == LogicalOperatorTag.RUNNINGAGGREGATE) {
                // No plan change: do not compute a range map if a RUNNINGAGGREGATE operator is present in the parent chain.
                return true;
            }
        }

        return !shouldComputeRangeMap(orderOp, context);
    }

    private boolean shouldComputeRangeMap(AbstractLogicalOperator orderOp, IOptimizationContext ctx)
            throws AlgebricksException {
        // sort-parallel disabled
        if (!ctx.getPhysicalOptimizationConfig().getSortParallel()) {
            return false;
        }

        // verify samples exist under order input subtree
        ILogicalOperator root = orderOp.getInputs().get(0).getValue();
        List<ILogicalOperator> scanOps = collectDataSourceScanOperators(root);
        //if there are no dataset scans / unnest map logical ops, there can’t be sample indexes
        if (scanOps.isEmpty()) {
            return false;
        }
        List<AbstractLeafInput> leafInputs = new ArrayList<>(scanOps.size());
        for (ILogicalOperator op : scanOps) {
            leafInputs.add(new RealLeafInput(op));
        }

        return OperatorUtils.doAllDataSourcesHaveSamples(leafInputs, ctx);
    }

    private List<ILogicalOperator> collectDataSourceScanOperators(ILogicalOperator root) {
        List<ILogicalOperator> scans = new ArrayList<>();
        Deque<ILogicalOperator> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            ILogicalOperator op = stack.pop();
            if (op.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN
                    || op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                scans.add(op);
            }
            for (Mutable<ILogicalOperator> in : op.getInputs()) {
                stack.push(in.getValue());
            }
        }
        return scans;
    }

    private static boolean hasIntervalMergeJoin(ILogicalOperator op) {
        if (op == null) {
            return false;
        }
        if (op instanceof AbstractBinaryJoinOperator) {
            IPhysicalOperator pop = ((AbstractLogicalOperator) op).getPhysicalOperator();
            if (pop != null && pop.getOperatorTag() == PhysicalOperatorTag.INTERVAL_MERGE_JOIN) {
                return true;
            }
        }
        for (Mutable<ILogicalOperator> in : op.getInputs()) {
            if (hasIntervalMergeJoin(in.getValue()))
                return true;
        }
        return false;
    }
}
