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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractStableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortGroupByPOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * @author yingyib
 *         merge externalsort+preclustered-gby into sort-gby
 */
public class PushGroupByIntoSortRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op1 = opRef.getValue();
        if (op1 == null) {
            return false;
        }
        boolean changed = false;
        for (Mutable<ILogicalOperator> childRef : op1.getInputs()) {
            AbstractLogicalOperator op = (AbstractLogicalOperator) childRef.getValue();
            if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                PhysicalOperatorTag opTag = op.getPhysicalOperator().getOperatorTag();
                GroupByOperator groupByOperator = (GroupByOperator) op;
                if (opTag == PhysicalOperatorTag.PRE_CLUSTERED_GROUP_BY) {
                    Mutable<ILogicalOperator> op2Ref = op.getInputs().get(0).getValue().getInputs().get(0);
                    AbstractLogicalOperator op2 = (AbstractLogicalOperator) op2Ref.getValue();
                    if (op2.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.STABLE_SORT) {
                        AbstractStableSortPOperator sortPhysicalOperator =
                                (AbstractStableSortPOperator) op2.getPhysicalOperator();
                        if (groupByOperator.getNestedPlans().size() != 1) {
                            //Sort group-by currently works only for one nested plan with one root containing
                            //an aggregate and a nested-tuple-source.
                            continue;
                        }
                        ILogicalPlan p0 = groupByOperator.getNestedPlans().get(0);
                        if (p0.getRoots().size() != 1) {
                            //Sort group-by currently works only for one nested plan with one root containing
                            //an aggregate and a nested-tuple-source.
                            continue;
                        }

                        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
                        AbstractLogicalOperator r0Logical = (AbstractLogicalOperator) r0.getValue();
                        if (r0Logical.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                            //we only rewrite aggregation function; do nothing for running aggregates
                            continue;
                        }
                        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
                        AbstractLogicalOperator aggInputOp =
                                (AbstractLogicalOperator) aggOp.getInputs().get(0).getValue();
                        if (aggInputOp.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                            continue;
                        }

                        boolean hasIntermediateAggregate =
                                generateMergeAggregationExpressions(groupByOperator, context);
                        if (!hasIntermediateAggregate) {
                            continue;
                        }

                        //replace preclustered gby with sort gby
                        if (!groupByOperator.isGroupAll()) {
                            op.setPhysicalOperator(new SortGroupByPOperator(groupByOperator.getGroupByVarList(),
                                    sortPhysicalOperator.getSortColumns()));
                        }
                        // remove the stable sort operator
                        op.getInputs().clear();
                        op.getInputs().addAll(op2.getInputs());
                        changed = true;
                    }
                }
                continue;
            } else {
                continue;
            }
        }
        return changed;
    }

    private boolean generateMergeAggregationExpressions(GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "External/sort group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "External/sort group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        IMergeAggregationExpressionFactory mergeAggregationExpressionFactory =
                context.getMergeAggregationExpressionFactory();
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
        List<Mutable<ILogicalExpression>> aggFuncRefs = aggOp.getExpressions();
        List<LogicalVariable> originalAggVars = aggOp.getVariables();
        int n = aggOp.getExpressions().size();
        List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < n; i++) {
            ILogicalExpression mergeExpr = mergeAggregationExpressionFactory
                    .createMergeAggregation(originalAggVars.get(i), aggFuncRefs.get(i).getValue(), context);
            if (mergeExpr == null) {
                return false;
            }
            mergeExpressionRefs.add(new MutableObject<ILogicalExpression>(mergeExpr));
        }
        aggOp.setMergeExpressions(mergeExpressionRefs);
        return true;
    }
}
