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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;

public class IntroduceAggregateCombinerRule extends AbstractIntroduceCombinerRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        // Disable aggregate combiners for nested plans inside window operators
        if (op.getOperatorTag() == LogicalOperatorTag.WINDOW) {
            WindowOperator winOp = (WindowOperator) op;
            if (winOp.hasNestedPlans()) {
                for (ILogicalPlan plan : winOp.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                        ILogicalOperator rootOp = root.getValue();
                        if (rootOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                            context.addToDontApplySet(this, rootOp);
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOp = (AggregateOperator) op;
        if (!aggOp.isGlobal() || aggOp.getExecutionMode() == ExecutionMode.LOCAL) {
            return false;
        }
        Set<SimilarAggregatesInfo> toReplaceSet = new HashSet<SimilarAggregatesInfo>();
        Pair<Boolean, Mutable<ILogicalOperator>> result = tryToPushAgg(aggOp, null, toReplaceSet, context);
        if (!result.first || result.second == null) {
            return false;
        }
        replaceOriginalAggFuncs(toReplaceSet);
        context.computeAndSetTypeEnvironmentForOperator(aggOp);
        return true;
    }
}
