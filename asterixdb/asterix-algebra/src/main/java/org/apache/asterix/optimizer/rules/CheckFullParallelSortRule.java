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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * <pre>
 * Description:
 *      This rule checks whether full parallel sort is applicable to {@link OrderOperator}. It disables full parallel
 *      sort when a limit operator or running aggregate operator is present in the parents of the order operator.
 * Pre-conditions:
 *      None.
 * Post-requirements:
 *      1. {@link org.apache.hyracks.algebricks.rewriter.rules.EnforceStructuralPropertiesRule}
 * </pre>
 */
public class CheckFullParallelSortRule implements IAlgebraicRewriteRule {
    private final List<AbstractLogicalOperator> parents = new ArrayList<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        parents.add((AbstractLogicalOperator) opRef.getValue());
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx) throws AlgebricksException {
        parents.remove(parents.size() - 1);
        AbstractLogicalOperator orderOp = (AbstractLogicalOperator) opRef.getValue();
        if (orderOp.getOperatorTag() == LogicalOperatorTag.ORDER
                && !orderOp.getAnnotations().containsKey(OperatorAnnotations.USE_STATIC_RANGE)) {
            // disable when sort output is consumed by limit & running agg op (result should be unpartitioned for limit)
            AbstractLogicalOperator parent;
            for (int i = parents.size() - 1; i >= 0; i--) {
                parent = parents.get(i);
                if (parent.getOperatorTag() == LogicalOperatorTag.LIMIT
                        && ((LimitOperator) parent).isTopmostLimitOp()) {
                    orderOp.getAnnotations().put(OperatorAnnotations.USE_DYNAMIC_RANGE, Boolean.FALSE);
                    return true;
                }
                if (parent.getOperatorTag() == LogicalOperatorTag.RUNNINGAGGREGATE) {
                    orderOp.getAnnotations().put(OperatorAnnotations.USE_DYNAMIC_RANGE, Boolean.FALSE);
                    return true;
                }
            }
        }
        return false;
    }
}
