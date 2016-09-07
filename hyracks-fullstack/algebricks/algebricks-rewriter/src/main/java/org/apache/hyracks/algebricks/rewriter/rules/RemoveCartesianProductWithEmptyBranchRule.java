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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes Cartesian product operators that have one input branch is EmptyTupleSource.
 */
public class RemoveCartesianProductWithEmptyBranchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator joinOperator = (AbstractBinaryJoinOperator) op;
        ILogicalOperator left = joinOperator.getInputs().get(0).getValue();
        ILogicalOperator right = joinOperator.getInputs().get(1).getValue();

        if (!joinOperator.getCondition().getValue().equals(ConstantExpression.TRUE)) {
            return false;
        }
        if (emptyBranch(left)) {
            opRef.setValue(right);
            return true;
        }
        if (emptyBranch(right)) {
            opRef.setValue(left);
            return true;
        }
        return false;
    }

    private boolean emptyBranch(ILogicalOperator op) throws AlgebricksException {
        if (op.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return true;
        }
        Set<LogicalVariable> liveVariables = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVariables);
        // No variables will be populated and the cardinality does not change.
        // If there is only one tuple from the branch, the output cardinality
        // of the cartesian product will not be changed.
        return liveVariables.isEmpty() && OperatorPropertiesUtil.isCardinalityExactOne(op);
    }
}
