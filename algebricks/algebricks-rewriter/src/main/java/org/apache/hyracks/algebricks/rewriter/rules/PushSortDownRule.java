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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule pushes down the sort operator
 * as much as possible to where the sort keys are available.
 * The rule pushes the sort operator down one-step-at-a-time.
 */
public class PushSortDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator operator = opRef.getValue();
        if (operator.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        }

        // Gets used variables in the sort operator.
        OrderOperator orderOperator = (OrderOperator) operator;
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderKeys = orderOperator.getOrderExpressions();
        Set<LogicalVariable> orderUsedVars = new HashSet<LogicalVariable>();
        for (Pair<IOrder, Mutable<ILogicalExpression>> orderKey : orderKeys) {
            orderKey.second.getValue().getUsedVariables(orderUsedVars);
        }
        Mutable<ILogicalOperator> inputOpRef = orderOperator.getInputs().get(0);
        ILogicalOperator inputOperator = inputOpRef.getValue();

        // Only pushes sort through assign:
        // 1. Blocking operators like sort/group/join cannot be pushed through.
        // 2. Data reducing operators like select/project should not be pushed through.
        // 3. Order-destroying operator like unnest/unnest-map cannot be pushed through.
        if (inputOperator.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        Set<LogicalVariable> inputProducedVars = new HashSet<LogicalVariable>();
        VariableUtilities.getProducedVariables(inputOperator, inputProducedVars);

        // Intersects used variables in the sort and variables produced by inputOperator.
        orderUsedVars.retainAll(inputProducedVars);
        if (!orderUsedVars.isEmpty()) {
            // If the sort uses any variable that is produced by this operator.
            return false;
        }

        // Switches sort and its input operator.
        opRef.setValue(inputOperator);
        inputOpRef.setValue(inputOperator.getInputs().get(0).getValue());
        inputOperator.getInputs().get(0).setValue(orderOperator);

        // Re-computes the type environments.
        context.computeAndSetTypeEnvironmentForOperator(orderOperator);
        context.computeAndSetTypeEnvironmentForOperator(inputOperator);
        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

}
