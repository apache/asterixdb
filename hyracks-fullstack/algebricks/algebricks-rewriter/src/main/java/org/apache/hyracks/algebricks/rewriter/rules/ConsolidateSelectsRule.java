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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Matches the following operator pattern:
 * (select) <-- ((assign)* <-- (select)*)+
 * Consolidates the selects to:
 * (select) <-- (assign)*
 */
public class ConsolidateSelectsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator firstSelect = (SelectOperator) op;

        IFunctionInfo andFn = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
        // New conjuncts for consolidated select.
        AbstractFunctionCallExpression conj = null;
        AbstractLogicalOperator topMostOp = null;
        AbstractLogicalOperator selectParent = null;
        AbstractLogicalOperator nextSelect = firstSelect;
        do {
            // Skip through assigns.
            do {
                selectParent = nextSelect;
                nextSelect = (AbstractLogicalOperator) selectParent.getInputs().get(0).getValue();
            } while (nextSelect.getOperatorTag() == LogicalOperatorTag.ASSIGN && OperatorPropertiesUtil
                    .isMovable(nextSelect) /* Select cannot be pushed through un-movable operators.*/);
            // Stop if the child op is not a select.
            if (nextSelect.getOperatorTag() != LogicalOperatorTag.SELECT) {
                break;
            }
            // Remember the top-most op that we are not removing.
            topMostOp = selectParent;

            // Initialize the new conjuncts, if necessary.
            if (conj == null) {
                conj = new ScalarFunctionCallExpression(andFn);
                conj.setSourceLocation(firstSelect.getSourceLocation());
                // Add the first select's condition.
                conj.getArguments().add(new MutableObject<ILogicalExpression>(firstSelect.getCondition().getValue()));
            }

            // Consolidate all following selects.
            do {
                // Add the condition nextSelect to the new list of conjuncts.
                conj.getArguments().add(((SelectOperator) nextSelect).getCondition());
                selectParent = nextSelect;
                nextSelect = (AbstractLogicalOperator) nextSelect.getInputs().get(0).getValue();
            } while (nextSelect.getOperatorTag() == LogicalOperatorTag.SELECT);

            // Hook up the input of the top-most remaining op if necessary.
            if (topMostOp.getOperatorTag() == LogicalOperatorTag.ASSIGN || topMostOp == firstSelect) {
                topMostOp.getInputs().set(0, selectParent.getInputs().get(0));
            }

            // Prepare for next iteration.
            nextSelect = selectParent;
        } while (true);

        // Did we consolidate any selects?
        if (conj == null) {
            return false;
        }

        // Set the new conjuncts.
        firstSelect.getCondition().setValue(conj);
        context.computeAndSetTypeEnvironmentForOperator(firstSelect);
        return true;
    }
}
