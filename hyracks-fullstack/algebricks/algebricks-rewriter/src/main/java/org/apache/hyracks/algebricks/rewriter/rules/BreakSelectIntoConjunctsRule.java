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
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rule breaks the select operator condition into conjuncts and create a different select operator per conjunct
 * when applicable.
 *
 * Example (simplified):
 * Before:
 * select (and(lt($$test.getField("field1"), 100), lt($$test.getField("field2"), 20)))
 * unnest $$test <- dataset("test.test")
 *
 * After:
 * select (lt($$test.getField("field1"), 100))
 * select (lt($$test.getField("field2"), 20))
 * unnest $$test <- dataset("test.test")
 */
public class BreakSelectIntoConjunctsRule implements IAlgebraicRewriteRule {

    // Conjuncts of the select operator condition
    private List<Mutable<ILogicalExpression>> selectOperatorConditionConjuncts = new ArrayList<>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> operatorRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> operatorRef, IOptimizationContext context)
            throws AlgebricksException {
        // Expected select operator
        AbstractLogicalOperator expectedSelectOperator = (AbstractLogicalOperator) operatorRef.getValue();

        // Bail if it's not a select operator
        if (expectedSelectOperator.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        // Select operator
        SelectOperator originalSelectOperator = (SelectOperator) expectedSelectOperator;

        // Select operator condition
        ILogicalExpression selectOperatorCondition = originalSelectOperator.getCondition().getValue();

        // Clear the condition conjuncts
        selectOperatorConditionConjuncts.clear();

        // Break the condition into conjuncts, bail if it's not applicable
        if (!selectOperatorCondition.splitIntoConjuncts(selectOperatorConditionConjuncts)) {
            return false;
        }

        // Source location
        SourceLocation sourceLoc = originalSelectOperator.getSourceLocation();

        // Inputs of original select operator
        Mutable<ILogicalOperator> originalSelectOperatorInputs = originalSelectOperator.getInputs().get(0);

        // First expression read of the conjuncts, this will be set to the original select operator at the end
        boolean isFirst = true;
        ILogicalExpression firstExpression = null;

        // Bottom operator points to the original select operator at first
        ILogicalOperator bottomOperator = originalSelectOperator;

        // Start creating the select operators for the condition conjuncts
        for (Mutable<ILogicalExpression> expressionRef : selectOperatorConditionConjuncts) {
            ILogicalExpression expression = expressionRef.getValue();

            // Hold reference to the first condition
            if (isFirst) {
                isFirst = false;
                firstExpression = expression;
            } else {
                // New select operator
                SelectOperator newSelectOperator =
                        new SelectOperator(new MutableObject<>(expression), originalSelectOperator.getRetainMissing(),
                                originalSelectOperator.getMissingPlaceholderVariable());
                newSelectOperator.setSourceLocation(sourceLoc);

                // Put the new operator at the bottom (child of current operator)
                List<Mutable<ILogicalOperator>> bottomOperatorInputs = bottomOperator.getInputs();
                bottomOperatorInputs.clear();
                bottomOperatorInputs.add(new MutableObject<>(newSelectOperator));

                // Compute the output type environment
                context.computeAndSetTypeEnvironmentForOperator(bottomOperator);

                // Bottom operator points to the new operator
                bottomOperator = newSelectOperator;
            }
        }

        // Add the original select operator inputs to the bottom operator
        bottomOperator.getInputs().add(originalSelectOperatorInputs);

        // Assign the first expression to the original operator (top)
        originalSelectOperator.getCondition().setValue(firstExpression);

        // (Re)compute the output type environment
        context.computeAndSetTypeEnvironmentForOperator(bottomOperator);
        context.computeAndSetTypeEnvironmentForOperator(originalSelectOperator);

        return true;
    }
}
