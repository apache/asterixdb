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
package org.apache.hyracks.algebricks.rewriter.rules.subplan;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.rewriter.rules.AbstractDecorrelationRule;

/**
 * The rule searches for operators that can be moved outside the subplan.
 *
 * <pre>
 * Before
 *
 *   %PARENT_PLAN
 *   SUBPLAN{
 *     %NESTED_OPERATORS_B+
 *     ASSIGN || %SUBPLAN
 *     %NESTED_OPERATORS_A*
 *     NESTEDTUPLESOURCE
 *   }
 *   %CHILD_PLAN
 *
 *   where
 *     %SUBPLAN has one nested plan with a root AGGREGATE operator.
 *
 * After
 *
 *   %PARENT_PLAN
 *   SUBPLAN{
 *     %NESTED_OPERATORS_B+
 *     %NESTED_OPERATORS_A*
 *     NESTEDTUPLESOURCE
 *   }
 *   ASSIGN || %SUBPLAN
 *   %CHILD_PLAN
 * </pre>
 */
public class MoveFreeVariableOperatorOutOfSubplanRule extends AbstractDecorrelationRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplanOp = (SubplanOperator) op;
        ILogicalOperator inputOp = subplanOp.getInputs().get(0).getValue();
        Set<LogicalVariable> liveVarsBeforeSubplan = new HashSet<>();
        VariableUtilities.getLiveVariables(inputOp, liveVarsBeforeSubplan);

        boolean changed = false;
        for (ILogicalPlan plan : subplanOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
                //Make sure we are looking at subplan with a scan/join
                if (!descOrSelfIsScanOrJoin(rootRef.getValue())) {
                    continue;
                }
                Mutable<ILogicalOperator> currentOpRef = rootRef;
                ILogicalOperator currentOp = rootRef.getValue();
                while (currentOp.getInputs().size() == 1) {
                    Mutable<ILogicalOperator> childOpRef = currentOp.getInputs().get(0);
                    ILogicalOperator childOp = childOpRef.getValue();

                    // Try to move operators that only uses free variables out of the subplan.
                    if (movableOperator(currentOp.getOperatorTag())
                            && independentOperator(currentOp, liveVarsBeforeSubplan)
                            && producedVariablesCanbePropagated(currentOp)) {
                        extractOperator(subplanOp, inputOp, currentOpRef);
                        inputOp = currentOp;
                        changed = true;
                    } else {
                        // in the case the operator is not moved, move currentOpRef to childOpRef.
                        currentOpRef = childOpRef;
                    }
                    currentOp = childOp;
                }
            }
        }
        return changed;
    }

    // Checks whether the current operator is independent of the nested input pipeline in the subplan.
    private boolean independentOperator(ILogicalOperator op, Set<LogicalVariable> liveVarsBeforeSubplan)
            throws AlgebricksException {
        Set<LogicalVariable> usedVars = new HashSet<>();
        VariableUtilities.getUsedVariables(op, usedVars);
        return liveVarsBeforeSubplan.containsAll(usedVars);
    }

    // Checks whether there is a variable killing operator in the nested pipeline
    private boolean producedVariablesCanbePropagated(ILogicalOperator operator) throws AlgebricksException {
        ILogicalOperator currentOperator = operator;
        // Makes sure the produced variables by operator are not killed in the nested pipeline below it.
        while (!currentOperator.getInputs().isEmpty()) {
            LogicalOperatorTag operatorTag = currentOperator.getOperatorTag();
            if (operatorTag == LogicalOperatorTag.AGGREGATE || operatorTag == LogicalOperatorTag.RUNNINGAGGREGATE
                    || operatorTag == LogicalOperatorTag.GROUP) {
                return false;
            }
            if (operatorTag == LogicalOperatorTag.PROJECT) {
                Set<LogicalVariable> producedVars = new HashSet<>();
                VariableUtilities.getProducedVariables(currentOperator, producedVars);
                ProjectOperator projectOperator = (ProjectOperator) currentOperator;
                if (!projectOperator.getVariables().containsAll(producedVars)) {
                    return false;
                }
            }
            currentOperator = currentOperator.getInputs().get(0).getValue();
        }
        return true;
    }

    // Extracts the current operator out of the subplan.
    private void extractOperator(ILogicalOperator subplan, ILogicalOperator inputOp,
            Mutable<ILogicalOperator> currentOpRef) {
        // Removes currentOp from the nested pipeline inside subplan.
        ILogicalOperator currentOp = currentOpRef.getValue();
        currentOpRef.setValue(currentOp.getInputs().get(0).getValue());

        // Inserts currentOp between subplanOp and inputOp.
        subplan.getInputs().get(0).setValue(currentOp);
        currentOp.getInputs().get(0).setValue(inputOp);
    }

    protected boolean movableOperator(LogicalOperatorTag operatorTag) {
        return (operatorTag == LogicalOperatorTag.ASSIGN || operatorTag == LogicalOperatorTag.SUBPLAN);
    }
}
