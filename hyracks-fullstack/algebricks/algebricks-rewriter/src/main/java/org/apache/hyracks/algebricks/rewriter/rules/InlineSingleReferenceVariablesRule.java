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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

/**
 * Inlines variables that are referenced exactly once.
 *
 * Preconditions/Assumptions:
 * Assumes no projects are in the plan.
 *
 * Example assuming variable $$3 is referenced exactly once.
 *
 * Before plan:
 * select (funcA($$3))
 *   ...
 *     assign [$$3] <- [field-access($$0, 1)]
 *
 * After plan:
 * select (funcA(field-access($$0, 1))
 *   ...
 *     assign [] <- []
 */
public class InlineSingleReferenceVariablesRule extends InlineVariablesRule {

    // Maps from variable to a list of operators using that variable.
    protected Map<LogicalVariable, List<ILogicalOperator>> usedVarsMap =
            new LinkedHashMap<LogicalVariable, List<ILogicalOperator>>();
    protected List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();

    @Override
    protected void prepare(IOptimizationContext context) {
        super.prepare(context);
        usedVarsMap.clear();
        usedVars.clear();
    }

    @Override
    protected boolean performFinalAction() throws AlgebricksException {
        boolean modified = false;
        for (Map.Entry<LogicalVariable, List<ILogicalOperator>> entry : usedVarsMap.entrySet()) {
            // Perform replacement only if variable is referenced a single time.
            if (entry.getValue().size() == 1) {
                ILogicalOperator op = entry.getValue().get(0);
                if (!op.requiresVariableReferenceExpressions()) {
                    inlineVisitor.setOperator(op);
                    inlineVisitor.setTargetVariable(entry.getKey());
                    if (op.accept(inlineVisitor, inlineVisitor)) {
                        modified = true;
                    }
                    inlineVisitor.setTargetVariable(null);
                }
            }
        }
        return modified;
    }

    @Override
    protected boolean performBottomUpAction(AbstractLogicalOperator op) throws AlgebricksException {
        usedVars.clear();
        VariableUtilities.getUsedVariables(op, usedVars);
        for (LogicalVariable var : usedVars) {
            List<ILogicalOperator> opsUsingVar = usedVarsMap.get(var);
            if (opsUsingVar == null) {
                opsUsingVar = new ArrayList<ILogicalOperator>();
                usedVarsMap.put(var, opsUsingVar);
            }
            opsUsingVar.add(op);
        }
        return false;
    }
}
