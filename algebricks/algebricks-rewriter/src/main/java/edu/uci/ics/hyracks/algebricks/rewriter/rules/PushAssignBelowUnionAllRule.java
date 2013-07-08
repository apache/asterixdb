/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes an AssignOperator below a UnionAll operator by creating an new AssignOperator below each of 
 * the UnionAllOperator's branches with appropriate variable replacements.
 * This rule can help to enable other rules that are difficult to fire across a UnionAllOperator, 
 * for example, eliminating common sub-expressions.
 * 
 * Example:
 * 
 * Before plan:
 * ...
 * assign [$$20, $$21] <- [funcA($$3), funcB($$6)]
 *   union ($$1, $$2, $$3) ($$4, $$5, $$6)
 *     union_branch_0
 *       ...
 *     union_branch_1
 *       ...
 *     
 * After plan:
 * ...
 * union ($$1, $$2, $$3) ($$4, $$5, $$6) ($$22, $$24, $$20) ($$23, $$25, $$21)
 *   assign [$$22, $$23] <- [funcA($$1), funcB($$4)]
 *     union_branch_0
 *       ...
 *   assign [$$24, $$25] <- [funcA($$2), funcB($$5)]
 *     union_branch_1
 *       ...
 */
public class PushAssignBelowUnionAllRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (!op.hasInputs()) {
            return false;
        }

        boolean modified = false;
        for (int i = 0; i < op.getInputs().size(); i++) {
            AbstractLogicalOperator childOp = (AbstractLogicalOperator) op.getInputs().get(i).getValue();
            if (childOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                continue;
            }
            AssignOperator assignOp = (AssignOperator) childOp;

            AbstractLogicalOperator childOfChildOp = (AbstractLogicalOperator) assignOp.getInputs().get(0).getValue();
            if (childOfChildOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
                continue;
            }
            UnionAllOperator unionOp = (UnionAllOperator) childOfChildOp;

            Set<LogicalVariable> assignUsedVars = new HashSet<LogicalVariable>();
            VariableUtilities.getUsedVariables(assignOp, assignUsedVars);

            List<LogicalVariable> assignVars = assignOp.getVariables();

            AssignOperator[] newAssignOps = new AssignOperator[2];
            for (int j = 0; j < unionOp.getInputs().size(); j++) {
                newAssignOps[j] = createAssignBelowUnionAllBranch(unionOp, j, assignOp, assignUsedVars, context);
            }
            // Add original assign variables to the union variable mappings.
            for (int j = 0; j < assignVars.size(); j++) {
                LogicalVariable first = newAssignOps[0].getVariables().get(j);
                LogicalVariable second = newAssignOps[1].getVariables().get(j);
                Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(
                        first, second, assignVars.get(j));
                unionOp.getVariableMappings().add(varMapping);
            }
            context.computeAndSetTypeEnvironmentForOperator(unionOp);

            // Remove original assign operator.
            op.getInputs().set(i, assignOp.getInputs().get(0));
            context.computeAndSetTypeEnvironmentForOperator(op);
            modified = true;
        }

        return modified;
    }

    private AssignOperator createAssignBelowUnionAllBranch(UnionAllOperator unionOp, int inputIndex,
            AssignOperator originalAssignOp, Set<LogicalVariable> assignUsedVars, IOptimizationContext context)
            throws AlgebricksException {
        AssignOperator newAssignOp = cloneAssignOperator(originalAssignOp, context);
        newAssignOp.getInputs()
                .add(new MutableObject<ILogicalOperator>(unionOp.getInputs().get(inputIndex).getValue()));
        context.computeAndSetTypeEnvironmentForOperator(newAssignOp);
        unionOp.getInputs().get(inputIndex).setValue(newAssignOp);
        int numVarMappings = unionOp.getVariableMappings().size();
        for (int i = 0; i < numVarMappings; i++) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = unionOp.getVariableMappings().get(i);
            if (assignUsedVars.contains(varMapping.third)) {
                LogicalVariable replacementVar;
                if (inputIndex == 0) {
                    replacementVar = varMapping.first;
                } else {
                    replacementVar = varMapping.second;
                }
                VariableUtilities.substituteVariables(newAssignOp, varMapping.third, replacementVar, context);
            }
        }
        return newAssignOp;
    }

    /**
     * Clones the given assign operator changing the returned variables to be new ones.
     * Also, leaves the inputs of the clone clear.
     */
    private AssignOperator cloneAssignOperator(AssignOperator assignOp, IOptimizationContext context) {
        List<LogicalVariable> vars = new ArrayList<LogicalVariable>();
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
        int numVars = assignOp.getVariables().size();
        for (int i = 0; i < numVars; i++) {
            vars.add(context.newVar());
            exprs.add(new MutableObject<ILogicalExpression>(assignOp.getExpressions().get(i).getValue()
                    .cloneExpression()));
        }
        AssignOperator assignCloneOp = new AssignOperator(vars, exprs);
        assignCloneOp.setExecutionMode(assignOp.getExecutionMode());
        return assignCloneOp;
    }
}
