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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes an AssignOperator below a UnionAll operator by creating an new AssignOperator below each of
 * the UnionAllOperator's branches with appropriate variable replacements.
 * This rule can help to enable other rules that are difficult to fire across a UnionAllOperator,
 * for example, eliminating common sub-expressions.
 * Example:
 * Before plan:
 * ...
 * assign [$$20, $$21] <- [funcA($$3), funcB($$6)]
 * union ($$1, $$2, $$3) ($$4, $$5, $$6)
 * union_branch_0
 * ...
 * union_branch_1
 * ...
 * After plan:
 * ...
 * union ($$1, $$2, $$3) ($$4, $$5, $$6) ($$22, $$24, $$20) ($$23, $$25, $$21)
 * assign [$$22, $$23] <- [funcA($$1), funcB($$4)]
 * union_branch_0
 * ...
 * assign [$$24, $$25] <- [funcA($$2), funcB($$5)]
 * union_branch_1
 * ...
 */
public class PushAssignBelowUnionAllRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
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
        inputs_loop: for (int i = 0; i < op.getInputs().size(); i++) {
            AbstractLogicalOperator childOp = (AbstractLogicalOperator) op.getInputs().get(i).getValue();
            if (childOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                continue;
            }
            AssignOperator assignOp = (AssignOperator) childOp;
            for (Mutable<ILogicalExpression> expr : assignOp.getExpressions()) {
                if (!expr.getValue().isFunctional()) {
                    continue inputs_loop;
                }
            }

            AbstractLogicalOperator childOfChildOp = (AbstractLogicalOperator) assignOp.getInputs().get(0).getValue();
            if (childOfChildOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
                continue;
            }
            UnionAllOperator unionOp = (UnionAllOperator) childOfChildOp;
            Set<LogicalVariable> assignUsedVars = new HashSet<>();
            VariableUtilities.getUsedVariables(assignOp, assignUsedVars);
            List<LogicalVariable> assignVars = assignOp.getVariables();
            AssignOperator[] newAssignOps = new AssignOperator[2];
            for (int j = 0; j < unionOp.getInputs().size(); j++) {
                newAssignOps[j] = createAssignBelowUnionAllBranch(unionOp, j, assignOp, assignUsedVars, context);
                if (newAssignOps[j] == null) {
                    continue inputs_loop;
                }
            }
            // Add original assign variables to the union variable mappings.
            for (int j = 0; j < assignVars.size(); j++) {
                LogicalVariable first = newAssignOps[0].getVariables().get(j);
                LogicalVariable second = newAssignOps[1].getVariables().get(j);
                unionOp.getVariableMappings().add(new Triple<>(first, second, assignVars.get(j)));
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
        AssignOperator newAssignOp = cloneAssignOperator(originalAssignOp, context, unionOp, inputIndex);
        if (newAssignOp == null) {
            return null;
        }
        newAssignOp.getInputs().add(new MutableObject<>(unionOp.getInputs().get(inputIndex).getValue()));
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
        context.computeAndSetTypeEnvironmentForOperator(newAssignOp);
        return newAssignOp;
    }

    /**
     * Clones the given assign operator changing the returned variables to be new ones.
     * Also, leaves the inputs of the clone clear. It returns null if the assign operator cannot be pushed.
     */
    private AssignOperator cloneAssignOperator(AssignOperator assignOp, IOptimizationContext context,
            UnionAllOperator unionOp, int inputIndex) throws AlgebricksException {
        List<LogicalVariable> vars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<>();
        int numVars = assignOp.getVariables().size();
        for (int i = 0; i < numVars; i++) {
            vars.add(context.newVar());
            ILogicalExpression clonedExpression = assignOp.getExpressions().get(i).getValue().cloneExpression();
            if (!modifyExpression(clonedExpression, unionOp, context, inputIndex)) {
                return null; // bail if the expression couldn't be modified according to the branch it is moved to
            }
            exprs.add(new MutableObject<>(clonedExpression));
        }
        AssignOperator assignCloneOp = new AssignOperator(vars, exprs);
        assignCloneOp.setSourceLocation(assignOp.getSourceLocation());
        assignCloneOp.setExecutionMode(assignOp.getExecutionMode());
        return assignCloneOp;
    }

    // modifies the cloned expression according to the branch it'll be moved to. returns true if successful.
    protected boolean modifyExpression(ILogicalExpression expression, UnionAllOperator unionOp,
            IOptimizationContext ctx, int inputIndex) throws AlgebricksException {
        // default implementation does not check specific expressions
        return true;
    }
}
