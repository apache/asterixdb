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
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes redundant variable mapping from the union-all operator as follows:
 * <p>
 * Before rule:
 * <pre>
 * union-all([$left, $right, $out_1], [$left, $right, $out_2])
 * </pre>
 * <p>
 * After rule:
 * <pre>
 * assign $out_2 := $out_1
 * union-all([$left, $right, $out_1])
 * </pre>
 * Notes:
 * <ul>
 * <li>
 * The new assign operator is annotated as {@link OperatorPropertiesUtil#MOVABLE non-movable}
 * to prevent it from being pushed back into the union-all by {@link PushMapOperatorThroughUnionRule}.
 * It is supposed to be removed later by {@link RemoveUnusedAssignAndAggregateRule}.
 * <li>
 * This rule is supposed to run during logical optimization stage because
 * it does not compute schema for the new assign operator
 * </ul>
 */
public final class RemoveRedundantVariablesInUnionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        UnionAllOperator unionAllOp = (UnionAllOperator) op;
        List<LogicalVariable> newAssignVars = null;
        List<Mutable<ILogicalExpression>> newAssignExprs = null;

        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMappings = unionAllOp.getVariableMappings();

        for (int i = 0; i < varMappings.size();) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = varMappings.get(i);
            LogicalVariable existingOutputVar = findVarMapping(varMappings, i, varMapping.first, varMapping.second);
            if (existingOutputVar != null) {
                LogicalVariable thisOutputVar = varMapping.third;
                if (newAssignVars == null) {
                    newAssignVars = new ArrayList<>();
                    newAssignExprs = new ArrayList<>();
                }
                VariableReferenceExpression existingOutputVarRef = new VariableReferenceExpression(existingOutputVar);
                existingOutputVarRef.setSourceLocation(unionAllOp.getSourceLocation());
                newAssignVars.add(thisOutputVar);
                newAssignExprs.add(new MutableObject<>(existingOutputVarRef));

                varMappings.remove(i);
            } else {
                i++;
            }
        }

        if (newAssignVars == null) {
            return false;
        }

        AssignOperator newAssignOp = new AssignOperator(newAssignVars, newAssignExprs);
        newAssignOp.setSourceLocation(unionAllOp.getSourceLocation());
        newAssignOp.getInputs().add(new MutableObject<>(unionAllOp));

        // this Assign is supposed to be removed later by RemoveUnusedAssignAndAggregateRule.
        // mark it as non-movable to prevent PushMapThroughUnionRule from pushing it back into the UnionAll operator
        OperatorPropertiesUtil.markMovable(newAssignOp, false);

        context.computeAndSetTypeEnvironmentForOperator(unionAllOp);
        context.computeAndSetTypeEnvironmentForOperator(newAssignOp);
        opRef.setValue(newAssignOp);

        context.addToDontApplySet(this, unionAllOp);

        return true;
    }

    private static LogicalVariable findVarMapping(
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMappings, int endIndexExclusive,
            LogicalVariable firstBranchVar, LogicalVariable secondBranchVar) {
        int n = Math.min(endIndexExclusive, varMappings.size());
        for (int i = 0; i < n; i++) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> t = varMappings.get(i);
            if (t.first.equals(firstBranchVar) && t.second.equals(secondBranchVar)) {
                return t.third;
            }
        }
        return null;
    }
}
