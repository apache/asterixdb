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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * @author kereno, ecarm002, ildar.absalyamov
 *         Pushes down 'map' operator through both branches of the union-all operator
 *         Before rule:
 *         ============
 *         map_op
 *         union-all (left_branch, right_branch, res)
 *         left_branch
 *         left_op_1
 *         ...
 *         right_branch
 *         right_op_1
 *
 *         After rule:
 *         ============
 *         union-all (left_branch, right_branch, res)
 *         left_branch
 *         map_op
 *         left_op_1
 *         right_branch
 *         map_op
 *         right_op_1
 */
public abstract class PushMapOperatorThroughUnionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    protected boolean isOperatorKindPushableThroughUnion(ILogicalOperator op) {
        return op.isMap();
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        boolean pushable = isOperatorKindPushableThroughUnion(op) && OperatorPropertiesUtil.isMovable(op);
        if (!pushable) {
            return false;
        }

        Mutable<ILogicalOperator> inputOpRef = op.getInputs().get(0);
        AbstractLogicalOperator inputOp = (AbstractLogicalOperator) inputOpRef.getValue();
        if (inputOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }
        UnionAllOperator unionAllOp = (UnionAllOperator) inputOp;

        List<LogicalVariable> opProducedVars = new ArrayList<>();
        VariableUtilities.getProducedVariables(op, opProducedVars);

        Set<LogicalVariable> opUsedVars = new HashSet<>();
        VariableUtilities.getUsedVariables(op, opUsedVars);

        Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> leftBranchPair =
                insertIntoBranch(op, opUsedVars, unionAllOp, 0, context);
        if (leftBranchPair == null) {
            return false;
        }
        ILogicalOperator leftBranchRootOp = leftBranchPair.first;
        //  < original produced var, new produced variable in left branch >
        Map<LogicalVariable, LogicalVariable> leftBranchProducedVarMap = leftBranchPair.second;

        Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> rightBranchPair =
                insertIntoBranch(op, opUsedVars, unionAllOp, 1, context);
        if (rightBranchPair == null) {
            return false;
        }
        ILogicalOperator rightBranchRootOp = rightBranchPair.first;
        // < original produced var, new produced variable in right branch >
        Map<LogicalVariable, LogicalVariable> rightBranchProducedVarMap = rightBranchPair.second;

        UnionAllOperator newUnionAllOp = (UnionAllOperator) OperatorManipulationUtil.deepCopy(unionAllOp);
        newUnionAllOp.getInputs().add(new MutableObject<>(leftBranchRootOp));
        newUnionAllOp.getInputs().add(new MutableObject<>(rightBranchRootOp));

        for (LogicalVariable opProducedVar : opProducedVars) {
            LogicalVariable leftBranchProducedVar = leftBranchProducedVarMap.get(opProducedVar);
            if (leftBranchProducedVar == null) {
                throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, op.getSourceLocation(),
                        "Cannot find " + opProducedVar);
            }
            LogicalVariable rightBranchProducedVar = rightBranchProducedVarMap.get(opProducedVar);
            if (rightBranchProducedVar == null) {
                throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, op.getSourceLocation(),
                        "Cannot find " + opProducedVar);
            }

            newUnionAllOp.getVariableMappings()
                    .add(new Triple<>(leftBranchProducedVar, rightBranchProducedVar, opProducedVar));
        }

        context.computeAndSetTypeEnvironmentForOperator(newUnionAllOp);
        opRef.setValue(newUnionAllOp);

        return true;
    }

    private Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> insertIntoBranch(AbstractLogicalOperator op,
            Set<LogicalVariable> opUsedVars, UnionAllOperator unionAllOp, int branchIdx, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator branchRootOp = unionAllOp.getInputs().get(branchIdx).getValue();
        Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> newBranchRootOpPair =
                deepCopyForBranch(op, opUsedVars, unionAllOp, branchIdx, context);
        if (newBranchRootOpPair == null) {
            return null;
        }

        ILogicalOperator newBranchRootOp = newBranchRootOpPair.first;
        newBranchRootOp.getInputs().get(0).setValue(branchRootOp);
        context.computeAndSetTypeEnvironmentForOperator(newBranchRootOp);

        // [ operator, < original produced var, new variable (produced in branch) > ]
        return newBranchRootOpPair;
    }

    protected Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> deepCopyForBranch(ILogicalOperator op,
            Set<LogicalVariable> opUsedVars, UnionAllOperator unionAllOp, int branchIdx, IOptimizationContext context)
            throws AlgebricksException {
        // < union-output-var, union-branch-var >
        LinkedHashMap<LogicalVariable, LogicalVariable> usedVarsMapping = new LinkedHashMap<>();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : unionAllOp.getVariableMappings()) {
            LogicalVariable unionOutputVar = t.third;
            if (opUsedVars.contains(unionOutputVar)) {
                LogicalVariable branchVar = branchIdx == 0 ? t.first : t.second;
                usedVarsMapping.put(unionOutputVar, branchVar);
            }
        }

        LogicalOperatorDeepCopyWithNewVariablesVisitor deepCopyVisitor =
                new LogicalOperatorDeepCopyWithNewVariablesVisitor(context, null, usedVarsMapping, true);
        ILogicalOperator newOp = deepCopyVisitor.deepCopy(op);

        return new Pair<>(newOp, deepCopyVisitor.getInputToOutputVariableMapping());
    }
}