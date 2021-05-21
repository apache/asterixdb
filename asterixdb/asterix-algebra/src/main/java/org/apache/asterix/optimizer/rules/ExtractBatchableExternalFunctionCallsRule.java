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

package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ExtractBatchableExternalFunctionCallsRule implements IAlgebraicRewriteRule {

    private final ExtractFunctionCallsVisitor extractVisitor = new ExtractFunctionCallsVisitor();

    private Boolean isRuleEnabled;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (isRuleEnabled == null) {
            isRuleEnabled = SetAsterixPhysicalOperatorsRule.isBatchAssignEnabled(context);
        }
        if (!isRuleEnabled) {
            return false;
        }

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        List<Mutable<ILogicalExpression>> assignTopExprRefs = Collections.emptyList();
        switch (op.getOperatorTag()) {
            case ASSIGN:
                assignTopExprRefs = ((AssignOperator) op).getExpressions();
                break;
            case SELECT:
                break;
            default:
                return false;
        }

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);

        extractVisitor.reset(context, assignTopExprRefs);
        if (!op.acceptExpressionTransform(extractVisitor)) {
            return false;
        }
        SourceLocation sourceLoc = op.getSourceLocation();

        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        for (int i = 0, ln = extractVisitor.assignVars.size(); i < ln; i++) {
            List<LogicalVariable> assignVarList = extractVisitor.assignVars.get(i);
            List<Mutable<ILogicalExpression>> assignExprList = extractVisitor.assignExprs.get(i);
            AssignOperator assignOp = new AssignOperator(assignVarList, assignExprList);
            assignOp.setSourceLocation(sourceLoc);
            assignOp.getInputs().add(new MutableObject<>(inputOp));
            context.computeAndSetTypeEnvironmentForOperator(assignOp);
            assignOp.recomputeSchema();
            OperatorPropertiesUtil.markMovable(assignOp, false);

            context.addToDontApplySet(this, assignOp);
            for (LogicalVariable assignVar : assignVarList) {
                context.addNotToBeInlinedVar(assignVar);
            }

            inputOp = assignOp;
        }

        op.getInputs().clear();
        op.getInputs().add(new MutableObject<>(inputOp));
        context.computeAndSetTypeEnvironmentForOperator(op);
        op.recomputeSchema();
        return true;
    }

    private static final class ExtractFunctionCallsVisitor implements ILogicalExpressionReferenceTransform {

        private final List<List<LogicalVariable>> assignVars = new ArrayList<>();

        private final List<List<Mutable<ILogicalExpression>>> assignExprs = new ArrayList<>();

        private final List<LogicalVariable> usedVarList = new ArrayList<>();

        private IOptimizationContext context;

        private List<Mutable<ILogicalExpression>> dontExtractFromExprRefs;

        public void reset(IOptimizationContext context, List<Mutable<ILogicalExpression>> dontExtractFromExprRefs) {
            this.context = context;
            this.dontExtractFromExprRefs = dontExtractFromExprRefs;
            assignVars.clear();
            assignExprs.clear();
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression expr = exprRef.getValue();
            switch (expr.getExpressionTag()) {
                case FUNCTION_CALL:
                    AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
                    boolean applied = false;
                    for (Mutable<ILogicalExpression> argRef : callExpr.getArguments()) {
                        applied |= transform(argRef);
                    }
                    AbstractFunctionCallExpression.FunctionKind fnKind = callExpr.getKind();
                    IFunctionInfo fnInfo = callExpr.getFunctionInfo();
                    if (ExternalFunctionCompilerUtil.supportsBatchInvocation(fnKind, fnInfo)
                            && callExpr.isFunctional()) {
                        // need to extract non-variable arguments into separate ASSIGNS
                        // because batched assign can only operate on columns
                        for (Mutable<ILogicalExpression> argRef : callExpr.getArguments()) {
                            ILogicalExpression argExpr = argRef.getValue();
                            if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                                LogicalVariable newArgVar = context.newVar();
                                VariableReferenceExpression newArgVarRef = new VariableReferenceExpression(newArgVar);
                                newArgVarRef.setSourceLocation(expr.getSourceLocation());
                                saveAssignVar(newArgVar, argExpr);
                                argRef.setValue(newArgVarRef);
                                applied = true;
                            }
                        }
                        // need extract function call itself into a separate ASSIGN
                        // (unless it's already a top level expression of the ASSIGN operator we're visiting)
                        boolean dontExtractExprRef = indexOf(dontExtractFromExprRefs, exprRef) >= 0;
                        if (!dontExtractExprRef) {
                            LogicalVariable newVar = context.newVar();
                            VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
                            newVarRef.setSourceLocation(expr.getSourceLocation());
                            saveAssignVar(newVar, expr);
                            exprRef.setValue(newVarRef);
                            applied = true;
                        }
                    }
                    return applied;
                case VARIABLE:
                case CONSTANT:
                    return false;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, expr.getSourceLocation(),
                            expr.getExpressionTag().toString());
            }
        }

        private void saveAssignVar(LogicalVariable var, ILogicalExpression expr) {
            List<LogicalVariable> assignVarList = null;
            List<Mutable<ILogicalExpression>> assignExprList = null;

            if (!assignVars.isEmpty()) {
                usedVarList.clear();
                expr.getUsedVariables(usedVarList);
                int candidateVarListIdx = assignVars.size() - 1;
                List<LogicalVariable> candidateVarList = assignVars.get(candidateVarListIdx);
                if (OperatorPropertiesUtil.disjoint(candidateVarList, usedVarList)) {
                    assignVarList = candidateVarList;
                    assignExprList = assignExprs.get(candidateVarListIdx);
                }
            }

            if (assignVarList == null) {
                // first time, or couldn't find a disjoint var list
                assignVarList = new ArrayList<>();
                assignExprList = new ArrayList<>();
                assignVars.add(assignVarList);
                assignExprs.add(assignExprList);
            }

            assignVarList.add(var);
            assignExprList.add(new MutableObject<>(expr));
        }

        public static int indexOf(List<Mutable<ILogicalExpression>> exprList, Mutable<ILogicalExpression> exprRef) {
            return OperatorManipulationUtil.indexOf(exprList,
                    (listItemExprRef, paramExprRef) -> listItemExprRef == paramExprRef, exprRef);
        }
    }
}
