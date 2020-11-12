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
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamLimitPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CopyLimitDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.LIMIT) {
            return false;
        }
        LimitOperator limitOp = (LimitOperator) op;
        if (!limitOp.isTopmostLimitOp()) {
            return false;
        }

        List<LogicalVariable> limitUsedVars = new ArrayList<>();
        VariableUtilities.getUsedVariables(limitOp, limitUsedVars);

        List<ILogicalOperator> safeOps = new ArrayList<>();
        List<LogicalVariable> tmpCandidateProducedVars = new ArrayList<>();
        ILogicalOperator limitInputOp = limitOp.getInputs().get(0).getValue();

        findSafeOpsInSubtree(limitInputOp, limitUsedVars, safeOps, tmpCandidateProducedVars);
        if (safeOps.isEmpty()) {
            return false;
        }

        SourceLocation sourceLoc = limitOp.getSourceLocation();

        for (ILogicalOperator safeOp : safeOps) {
            for (Mutable<ILogicalOperator> unsafeOpRef : safeOp.getInputs()) {
                ILogicalOperator unsafeOp = unsafeOpRef.getValue();
                ILogicalExpression maxObjectsExpr = limitOp.getMaxObjects().getValue();
                ILogicalExpression newMaxObjectsExpr;
                if (limitOp.getOffset().getValue() == null) {
                    newMaxObjectsExpr = maxObjectsExpr.cloneExpression();
                } else {
                    // Need to add an offset to the given limit value
                    // since the original topmost limit will use the offset value.
                    // We can't apply the offset multiple times.
                    IFunctionInfo finfoAdd =
                            context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NUMERIC_ADD);
                    List<Mutable<ILogicalExpression>> addArgs = new ArrayList<>(2);
                    addArgs.add(new MutableObject<>(maxObjectsExpr.cloneExpression()));
                    addArgs.add(new MutableObject<>(limitOp.getOffset().getValue().cloneExpression()));
                    ScalarFunctionCallExpression maxPlusOffset = new ScalarFunctionCallExpression(finfoAdd, addArgs);
                    maxPlusOffset.setSourceLocation(sourceLoc);
                    newMaxObjectsExpr = maxPlusOffset;
                }
                LimitOperator limitCloneOp = new LimitOperator(newMaxObjectsExpr, false);
                limitCloneOp.setSourceLocation(sourceLoc);
                limitCloneOp.setPhysicalOperator(new StreamLimitPOperator());
                limitCloneOp.getInputs().add(new MutableObject<>(unsafeOp));
                limitCloneOp.setExecutionMode(unsafeOp.getExecutionMode());
                context.computeAndSetTypeEnvironmentForOperator(limitCloneOp);
                limitCloneOp.recomputeSchema();
                unsafeOpRef.setValue(limitCloneOp);
            }
        }

        context.addToDontApplySet(this, limitOp);

        return true;
    }

    private boolean findSafeOpsInSubtree(ILogicalOperator candidateOp, List<LogicalVariable> limitUsedVars,
            Collection<? super ILogicalOperator> outSafeOps, List<LogicalVariable> tmpCandidateProducedVars)
            throws AlgebricksException {
        ILogicalOperator safeOp = null;

        while (isSafeOpCandidate(candidateOp)) {
            tmpCandidateProducedVars.clear();
            VariableUtilities.getProducedVariables(candidateOp, tmpCandidateProducedVars);
            if (!OperatorPropertiesUtil.disjoint(limitUsedVars, tmpCandidateProducedVars)) {
                break;
            }

            List<Mutable<ILogicalOperator>> candidateOpInputs = candidateOp.getInputs();
            if (candidateOpInputs.size() > 1) {
                boolean foundSafeOpInBranch = false;
                for (Mutable<ILogicalOperator> inputOpRef : candidateOpInputs) {
                    foundSafeOpInBranch |= findSafeOpsInSubtree(inputOpRef.getValue(), limitUsedVars, outSafeOps,
                            tmpCandidateProducedVars);
                }
                if (!foundSafeOpInBranch) {
                    outSafeOps.add(candidateOp);
                }
                return true;
            }

            safeOp = candidateOp;
            candidateOp = candidateOpInputs.get(0).getValue();
        }

        if (safeOp != null) {
            outSafeOps.add(safeOp);
            return true;
        } else {
            return false;
        }
    }

    private static boolean isSafeOpCandidate(ILogicalOperator op) {
        switch (op.getOperatorTag()) {
            case UNIONALL:
            case SUBPLAN: // subplan is a 'map' but is not yet marked as such
                return true;
            // exclude following 'map' operators because they change cardinality
            case SELECT:
            case UNNEST:
            case UNNEST_MAP:
                return false;
            default:
                return op.getInputs().size() == 1 && op.isMap();
        }
    }
}
