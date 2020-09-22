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

package org.apache.hyracks.algebricks.core.algebra.plan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

/**
 * Verifies plan structure and fails if it finds
 * <ul>
 * <li>a shared operator reference: {@code MutableObject<ILogicalOperator>}</li>
 * <li>a shared operator instance: {@code ILogicalOperator} (except if it's a multi-output operator)</li>
 * <li>a shared expression reference: {@code MutableObject<ILogicalExpression>}</li>
 * <li>a shared expression instance {@code ILogicalExpression} (except if it's a {@code ConstantExpression})</li>
 * </ul>
 */
public final class PlanStructureVerifier {

    private static final String ERROR_MESSAGE_TEMPLATE_1 = "shared %s (%s) in %s";

    private static final String ERROR_MESSAGE_TEMPLATE_2 = "shared %s (%s) between %s and %s";

    private static final String ERROR_MESSAGE_TEMPLATE_3 = "missing output type environment in %s";

    private static final String ERROR_MESSAGE_TEMPLATE_4 = "missing schema in %s";

    private final ExpressionReferenceVerifierVisitor exprVisitor = new ExpressionReferenceVerifierVisitor();

    private final Map<Mutable<ILogicalOperator>, ILogicalOperator> opRefMap = new IdentityHashMap<>();

    private final Map<ILogicalOperator, ILogicalOperator> opMap = new IdentityHashMap<>();

    private final Map<Mutable<ILogicalExpression>, ILogicalOperator> exprRefMap = new IdentityHashMap<>();

    private final Map<ILogicalExpression, ILogicalOperator> exprMap = new IdentityHashMap<>();

    private final Deque<Pair<Mutable<ILogicalOperator>, ILogicalOperator>> workQueue = new ArrayDeque<>();

    private final IPlanPrettyPrinter prettyPrinter;

    private final ITypingContext typeEnvProvider;

    private boolean ensureTypeEnv;

    private boolean ensureSchema;

    public PlanStructureVerifier(IPlanPrettyPrinter prettyPrinter, ITypingContext typeEnvProvider) {
        this.prettyPrinter = prettyPrinter;
        this.typeEnvProvider = typeEnvProvider;
    }

    public void verifyPlanStructure(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        reset();
        ILogicalOperator op = opRef.getValue();
        // if root has type-env/schema then ensure that all children have them too
        ensureTypeEnv = typeEnvProvider.getOutputTypeEnvironment(op) != null;
        ensureSchema = op.getSchema() != null;
        walk(opRef);
        reset();
    }

    private void reset() {
        opRefMap.clear();
        opMap.clear();
        exprRefMap.clear();
        exprMap.clear();
        ensureTypeEnv = false;
        ensureSchema = false;
    }

    private void walk(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        if (!workQueue.isEmpty()) {
            throw new IllegalStateException();
        }
        workQueue.add(new Pair<>(opRef, null));
        for (;;) {
            Pair<Mutable<ILogicalOperator>, ILogicalOperator> p = workQueue.pollFirst();
            if (p == null) {
                break;
            }
            Mutable<ILogicalOperator> currentOpRef = p.first;
            ILogicalOperator currentOp = currentOpRef.getValue();
            ILogicalOperator parentOp = p.second;

            List<Mutable<ILogicalOperator>> childOps = visitOp(currentOpRef, parentOp);

            for (Mutable<ILogicalOperator> childOpRef : childOps) {
                ILogicalOperator childOp = childOpRef.getValue();
                if (!OperatorPropertiesUtil.isMultiOutputOperator(childOp) && opMap.containsKey(childOp)) {
                    throw new AlgebricksException(
                            "cycle: " + PlanStabilityVerifier.printOperator(childOp, prettyPrinter));
                }
                workQueue.add(new Pair<>(childOpRef, currentOp));
            }
        }
    }

    private List<Mutable<ILogicalOperator>> visitOp(Mutable<ILogicalOperator> opRef, ILogicalOperator parentOp)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        ILogicalOperator firstParentOp;
        firstParentOp = opRefMap.put(opRef, parentOp);
        if (firstParentOp != null) {
            raiseException(PlanStabilityVerifier.MSG_OPERATOR_REFERENCE,
                    PlanStabilityVerifier.printOperator(op, prettyPrinter), firstParentOp, parentOp);
        }

        if (OperatorPropertiesUtil.isMultiOutputOperator(op) && opMap.containsKey(op)) {
            // don't visit input ops because we've already looked at them
            return Collections.emptyList();
        }

        firstParentOp = opMap.put(op, parentOp);
        if (firstParentOp != null) {
            raiseException(PlanStabilityVerifier.MSG_OPERATOR_INSTANCE,
                    PlanStabilityVerifier.printOperator(op, prettyPrinter), firstParentOp, parentOp);
        }

        exprVisitor.setOperator(op);
        op.acceptExpressionTransform(exprVisitor);

        if (ensureTypeEnv && typeEnvProvider.getOutputTypeEnvironment(op) == null) {
            throw new AlgebricksException(
                    String.format(ERROR_MESSAGE_TEMPLATE_3, PlanStabilityVerifier.printOperator(op, prettyPrinter)));
        }
        if (ensureSchema && op.getSchema() == null) {
            throw new AlgebricksException(
                    String.format(ERROR_MESSAGE_TEMPLATE_4, PlanStabilityVerifier.printOperator(op, prettyPrinter)));
        }

        List<Mutable<ILogicalOperator>> children = op.getInputs();
        if (op instanceof AbstractOperatorWithNestedPlans) {
            children = new ArrayList<>(children);
            for (ILogicalPlan nestedPlan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                children.addAll(nestedPlan.getRoots());
            }
        }
        return children;
    }

    private void raiseException(String sharedReferenceKind, String sharedEntity, ILogicalOperator firstOp,
            ILogicalOperator secondOp) throws AlgebricksException {
        String errorMessage;
        if (firstOp == secondOp) {
            errorMessage = String.format(ERROR_MESSAGE_TEMPLATE_1, sharedReferenceKind, sharedEntity,
                    PlanStabilityVerifier.printOperator(firstOp, prettyPrinter));
        } else {
            errorMessage = String.format(ERROR_MESSAGE_TEMPLATE_2, sharedReferenceKind, sharedEntity,
                    PlanStabilityVerifier.printOperator(firstOp, prettyPrinter),
                    PlanStabilityVerifier.printOperator(secondOp, prettyPrinter));
        }
        throw new AlgebricksException(errorMessage);
    }

    private final class ExpressionReferenceVerifierVisitor implements ILogicalExpressionReferenceTransform {

        private ILogicalOperator currentOp;

        void setOperator(ILogicalOperator currentOp) {
            this.currentOp = Objects.requireNonNull(currentOp);
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression expr = exprRef.getValue();
            ILogicalOperator firstOp;
            firstOp = exprRefMap.put(exprRef, currentOp);
            if (firstOp != null) {
                raiseException(PlanStabilityVerifier.MSG_EXPRESSION_REFERENCE,
                        PlanStabilityVerifier.printExpression(expr, prettyPrinter), firstOp, currentOp);
            }
            if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                firstOp = exprMap.put(expr, currentOp);
                if (firstOp != null) {
                    raiseException(PlanStabilityVerifier.MSG_EXPRESSION_INSTANCE,
                            PlanStabilityVerifier.printExpression(expr, prettyPrinter), firstOp, currentOp);
                }
            }
            return false;
        }
    }
}