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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
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

    private static final String ERROR_MESSAGE_TEMPLATE_5 =
            "produced variables %s that intersect used variables %s on %s in %s";

    private static final String ERROR_MESSAGE_TEMPLATE_6 = "undefined used variables %s in %s";

    public static final Comparator<LogicalVariable> VARIABLE_CMP = Comparator.comparing(LogicalVariable::toString);

    private final ExpressionReferenceVerifierVisitor exprVisitor = new ExpressionReferenceVerifierVisitor();

    private final Map<Mutable<ILogicalOperator>, ILogicalOperator> opRefMap = new IdentityHashMap<>();

    private final Map<ILogicalOperator, ILogicalOperator> opMap = new IdentityHashMap<>();

    private final Map<Mutable<ILogicalExpression>, ILogicalOperator> exprRefMap = new IdentityHashMap<>();

    private final Map<ILogicalExpression, ILogicalOperator> exprMap = new IdentityHashMap<>();

    private final Deque<Pair<Mutable<ILogicalOperator>, ILogicalOperator>> workQueue = new ArrayDeque<>();

    private final Set<LogicalVariable> tmpVarSet1 = new HashSet<>();

    private final Set<LogicalVariable> tmpVarSet2 = new HashSet<>();

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

        checkOperatorTypeEnvironment(op);
        checkOperatorSchema(op);
        checkOperatorVariables(op);

        List<Mutable<ILogicalOperator>> children = op.getInputs();
        if (op instanceof AbstractOperatorWithNestedPlans) {
            children = new ArrayList<>(children);
            for (ILogicalPlan nestedPlan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                children.addAll(nestedPlan.getRoots());
            }
        }
        return children;
    }

    private void checkOperatorTypeEnvironment(ILogicalOperator op) throws AlgebricksException {
        if (ensureTypeEnv && typeEnvProvider.getOutputTypeEnvironment(op) == null) {
            throw new AlgebricksException(
                    String.format(ERROR_MESSAGE_TEMPLATE_3, PlanStabilityVerifier.printOperator(op, prettyPrinter)));
        }
    }

    private void checkOperatorSchema(ILogicalOperator op) throws AlgebricksException {
        if (ensureSchema && op.getSchema() == null) {
            throw new AlgebricksException(
                    String.format(ERROR_MESSAGE_TEMPLATE_4, PlanStabilityVerifier.printOperator(op, prettyPrinter)));
        }
    }

    private void checkOperatorVariables(ILogicalOperator op) throws AlgebricksException {
        if (op instanceof AbstractOperatorWithNestedPlans) {
            return;
        }

        tmpVarSet1.clear();
        VariableUtilities.getUsedVariables(op, tmpVarSet1);
        if (!tmpVarSet1.isEmpty()) {
            ensureUsedVarsAreDefined(op, tmpVarSet1);
            ensureProducedVarsDisjointFromUsedVars(op, tmpVarSet1);
        }
    }

    private void ensureUsedVarsAreDefined(ILogicalOperator op, Collection<LogicalVariable> usedVars)
            throws AlgebricksException {
        if (!ensureTypeEnv) {
            return;
        }

        tmpVarSet2.clear();
        tmpVarSet2.addAll(usedVars);
        Set<LogicalVariable> usedVarsCopy = tmpVarSet2;

        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            ILogicalOperator childOp = childRef.getValue();
            IVariableTypeEnvironment childOpTypeEnv = typeEnvProvider.getOutputTypeEnvironment(childOp);
            if (childOpTypeEnv == null) {
                throw new AlgebricksException(String.format(ERROR_MESSAGE_TEMPLATE_3,
                        PlanStabilityVerifier.printOperator(childOp, prettyPrinter)));
            }
            for (Iterator<LogicalVariable> i = usedVarsCopy.iterator(); i.hasNext();) {
                LogicalVariable usedVar = i.next();
                if (childOpTypeEnv.getVarType(usedVar) != null) {
                    i.remove();
                }
            }
        }
        if (!usedVarsCopy.isEmpty()) {
            throw new AlgebricksException(String.format(ERROR_MESSAGE_TEMPLATE_6, sorted(usedVarsCopy, VARIABLE_CMP),
                    PlanStabilityVerifier.printOperator(op, prettyPrinter)));
        }
    }

    private void ensureProducedVarsDisjointFromUsedVars(ILogicalOperator op, Set<LogicalVariable> usedVars)
            throws AlgebricksException {
        tmpVarSet2.clear();
        VariableUtilities.getProducedVariables(op, tmpVarSet2);
        Set<LogicalVariable> producedVars = tmpVarSet2;

        Collection<LogicalVariable> intersection = CollectionUtils.intersection(producedVars, usedVars);
        if (!intersection.isEmpty()) {
            throw new AlgebricksException(String.format(ERROR_MESSAGE_TEMPLATE_5, sorted(producedVars, VARIABLE_CMP),
                    sorted(usedVars, VARIABLE_CMP), sorted(intersection, VARIABLE_CMP),
                    PlanStabilityVerifier.printOperator(op, prettyPrinter)));
        }
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

    private <T> List<T> sorted(Collection<T> inColl, Comparator<T> comparator) {
        return inColl.stream().sorted(comparator).collect(Collectors.toList());
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