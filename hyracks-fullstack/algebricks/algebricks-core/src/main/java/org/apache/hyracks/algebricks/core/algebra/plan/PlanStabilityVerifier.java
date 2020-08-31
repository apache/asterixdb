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
import java.util.Deque;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

/**
 * Verifies whether there were any changes in the plan and fails if it finds:
 * <ul>
 * <li>new or deleted operator reference: {@code MutableObject<ILogicalOperator>}</li>
 * <li>new or deleted operator instance: {@code ILogicalOperator}</li>
 * <li>new or deleted expression reference: {@code MutableObject<ILogicalExpression>}</li>
 * <li>new or deleted expression instance {@code ILogicalExpression}</li>
 * </ul>
 *
 * Usage:
 * <ol>
 * <li>Invoke {@link #recordPlanSignature(Mutable)} to save the plan signature</li>
 * <li>Run an optimization rule on this plan
 * <li>If the rule said that it didn't make any changes then
 * invoke {@link #comparePlanSignature(Mutable)} to verify that</li>
 * <li>(optionally) Invoke {@link #discardPlanSignature()} to discard the recorded state</li>
 * </ol>
 */
public final class PlanStabilityVerifier {

    static final String MSG_CREATED = "created";

    static final String MSG_DELETED = "deleted";

    static final String MSG_OPERATOR_REFERENCE = "operator reference (Mutable) to";

    static final String MSG_OPERATOR_INSTANCE = "operator instance";

    static final String MSG_EXPRESSION_REFERENCE = "expression reference (Mutable) to";

    static final String MSG_EXPRESSION_INSTANCE = "expression instance";

    private static final String ERROR_MESSAGE_TEMPLATE = "%s %s (%s)";

    private static final int COLL_INIT_CAPACITY = 256;

    private final PlanSignatureRecorderVisitor recorderVisitor = new PlanSignatureRecorderVisitor();

    private final PlanStabilityVerifierVisitor verifierVisitor = new PlanStabilityVerifierVisitor();

    private final List<Mutable<ILogicalOperator>> opRefColl = new ArrayList<>(COLL_INIT_CAPACITY);

    private final List<ILogicalOperator> opColl = new ArrayList<>(COLL_INIT_CAPACITY);

    private final List<Mutable<ILogicalExpression>> exprRefColl = new ArrayList<>(COLL_INIT_CAPACITY);

    private final List<ILogicalExpression> exprColl = new ArrayList<>(COLL_INIT_CAPACITY);

    private final Deque<Mutable<ILogicalOperator>> workQueue = new ArrayDeque<>(COLL_INIT_CAPACITY);

    private final IPlanPrettyPrinter prettyPrinter;

    public PlanStabilityVerifier(IPlanPrettyPrinter prettyPrinter) {
        this.prettyPrinter = prettyPrinter;
    }

    public void recordPlanSignature(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        reset();
        walk(opRef, recorderVisitor);
    }

    public void comparePlanSignature(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        if (opRefColl.isEmpty()) {
            throw new IllegalStateException();
        }
        try {
            walk(opRef, verifierVisitor);
            ensureEmpty(opRefColl, MSG_DELETED, MSG_OPERATOR_REFERENCE, PlanStabilityVerifier::printOperator);
            ensureEmpty(opColl, MSG_DELETED, MSG_OPERATOR_INSTANCE, PlanStabilityVerifier::printOperator);
            ensureEmpty(exprRefColl, MSG_DELETED, MSG_EXPRESSION_REFERENCE, PlanStabilityVerifier::printExpression);
            ensureEmpty(exprColl, MSG_DELETED, MSG_EXPRESSION_INSTANCE, PlanStabilityVerifier::printExpression);
        } finally {
            reset();
        }
    }

    public void discardPlanSignature() {
        reset();
    }

    private void reset() {
        opRefColl.clear();
        opColl.clear();
        exprRefColl.clear();
        exprColl.clear();
    }

    private final class PlanSignatureRecorderVisitor extends AbstractStabilityCheckingVisitor {
        @Override
        public void visit(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
            ILogicalOperator op = opRef.getValue();
            // visit input ops of a multi-output op only if it's the first time we see this operator
            boolean skipInputs = OperatorPropertiesUtil.isMultiOutputOperator(op) && findItem(opColl, op) >= 0;

            opRefColl.add(opRef);
            opColl.add(op);

            if (!skipInputs) {
                super.visit(opRef);
            }
        }

        @Override
        protected void addChildToWorkQueue(Mutable<ILogicalOperator> childOpRef, boolean addFirst)
                throws AlgebricksException {
            ILogicalOperator childOp = childOpRef.getValue();
            if (!OperatorPropertiesUtil.isMultiOutputOperator(childOp) && opColl.contains(childOp)) {
                throw new AlgebricksException("cycle: " + printOperator(childOp, prettyPrinter));
            }
            super.addChildToWorkQueue(childOpRef, addFirst);
        }

        @Override
        protected void visitExpression(Mutable<ILogicalExpression> exprRef) {
            exprRefColl.add(exprRef);
            exprColl.add(exprRef.getValue());
        }
    }

    private final class PlanStabilityVerifierVisitor extends AbstractStabilityCheckingVisitor {
        @Override
        public void visit(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
            int idx = findItem(opRefColl, opRef);
            if (idx < 0) {
                raiseException(MSG_CREATED, MSG_OPERATOR_REFERENCE, printOperator(opRef, prettyPrinter));
            }
            opRefColl.set(idx, null);

            ILogicalOperator op = opRef.getValue();
            idx = findItem(opColl, op);
            if (idx < 0) {
                raiseException(MSG_CREATED, MSG_OPERATOR_INSTANCE, printOperator(op, prettyPrinter));
            }
            opColl.set(idx, null);

            // visit input ops of a multi-output op only if it's the last time we see this operator
            boolean skipInputs = OperatorPropertiesUtil.isMultiOutputOperator(op) && findItem(opColl, op) >= 0;
            if (!skipInputs) {
                super.visit(opRef);
            }
        }

        protected void visitExpression(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            int idx = findItem(exprRefColl, exprRef);
            if (idx < 0) {
                raiseException(MSG_CREATED, MSG_EXPRESSION_REFERENCE, printExpression(exprRef, prettyPrinter));
            }
            exprRefColl.set(idx, null);

            ILogicalExpression expr = exprRef.getValue();
            idx = findItem(exprColl, expr);
            if (idx < 0) {
                raiseException(MSG_CREATED, MSG_EXPRESSION_INSTANCE, printExpression(expr, prettyPrinter));
            }
            exprColl.set(idx, null);
        }
    }

    private abstract class AbstractStabilityCheckingVisitor
            implements IMutableReferenceVisitor<ILogicalOperator>, ILogicalExpressionReferenceTransform {

        @Override
        public void visit(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
            ILogicalOperator op = opRef.getValue();

            op.acceptExpressionTransform(this);

            for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
                addChildToWorkQueue(inputOpRef, true);
            }
            if (op instanceof AbstractOperatorWithNestedPlans) {
                for (ILogicalPlan nestedPlan : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                    for (Mutable<ILogicalOperator> nestedPlanRoot : nestedPlan.getRoots()) {
                        addChildToWorkQueue(nestedPlanRoot, false);
                    }
                }
            }
        }

        protected abstract void visitExpression(Mutable<ILogicalExpression> exprRef) throws AlgebricksException;

        @Override
        public final boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            visitExpression(exprRef);
            return false;
        }

        protected void addChildToWorkQueue(Mutable<ILogicalOperator> childOpRef, boolean addFirst)
                throws AlgebricksException {
            if (addFirst) {
                workQueue.addFirst(childOpRef);
            } else {
                workQueue.add(childOpRef);
            }

        }
    }

    private void raiseException(String operationKind, String referenceKind, String entity) throws AlgebricksException {
        String errorMessage = String.format(ERROR_MESSAGE_TEMPLATE, operationKind, referenceKind, entity);
        throw new AlgebricksException(errorMessage);
    }

    private <T> void ensureEmpty(List<T> list, String operationKind, String referenceKind,
            BiFunction<T, IPlanPrettyPrinter, String> printFunction) throws AlgebricksException {
        int idx = findNonNull(list);
        if (idx >= 0) {
            T listItem = list.get(idx);
            raiseException(operationKind, referenceKind, printFunction.apply(listItem, prettyPrinter));
        }
    }

    private static <T> int findItem(List<T> list, T item) {
        return indexOf(list, (listItem, paramItem) -> listItem == paramItem, item);
    }

    private static <T> int findNonNull(List<T> list) {
        return indexOf(list, (listItem, none) -> listItem != null, null);
    }

    private static <T, U> int indexOf(List<T> list, BiPredicate<T, U> predicate, U predicateParam) {
        for (int i = 0, n = list.size(); i < n; i++) {
            T listItem = list.get(i);
            if (predicate.test(listItem, predicateParam)) {
                return i;
            }
        }
        return -1;
    }

    static String printOperator(Mutable<ILogicalOperator> opRef, IPlanPrettyPrinter printer) {
        return printOperator(opRef.getValue(), printer);
    }

    static String printOperator(ILogicalOperator op, IPlanPrettyPrinter printer) {
        try {
            return printer.reset().printOperator((AbstractLogicalOperator) op, false).toString();
        } catch (AlgebricksException e) {
            // shouldn't happen
            return op.toString();
        }
    }

    static String printExpression(Mutable<ILogicalExpression> exprRef, IPlanPrettyPrinter printer) {
        return printExpression(exprRef.getValue(), printer);
    }

    static String printExpression(ILogicalExpression expr, IPlanPrettyPrinter printer) {
        try {
            return printer.reset().printExpression(expr).toString();
        } catch (AlgebricksException e) {
            // shouldn't happen
            return expr.toString();
        }
    }

    private void walk(Mutable<ILogicalOperator> opRef, IMutableReferenceVisitor<ILogicalOperator> visitor)
            throws AlgebricksException {
        if (!workQueue.isEmpty()) {
            throw new IllegalStateException();
        }
        Mutable<ILogicalOperator> currentOpRef = opRef;
        do {
            visitor.visit(currentOpRef);
            currentOpRef = workQueue.pollFirst();
        } while (currentOpRef != null);
    }

    private interface IMutableReferenceVisitor<T> {
        void visit(Mutable<T> ref) throws AlgebricksException;
    }
}