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
package org.apache.hyracks.algebricks.core.algebra.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public class OperatorPropertiesUtil {

    public static final String MOVABLE = "isMovable";

    private OperatorPropertiesUtil() {
    }

    public static <T> boolean disjoint(Collection<T> c1, Collection<T> c2) {
        for (T m : c1) {
            if (c2.contains(m)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds the free variables of the operator to the given set.
     *
     * @param op
     * @param freeVars
     */
    public static void getFreeVariablesInOp(ILogicalOperator op, Set<LogicalVariable> freeVars)
            throws AlgebricksException {
        // Obs: doesn't return expected result for op. with nested plans.
        VariableUtilities.getUsedVariables(op, freeVars);
        HashSet<LogicalVariable> produced = new HashSet<>();
        VariableUtilities.getProducedVariables(op, produced);
        for (LogicalVariable v : produced) {
            freeVars.remove(v);
        }
    }

    /**
     * Adds the free variables of the plan rooted at that operator to the
     * collection provided.
     *
     * @param op
     * @param freeVars
     *            - The collection to which the free variables will be added.
     */
    public static void getFreeVariablesInSelfOrDesc(AbstractLogicalOperator op, Set<LogicalVariable> freeVars)
            throws AlgebricksException {
        HashSet<LogicalVariable> produced = new HashSet<>();
        VariableUtilities.getProducedVariables(op, produced);
        for (LogicalVariable v : produced) {
            freeVars.remove(v);
        }

        HashSet<LogicalVariable> used = new HashSet<>();
        VariableUtilities.getUsedVariables(op, used);
        for (LogicalVariable v : used) {
            freeVars.add(v);
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans s = (AbstractOperatorWithNestedPlans) op;
            getFreeVariablesInSubplans(s, freeVars);
        }
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) i.getValue(), freeVars);
        }
    }

    /**
     * Adds the free variables of the operator path from
     * op to dest, where dest is a direct/indirect input operator of op in the query plan.
     *
     * @param op
     *            , the start operator.
     * @param dest
     *            , the destination operator (a direct/indirect input operator).
     * @param freeVars
     *            - The collection to which the free variables will be added.
     */
    public static void getFreeVariablesInPath(ILogicalOperator op, ILogicalOperator dest, Set<LogicalVariable> freeVars)
            throws AlgebricksException {
        Set<LogicalVariable> producedVars = new ListSet<>();
        VariableUtilities.getLiveVariables(op, freeVars);
        collectUsedAndProducedVariablesInPath(op, dest, freeVars, producedVars);
        freeVars.removeAll(producedVars);
    }

    /**
     * @param op
     *            , the start operator.
     * @param dest
     *            , the destination operator (a direct/indirect input operator).
     * @param usedVars
     *            , the collection of used variables.
     * @param producedVars
     *            , the collection of produced variables.
     * @return if the current operator is on the path from the original start operator to the destination operator.
     * @throws AlgebricksException
     */
    private static boolean collectUsedAndProducedVariablesInPath(ILogicalOperator op, ILogicalOperator dest,
            Set<LogicalVariable> usedVars, Set<LogicalVariable> producedVars) throws AlgebricksException {
        if (op == dest) {
            return true;
        }
        if (((AbstractLogicalOperator) op).hasNestedPlans()) {
            AbstractOperatorWithNestedPlans a = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : a.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    if (collectUsedAndProducedVariablesInPath(r.getValue(), dest, usedVars, producedVars)) {
                        VariableUtilities.getUsedVariables(r.getValue(), usedVars);
                        VariableUtilities.getProducedVariables(r.getValue(), producedVars);
                        return true;
                    }
                }
            }
        }
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            if (collectUsedAndProducedVariablesInPath(childRef.getValue(), dest, usedVars, producedVars)) {
                VariableUtilities.getUsedVariables(op, usedVars);
                VariableUtilities.getProducedVariables(op, producedVars);
                return true;
            }
        }
        return false;
    }

    public static void getFreeVariablesInSubplans(AbstractOperatorWithNestedPlans op, Set<LogicalVariable> freeVars)
            throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) r.getValue(), freeVars);
            }
        }
    }

    public static boolean hasFreeVariablesInSelfOrDesc(AbstractLogicalOperator op) throws AlgebricksException {
        HashSet<LogicalVariable> free = new HashSet<>();
        getFreeVariablesInSelfOrDesc(op, free);
        return !free.isEmpty();
    }

    public static boolean hasFreeVariables(ILogicalOperator op) throws AlgebricksException {
        HashSet<LogicalVariable> free = new HashSet<>();
        getFreeVariablesInOp(op, free);
        return !free.isEmpty();
    }

    public static void computeSchemaAndPropertiesRecIfNull(AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        if (op.getSchema() == null) {
            for (Mutable<ILogicalOperator> i : op.getInputs()) {
                computeSchemaAndPropertiesRecIfNull((AbstractLogicalOperator) i.getValue(), context);
            }
            if (op.hasNestedPlans()) {
                AbstractOperatorWithNestedPlans a = (AbstractOperatorWithNestedPlans) op;
                for (ILogicalPlan p : a.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> r : p.getRoots()) {
                        computeSchemaAndPropertiesRecIfNull((AbstractLogicalOperator) r.getValue(), context);
                    }
                }
            }
            op.recomputeSchema();
            op.computeDeliveredPhysicalProperties(context);
        }
    }

    public static void computeSchemaRecIfNull(AbstractLogicalOperator op) throws AlgebricksException {
        if (op.getSchema() == null) {
            for (Mutable<ILogicalOperator> i : op.getInputs()) {
                computeSchemaRecIfNull((AbstractLogicalOperator) i.getValue());
            }
            if (op.hasNestedPlans()) {
                AbstractOperatorWithNestedPlans a = (AbstractOperatorWithNestedPlans) op;
                for (ILogicalPlan p : a.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> r : p.getRoots()) {
                        computeSchemaRecIfNull((AbstractLogicalOperator) r.getValue());
                    }
                }
            }
            op.recomputeSchema();
        }
    }

    public static boolean isMissingTest(AbstractLogicalOperator op) {
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        AbstractLogicalOperator doubleUnder = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        if (doubleUnder.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }
        ILogicalExpression eu = ((SelectOperator) op).getCondition().getValue();
        if (eu.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression f1 = (AbstractFunctionCallExpression) eu;
        if (!f1.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
            return false;
        }
        ILogicalExpression a1 = f1.getArguments().get(0).getValue();
        if (!a1.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            return false;
        }
        AbstractFunctionCallExpression f2 = (AbstractFunctionCallExpression) a1;
        if (!f2.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.IS_MISSING)) {
            return false;
        }
        return true;
    }

    public static void typePlan(ILogicalPlan p, IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> r : p.getRoots()) {
            typeOpRec(r, context);
        }
    }

    /**
     * Recursively visits all descendants of the given operator and
     * (re)computes and sets a type environment for each operator.
     *
     * @param r
     *            a mutable logical operator
     * @param context
     *            optimization context
     * @throws AlgebricksException
     */
    public static void typeOpRec(Mutable<ILogicalOperator> r, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) r.getValue();
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            typeOpRec(i, context);
        }
        if (op.hasNestedPlans()) {
            for (ILogicalPlan p : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                typePlan(p, context);
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    public static boolean isAlwaysTrueCond(ILogicalExpression cond) {
        if (cond.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return ((ConstantExpression) cond).getValue().isTrue();
        }
        return false;
    }

    public static boolean isCardinalityExactOne(ILogicalOperator operator) throws AlgebricksException {
        CardinalityInferenceVisitor visitor = new CardinalityInferenceVisitor();
        return operator.accept(visitor, null) == 1L;
    }

    public static boolean isCardinalityZeroOrOne(ILogicalOperator operator) throws AlgebricksException {
        CardinalityInferenceVisitor visitor = new CardinalityInferenceVisitor();
        return operator.accept(visitor, null) <= 1;
    }

    /**
     * Whether an operator can be moved around in the query plan.
     *
     * @param op
     *            the operator to consider.
     * @return true if the operator can be moved, false if the operator cannot be moved.
     */
    public static boolean isMovable(ILogicalOperator op) {
        Object annotation = op.getAnnotations().get(MOVABLE);
        if (annotation != null) {
            return (Boolean) annotation;
        }
        switch (op.getOperatorTag()) {
            case ASSIGN:
                // Can't move nonPures!
                AssignOperator assign = (AssignOperator) op;
                for (Mutable<ILogicalExpression> expr : assign.getExpressions()) {
                    if (!expr.getValue().isFunctional()) {
                        return false;
                    }
                }
                break;
            case RUNNINGAGGREGATE:
            case WINDOW:
                return false;
        }
        return true;
    }

    /**
     * Mark an operator to be either movable or not.
     *
     * @param op
     *            the operator to consider.
     * @param movable
     *            true means it is movable, false means it is not movable.
     */
    public static void markMovable(ILogicalOperator op, boolean movable) {
        op.getAnnotations().put(MOVABLE, movable);
    }

    /**
     * Checks whether a binary input operator in consideration needs to
     * run in a single partition mode. If it does, returns an empty properties vector.
     * Otherwise, returns the proposed partitioned properties vector.
     *
     * @param op,                          the binary input operator in consideration.
     * @param partitionedPropertiesVector, the proposed partitioned properties vector.
     * @return either an empty properties vector or the proposed partitioned properties vector.
     */
    public static StructuralPropertiesVector checkUnpartitionedAndGetPropertiesVector(ILogicalOperator op,
            StructuralPropertiesVector partitionedPropertiesVector) {
        ILogicalOperator leftChild = op.getInputs().get(0).getValue();
        ILogicalOperator rightChild = op.getInputs().get(1).getValue();
        boolean unPartitioned = leftChild.getExecutionMode().equals(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED)
                && rightChild.getExecutionMode().equals(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        return unPartitioned ? StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR : partitionedPropertiesVector;
    }
}
