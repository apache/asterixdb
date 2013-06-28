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
package edu.uci.ics.hyracks.algebricks.core.algebra.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class OperatorPropertiesUtil {

    public static <T> boolean disjoint(Collection<T> c1, Collection<T> c2) {
        for (T m : c1) {
            if (c2.contains(m)) {
                return false;
            }
        }
        return true;
    }

    // Obs: doesn't return expected result for op. with nested plans.
    private static void getFreeVariablesInOp(ILogicalOperator op, Set<LogicalVariable> freeVars)
            throws AlgebricksException {
        VariableUtilities.getUsedVariables(op, freeVars);
        HashSet<LogicalVariable> produced = new HashSet<LogicalVariable>();
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
     * @param vars
     *            - The collection to which the free variables will be added.
     */
    public static void getFreeVariablesInSelfOrDesc(AbstractLogicalOperator op, Set<LogicalVariable> freeVars)
            throws AlgebricksException {
        HashSet<LogicalVariable> produced = new HashSet<LogicalVariable>();
        VariableUtilities.getProducedVariables(op, produced);
        for (LogicalVariable v : produced) {
            freeVars.remove(v);
        }

        HashSet<LogicalVariable> used = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, used);
        for (LogicalVariable v : used) {
            if (!freeVars.contains(v)) {
                freeVars.add(v);
            }
        }

        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans s = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : s.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) r.getValue(), freeVars);
                }
            }
            s.getUsedVariablesExceptNestedPlans(freeVars);
            HashSet<LogicalVariable> produced2 = new HashSet<LogicalVariable>();
            s.getProducedVariablesExceptNestedPlans(produced2);
            freeVars.removeAll(produced);
        }
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) i.getValue(), freeVars);
        }
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
        HashSet<LogicalVariable> free = new HashSet<LogicalVariable>();
        getFreeVariablesInSelfOrDesc(op, free);
        return !free.isEmpty();
    }

    public static boolean hasFreeVariables(ILogicalOperator op) throws AlgebricksException {
        HashSet<LogicalVariable> free = new HashSet<LogicalVariable>();
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

    public static boolean isNullTest(AbstractLogicalOperator op) {
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
        if (!f2.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.IS_NULL)) {
            return false;
        }
        return true;
    }

    public static void typePlan(ILogicalPlan p, IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> r : p.getRoots()) {
            typeOpRec(r, context);
        }
    }

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
}
