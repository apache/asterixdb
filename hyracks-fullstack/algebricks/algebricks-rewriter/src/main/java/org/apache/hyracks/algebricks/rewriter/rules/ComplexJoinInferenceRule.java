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

import java.util.HashSet;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ComplexJoinInferenceRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (!(op instanceof AbstractScanOperator)) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op2;

        Mutable<ILogicalOperator> opRef3 = subplan.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();

        if (op3.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || op3.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return false;
        }

        if (subplanHasFreeVariables(subplan)) {
            return false;
        }

        HashSet<LogicalVariable> varsUsedInUnnest = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, varsUsedInUnnest);

        HashSet<LogicalVariable> producedInSubplan = new HashSet<LogicalVariable>();
        VariableUtilities.getProducedVariables(subplan, producedInSubplan);

        if (!producedInSubplan.containsAll(varsUsedInUnnest)) {
            return false;
        }

        ntsToEtsInSubplan(subplan, context);
        cleanupJoins(subplan);
        InnerJoinOperator join = new InnerJoinOperator(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        join.setSourceLocation(op.getSourceLocation());
        join.getInputs().add(opRef3);
        opRef2.setValue(OperatorManipulationUtil.eliminateSingleSubplanOverEts(subplan));
        join.getInputs().add(new MutableObject<ILogicalOperator>(op));
        opRef.setValue(join);
        context.computeAndSetTypeEnvironmentForOperator(join);
        return true;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    private static void cleanupJoins(SubplanOperator s) {
        for (ILogicalPlan p : s.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                cleanupJoins(r);
            }
        }
    }

    /** clean up joins that have one input branch that is empty tuple source */
    private static void cleanupJoins(Mutable<ILogicalOperator> opRef) {
        if (opRef.getValue() instanceof AbstractBinaryJoinOperator) {
            for (Mutable<ILogicalOperator> inputRef : opRef.getValue().getInputs()) {
                if (inputRef.getValue().getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    opRef.getValue().getInputs().remove(inputRef);
                    opRef.setValue(opRef.getValue().getInputs().get(0).getValue());
                    break;
                }
            }
        }
        for (Mutable<ILogicalOperator> inputRef : opRef.getValue().getInputs()) {
            cleanupJoins(inputRef);
        }
    }

    private static void ntsToEtsInSubplan(SubplanOperator s, IOptimizationContext context) throws AlgebricksException {
        for (ILogicalPlan p : s.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                OperatorManipulationUtil.ntsToEts(r, context);
            }
        }
    }

    private static boolean subplanHasFreeVariables(SubplanOperator s) throws AlgebricksException {
        for (ILogicalPlan p : s.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                if (OperatorPropertiesUtil.hasFreeVariablesInSelfOrDesc((AbstractLogicalOperator) r.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

}
