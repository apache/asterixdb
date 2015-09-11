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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.OperatorDeepCopyVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;

public class OperatorManipulationUtil {

    // Transforms all NestedTupleSource operators to EmptyTupleSource operators
    public static void ntsToEts(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
            context.computeAndSetTypeEnvironmentForOperator(ets);
            opRef.setValue(ets);
        } else {
            for (Mutable<ILogicalOperator> i : opRef.getValue().getInputs()) {
                ntsToEts(i, context);
            }
        }
    }

    public static ILogicalOperator eliminateSingleSubplanOverEts(SubplanOperator subplan) {
        if (subplan.getNestedPlans().size() > 1) {
            // not a single subplan
            List<Mutable<ILogicalOperator>> subInpList = subplan.getInputs();
            subInpList.clear();
            subInpList.add(new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator()));
            return subplan;
        }
        ILogicalPlan plan = subplan.getNestedPlans().get(0);
        if (plan.getRoots().size() > 1) {
            // not a single subplan
            List<Mutable<ILogicalOperator>> subInpList = subplan.getInputs();
            subInpList.clear();
            subInpList.add(new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator()));
            return subplan;
        }
        return plan.getRoots().get(0).getValue();
    }

    public static boolean setOperatorMode(AbstractLogicalOperator op) {
        boolean change = false;
        switch (op.getOperatorTag()) {
            case DATASOURCESCAN: {
                op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                AbstractLogicalOperator currentOp = op;
                while (currentOp.getInputs().size() == 1) {
                    AbstractLogicalOperator child = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
                    if (child.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                        break;
                    }
                    child.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                    currentOp = child;
                }
                change = true;
                break;
            }
            case NESTEDTUPLESOURCE: {
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
                AbstractLogicalOperator prevOp = (AbstractLogicalOperator) nts.getDataSourceReference().getValue()
                        .getInputs().get(0).getValue();
                if (prevOp.getExecutionMode() != AbstractLogicalOperator.ExecutionMode.UNPARTITIONED) {
                    nts.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
                    change = true;
                }
                break;
            }
            default: {
                boolean forceUnpartitioned = false;
                if (op.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                    LimitOperator opLim = (LimitOperator) op;
                    if (opLim.isTopmostLimitOp()) {
                        opLim.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                        change = true;
                        forceUnpartitioned = true;
                    }
                }
                if (op.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    AggregateOperator aggOp = (AggregateOperator) op;
                    if (aggOp.isGlobal()) {
                        op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                        change = true;
                        forceUnpartitioned = true;
                    }
                }

                for (Mutable<ILogicalOperator> i : op.getInputs()) {
                    boolean exit = false;
                    AbstractLogicalOperator inputOp = (AbstractLogicalOperator) i.getValue();
                    switch (inputOp.getExecutionMode()) {
                        case PARTITIONED: {
                            if (forceUnpartitioned)
                                break;
                            op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                            change = true;
                            exit = true;
                            break;
                        }
                        case LOCAL: {
                            op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
                            change = true;
                            break;
                        }
                    }
                    if (exit) {
                        break;
                    }
                }
                break;
            }
        }
        return change;
    }

    public static void substituteVarRec(AbstractLogicalOperator op, LogicalVariable v1, LogicalVariable v2,
            boolean goThroughNts, ITypingContext ctx) throws AlgebricksException {
        VariableUtilities.substituteVariables(op, v1, v2, goThroughNts, ctx);
        for (Mutable<ILogicalOperator> opRef2 : op.getInputs()) {
            substituteVarRec((AbstractLogicalOperator) opRef2.getValue(), v1, v2, goThroughNts, ctx);
        }
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE && goThroughNts) {
            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
            if (nts.getDataSourceReference() != null) {
                AbstractLogicalOperator op2 = (AbstractLogicalOperator) nts.getDataSourceReference().getValue()
                        .getInputs().get(0).getValue();
                substituteVarRec(op2, v1, v2, goThroughNts, ctx);
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans aonp = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : aonp.getNestedPlans()) {
                for (Mutable<ILogicalOperator> ref : p.getRoots()) {
                    AbstractLogicalOperator aop = (AbstractLogicalOperator) ref.getValue();
                    substituteVarRec(aop, v1, v2, goThroughNts, ctx);
                }
            }
        }
    }

    public static ILogicalPlan deepCopy(ILogicalPlan plan) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> roots = plan.getRoots();
        List<Mutable<ILogicalOperator>> newRoots = clonePipeline(roots);
        return new ALogicalPlanImpl(newRoots);
    }

    public static ILogicalPlan deepCopy(ILogicalPlan plan, IOptimizationContext ctx) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> roots = plan.getRoots();
        List<Mutable<ILogicalOperator>> newRoots = clonePipeline(roots);
        cloneTypeEnvironments(ctx, roots, newRoots);
        return new ALogicalPlanImpl(newRoots);
    }

    private static List<Mutable<ILogicalOperator>> clonePipeline(List<Mutable<ILogicalOperator>> roots)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> newRoots = new ArrayList<Mutable<ILogicalOperator>>();
        for (Mutable<ILogicalOperator> opRef : roots) {
            newRoots.add(new MutableObject<ILogicalOperator>(bottomUpCopyOperators(opRef.getValue())));
        }
        return newRoots;
    }

    private static void cloneTypeEnvironments(IOptimizationContext ctx, List<Mutable<ILogicalOperator>> roots,
            List<Mutable<ILogicalOperator>> newRoots) {
        for (int i = 0; i < newRoots.size(); i++) {
            Mutable<ILogicalOperator> opRef = newRoots.get(i);
            Mutable<ILogicalOperator> oldOpRef = roots.get(i);
            while (opRef.getValue().getInputs().size() > 0) {
                ctx.setOutputTypeEnvironment(opRef.getValue(), ctx.getOutputTypeEnvironment(oldOpRef.getValue()));
                opRef = opRef.getValue().getInputs().get(0);
                oldOpRef = oldOpRef.getValue().getInputs().get(0);
            }
            ctx.setOutputTypeEnvironment(opRef.getValue(), ctx.getOutputTypeEnvironment(oldOpRef.getValue()));
        }
    }

    public static ILogicalOperator bottomUpCopyOperators(ILogicalOperator op) throws AlgebricksException {
        ILogicalOperator newOp = deepCopy(op);
        newOp.getInputs().clear();
        for (Mutable<ILogicalOperator> child : op.getInputs())
            newOp.getInputs().add(new MutableObject<ILogicalOperator>(bottomUpCopyOperators(child.getValue())));
        return newOp;
    }

    public static ILogicalOperator deepCopy(ILogicalOperator op) throws AlgebricksException {
        OperatorDeepCopyVisitor visitor = new OperatorDeepCopyVisitor();
        return op.accept(visitor, null);
    }

}
