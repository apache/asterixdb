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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;

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
                // ILogicalExpression e = ((UnnestOperator) op).getExpression();
                // if (AnalysisUtil.isDataSetCall(e)) {
                op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                AbstractLogicalOperator child = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
                if (child.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    child.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                }
                change = true;
                // }
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

}
