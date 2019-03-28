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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
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
        AbstractLogicalOperator.ExecutionMode oldMode = op.getExecutionMode();
        switch (op.getOperatorTag()) {
            case DATASOURCESCAN: {
                op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                AbstractLogicalOperator currentOp = op;
                while (currentOp.getInputs().size() == 1) {
                    AbstractLogicalOperator child = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
                    // Empty tuple source is a special case that can be partitioned in the same way as the data scan.
                    if (child.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                        break;
                    }
                    child.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                    currentOp = child;
                }
                break;
            }
            case NESTEDTUPLESOURCE: {
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
                AbstractLogicalOperator prevOp =
                        (AbstractLogicalOperator) nts.getDataSourceReference().getValue().getInputs().get(0).getValue();
                if (prevOp.getExecutionMode() != AbstractLogicalOperator.ExecutionMode.UNPARTITIONED) {
                    nts.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
                }
                break;
            }
            default: {
                boolean forceUnpartitioned = false;
                if (op.getOperatorTag() == LogicalOperatorTag.LIMIT) {
                    LimitOperator opLim = (LimitOperator) op;
                    if (opLim.isTopmostLimitOp()) {
                        opLim.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                        forceUnpartitioned = true;
                    }
                }
                if (op.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    AggregateOperator aggOp = (AggregateOperator) op;
                    if (aggOp.isGlobal()) {
                        op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                        forceUnpartitioned = true;
                    }
                }
                if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                    GroupByOperator gbyOp = (GroupByOperator) op;
                    if (gbyOp.isGroupAll() && gbyOp.isGlobal()) {
                        op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                        forceUnpartitioned = true;
                    }
                }
                if (op.getOperatorTag() == LogicalOperatorTag.WINDOW) {
                    WindowOperator winOp = (WindowOperator) op;
                    if (winOp.getPartitionExpressions().isEmpty()) {
                        op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                        forceUnpartitioned = true;
                    }
                }

                for (Mutable<ILogicalOperator> i : op.getInputs()) {
                    boolean exit = false;
                    AbstractLogicalOperator inputOp = (AbstractLogicalOperator) i.getValue();
                    switch (inputOp.getExecutionMode()) {
                        case PARTITIONED: {
                            if (forceUnpartitioned) {
                                break;
                            }
                            op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
                            exit = true;
                            break;
                        }
                        case LOCAL: {
                            op.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
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
        return oldMode != op.getExecutionMode();
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
                AbstractLogicalOperator op2 =
                        (AbstractLogicalOperator) nts.getDataSourceReference().getValue().getInputs().get(0).getValue();
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

    public static ILogicalPlan deepCopy(ILogicalPlan plan, ILogicalOperator dataSource) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> roots = plan.getRoots();
        List<Mutable<ILogicalOperator>> newRoots = clonePipeline(roots);
        ILogicalPlan newPlan = new ALogicalPlanImpl(newRoots);
        setDataSource(newPlan, dataSource);
        return newPlan;
    }

    public static ILogicalPlan deepCopy(ILogicalPlan plan, IOptimizationContext ctx) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> roots = plan.getRoots();
        List<Mutable<ILogicalOperator>> newRoots = clonePipeline(roots);
        cloneTypeEnvironments(ctx, roots, newRoots);
        return new ALogicalPlanImpl(newRoots);
    }

    public static Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> deepCopyWithNewVars(
            ILogicalOperator root, IOptimizationContext ctx) throws AlgebricksException {
        LogicalOperatorDeepCopyWithNewVariablesVisitor deepCopyVisitor =
                new LogicalOperatorDeepCopyWithNewVariablesVisitor(ctx, ctx, true);
        ILogicalOperator newRoot = deepCopyVisitor.deepCopy(root);
        return new Pair<>(newRoot, deepCopyVisitor.getInputToOutputVariableMapping());
    }

    private static void setDataSource(ILogicalPlan plan, ILogicalOperator dataSource) {
        for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
            setDataSource(rootRef, dataSource);
        }
    }

    private static void setDataSource(Mutable<ILogicalOperator> opRef, ILogicalOperator dataSource) {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
            nts.setDataSourceReference(new MutableObject<ILogicalOperator>(dataSource));
        }
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            setDataSource(childRef, dataSource);
        }
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
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            newOp.getInputs().add(new MutableObject<ILogicalOperator>(bottomUpCopyOperators(child.getValue())));
        }
        return newOp;
    }

    public static ILogicalOperator deepCopy(ILogicalOperator op) throws AlgebricksException {
        OperatorDeepCopyVisitor visitor = new OperatorDeepCopyVisitor();
        AbstractLogicalOperator copiedOperator = (AbstractLogicalOperator) op.accept(visitor, null);
        copiedOperator.setSourceLocation(op.getSourceLocation());
        copiedOperator.setExecutionMode(op.getExecutionMode());
        copiedOperator.getAnnotations().putAll(op.getAnnotations());
        copiedOperator.setSchema(op.getSchema());
        AbstractLogicalOperator sourceOp = (AbstractLogicalOperator) op;
        copiedOperator.setPhysicalOperator(sourceOp.getPhysicalOperator());
        return copiedOperator;
    }

    /**
     * Compute type environment of a newly generated operator {@code op} and its input.
     *
     * @param op,
     *            the logical operator.
     * @param context,the
     *            optimization context.
     * @throws AlgebricksException
     */
    public static void computeTypeEnvironmentBottomUp(ILogicalOperator op, ITypingContext context)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> children : op.getInputs()) {
            computeTypeEnvironmentBottomUp(children.getValue(), context);
        }
        AbstractLogicalOperator abstractOp = (AbstractLogicalOperator) op;
        if (abstractOp.hasNestedPlans()) {
            for (ILogicalPlan p : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootRef : p.getRoots()) {
                    computeTypeEnvironmentBottomUp(rootRef.getValue(), context);
                }
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    /**
     * Computes the type environment for a logical query plan.
     *
     * @param plan,
     *            the logical plan to consider.
     * @param context
     *            the typing context.
     * @throws AlgebricksException
     */
    public static void computeTypeEnvironment(ILogicalPlan plan, ITypingContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
            computeTypeEnvironmentBottomUp(rootRef.getValue(), context);
        }
    }

    /***
     * Is the operator <code>>op</code> an ancestor of any operators with tags in the set <code>tags</code>?
     *
     * @param op
     * @param tags
     * @return True if yes; false other wise.
     */
    public static boolean ancestorOfOperators(ILogicalOperator op, Set<LogicalOperatorTag> tags) {
        LogicalOperatorTag opTag = op.getOperatorTag();
        if (tags.contains(opTag)) {
            return true;
        }
        for (Mutable<ILogicalOperator> children : op.getInputs()) {
            if (ancestorOfOperators(children.getValue(), tags)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns all descendants of an operator that are leaf operators
     *
     * @param opRef given operator
     * @return list containing all leaf descendants
     */
    public static List<Mutable<ILogicalOperator>> findLeafDescendantsOrSelf(Mutable<ILogicalOperator> opRef) {
        List<Mutable<ILogicalOperator>> result = Collections.emptyList();

        Deque<Mutable<ILogicalOperator>> queue = new ArrayDeque<>();
        queue.add(opRef);
        Mutable<ILogicalOperator> currentOpRef;
        while ((currentOpRef = queue.pollLast()) != null) {
            List<Mutable<ILogicalOperator>> inputs = currentOpRef.getValue().getInputs();
            if (inputs.isEmpty()) {
                if (result.isEmpty()) {
                    result = new ArrayList<>();
                }
                result.add(currentOpRef);
            } else {
                queue.addAll(inputs);
            }
        }
        return result;
    }

    /**
     * Find operator in a given list of operator references
     *
     * @param list list to search in
     * @param op   operator to find
     * @return operator position in the given list or {@code -1} if not found
     */
    public static int indexOf(List<Mutable<ILogicalOperator>> list, ILogicalOperator op) {
        for (int i = 0, ln = list.size(); i < ln; i++) {
            if (list.get(i).getValue() == op) {
                return i;
            }
        }
        return -1;
    }

    public static List<Mutable<ILogicalExpression>> cloneExpressions(List<Mutable<ILogicalExpression>> exprList) {
        if (exprList == null) {
            return null;
        }
        List<Mutable<ILogicalExpression>> clonedExprList = new ArrayList<>(exprList.size());
        for (Mutable<ILogicalExpression> expr : exprList) {
            clonedExprList.add(new MutableObject<>(expr.getValue().cloneExpression()));
        }
        return clonedExprList;
    }

    public static List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> cloneOrderExpressions(
            List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExprList) {
        if (orderExprList == null) {
            return null;
        }
        List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> clonedExprList =
                new ArrayList<>(orderExprList.size());
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> orderExpr : orderExprList) {
            clonedExprList.add(
                    new Pair<>(orderExpr.first, new MutableObject<>(orderExpr.second.getValue().cloneExpression())));
        }
        return clonedExprList;
    }

    /**
     * Finds a variable assigned to a given expression and returns a new {@link VariableReferenceExpression}
     * referring to this variable.
     * @param assignVarList list of variables
     * @param assignExprList list of expressions assigned to those variables
     * @param searchExpr expression to search for
     * @return said value, {@code null} if a variable is not found
     */
    public static VariableReferenceExpression findAssignedVariable(List<LogicalVariable> assignVarList,
            List<Mutable<ILogicalExpression>> assignExprList, ILogicalExpression searchExpr) {
        for (int i = 0, n = assignExprList.size(); i < n; i++) {
            ILogicalExpression expr = assignExprList.get(i).getValue();
            if (expr.equals(searchExpr)) {
                VariableReferenceExpression result = new VariableReferenceExpression(assignVarList.get(i));
                result.setSourceLocation(expr.getSourceLocation());
                return result;
            }
        }
        return null;
    }

    /**
     * Retains variables and expressions in provided lists as specified by given bitset
     */
    public static void retainAssignVariablesAndExpressions(List<LogicalVariable> assignVarList,
            List<Mutable<ILogicalExpression>> assignExprList, BitSet retainIndexes) {
        int targetIdx = 0;
        for (int sourceIdx = retainIndexes.nextSetBit(0); sourceIdx >= 0; sourceIdx =
                retainIndexes.nextSetBit(sourceIdx + 1)) {
            if (targetIdx != sourceIdx) {
                assignVarList.set(targetIdx, assignVarList.get(sourceIdx));
                assignExprList.set(targetIdx, assignExprList.get(sourceIdx));
            }
            targetIdx++;
        }

        for (int i = assignVarList.size() - 1; i >= targetIdx; i--) {
            assignVarList.remove(i);
            assignExprList.remove(i);
        }
    }
}
