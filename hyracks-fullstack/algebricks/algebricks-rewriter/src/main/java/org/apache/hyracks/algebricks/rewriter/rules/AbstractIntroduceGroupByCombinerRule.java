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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractIntroduceGroupByCombinerRule extends AbstractIntroduceCombinerRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gbyOp = (GroupByOperator) op;
        ExecutionMode executionMode = gbyOp.getExecutionMode();
        if (executionMode != ExecutionMode.PARTITIONED
                && !(executionMode == ExecutionMode.UNPARTITIONED && gbyOp.isGroupAll())) {
            return false;
        }

        BookkeepingInfo bi = new BookkeepingInfo();
        GroupByOperator newGbyOp = opToPush(gbyOp, bi, context);
        if (newGbyOp == null) {
            return false;
        }

        Set<LogicalVariable> newGbyLiveVars = new ListSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(newGbyOp, newGbyLiveVars);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyOp.getDecorList()) {
            List<LogicalVariable> usedDecorVars = new ArrayList<LogicalVariable>();
            // p.second.getValue() should always return a VariableReferenceExpression, hence
            // usedDecorVars should always contain only one variable.
            p.second.getValue().getUsedVariables(usedDecorVars);
            LogicalVariable usedVar = usedDecorVars.get(0);
            if (!newGbyLiveVars.contains(usedVar)) {
                // Let the left-hand side of gbyOp's decoration expressions populated through the combiner group-by without
                // any intermediate assignment.
                newGbyOp.addDecorExpression(null, p.second.getValue());
                newGbyLiveVars.add(usedVar);
            }
        }
        newGbyOp.setExecutionMode(ExecutionMode.LOCAL);
        Object v = gbyOp.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, v);

        Object v2 = gbyOp.getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_EXTERNAL_GROUP_BY, v2);

        Set<LogicalVariable> freeVars = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(gbyOp, freeVars);

        for (LogicalVariable var : freeVars) {
            if (!newGbyLiveVars.contains(var)) {
                LogicalVariable newDecorVar = context.newVar();
                VariableReferenceExpression varRef = new VariableReferenceExpression(var);
                varRef.setSourceLocation(gbyOp.getSourceLocation());
                newGbyOp.addDecorExpression(newDecorVar, varRef);
                VariableUtilities.substituteVariables(gbyOp.getNestedPlans().get(0).getRoots().get(0).getValue(), var,
                        newDecorVar, context);
            }
        }

        Mutable<ILogicalOperator> opRef3 = gbyOp.getInputs().get(0);
        opRef3.setValue(newGbyOp);
        typeGby(newGbyOp, context);
        typeGby(gbyOp, context);
        context.addToDontApplySet(this, op);
        return true;
    }

    private void typeGby(AbstractOperatorWithNestedPlans op, IOptimizationContext context) throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            OperatorPropertiesUtil.typePlan(p, context);
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    private GroupByOperator opToPush(GroupByOperator gbyOp, BookkeepingInfo bi, IOptimizationContext context)
            throws AlgebricksException {
        SourceLocation sourceLoc = gbyOp.getSourceLocation();
        // Hook up input to new group-by.
        Mutable<ILogicalOperator> opRef3 = gbyOp.getInputs().get(0);
        ILogicalOperator op3 = opRef3.getValue();
        GroupByOperator newGbyOp = new GroupByOperator();
        newGbyOp.setSourceLocation(sourceLoc);
        newGbyOp.getInputs().add(new MutableObject<ILogicalOperator>(op3));
        // Copy annotations.
        Map<String, Object> annotations = newGbyOp.getAnnotations();
        annotations.putAll(gbyOp.getAnnotations());

        List<LogicalVariable> gbyVars = gbyOp.getGroupByVarList();

        // Backup nested plans since tryToPushSubplan(...) may mutate them.
        List<ILogicalPlan> gbyNestedPlans = gbyOp.getNestedPlans();
        List<ILogicalPlan> backupNestedPlans = new ArrayList<>(gbyNestedPlans);

        for (int i = 0, n = gbyNestedPlans.size(); i < n; i++) {
            ILogicalPlan nestedPlan = gbyNestedPlans.get(i);

            // Replace nested plan with its copy
            ILogicalPlan p = OperatorManipulationUtil.deepCopy(nestedPlan, gbyOp);
            OperatorManipulationUtil.computeTypeEnvironment(p, context);
            gbyNestedPlans.set(i, p);

            // NOTE: tryToPushSubplan(...) can mutate the nested subplan p.
            Pair<Boolean, ILogicalPlan> bip = tryToPushSubplan(p, gbyOp, newGbyOp, bi, gbyVars, context);
            if (!bip.first) {
                // For now, if we cannot push everything, give up.
                // Resets the group-by operator with original nested plans.
                gbyNestedPlans.clear();
                gbyNestedPlans.addAll(backupNestedPlans);
                return null;
            }
            ILogicalPlan pushedSubplan = bip.second;
            if (pushedSubplan != null) {
                newGbyOp.getNestedPlans().add(pushedSubplan);
            }
        }

        // Nothing is pushed.
        if (bi.modifyGbyMap.isEmpty()) {
            return null;
        }

        ArrayList<LogicalVariable> newOpGbyList = new ArrayList<LogicalVariable>();
        ArrayList<LogicalVariable> replGbyList = new ArrayList<LogicalVariable>();
        // Find maximal sequence of variable.
        for (Map.Entry<GroupByOperator, List<LogicalVariable>> e : bi.modifyGbyMap.entrySet()) {
            List<LogicalVariable> varList = e.getValue();
            boolean see1 = true;
            int sz1 = newOpGbyList.size();
            int i = 0;
            for (LogicalVariable v : varList) {
                if (see1) {
                    if (i < sz1) {
                        LogicalVariable v2 = newOpGbyList.get(i);
                        if (v != v2) {
                            // cannot linearize
                            return null;
                        }
                    } else {
                        see1 = false;
                        newOpGbyList.add(v);
                        replGbyList.add(context.newVar());
                    }
                    i++;
                } else {
                    newOpGbyList.add(v);
                    replGbyList.add(context.newVar());
                }
            }
        }
        // set the vars in the new op
        int n = newOpGbyList.size();
        for (int i = 0; i < n; i++) {
            VariableReferenceExpression varRef = new VariableReferenceExpression(newOpGbyList.get(i));
            varRef.setSourceLocation(sourceLoc);
            newGbyOp.addGbyExpression(replGbyList.get(i), varRef);
            VariableUtilities.substituteVariables(gbyOp, newOpGbyList.get(i), replGbyList.get(i), false, context);
        }

        // Sets the global flag to be false.
        newGbyOp.setGlobal(false);
        // Sets the group all flag.
        newGbyOp.setGroupAll(gbyOp.isGroupAll());
        return newGbyOp;
    }

    private Pair<Boolean, ILogicalPlan> tryToPushSubplan(ILogicalPlan nestedPlan, GroupByOperator oldGbyOp,
            GroupByOperator newGbyOp, BookkeepingInfo bi, List<LogicalVariable> gbyVars, IOptimizationContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> pushedRoots = new ArrayList<Mutable<ILogicalOperator>>();
        Set<SimilarAggregatesInfo> toReplaceSet = new HashSet<SimilarAggregatesInfo>();
        for (Mutable<ILogicalOperator> r : nestedPlan.getRoots()) {
            if (!tryToPushRoot(r, oldGbyOp, newGbyOp, bi, gbyVars, context, pushedRoots, toReplaceSet)) {
                // For now, if we cannot push everything, give up.
                return new Pair<Boolean, ILogicalPlan>(false, null);
            }
        }
        if (pushedRoots.isEmpty()) {
            return new Pair<Boolean, ILogicalPlan>(true, null);
        } else {
            // Replaces the aggregation expressions in the original group-by op with new ones.
            ILogicalPlan newPlan = new ALogicalPlanImpl(pushedRoots);
            ILogicalPlan plan = fingIdenticalPlan(newGbyOp, newPlan);
            replaceOriginalAggFuncs(toReplaceSet);
            if (plan == null) {
                return new Pair<Boolean, ILogicalPlan>(true, newPlan);
            } else {
                // Does not add a nested subplan to newGbyOp if there already exists an isomorphic plan.
                Set<LogicalVariable> originalVars = new ListSet<LogicalVariable>();
                Set<LogicalVariable> newVars = new ListSet<LogicalVariable>();
                for (Mutable<ILogicalOperator> rootRef : pushedRoots) {
                    VariableUtilities.getProducedVariables(rootRef.getValue(), originalVars);
                }
                for (Mutable<ILogicalOperator> rootRef : plan.getRoots()) {
                    VariableUtilities.getProducedVariables(rootRef.getValue(), newVars);
                }

                // Replaces variable exprs referring to the variables produced by newPlan by
                // those produced by plan.
                Iterator<LogicalVariable> originalVarIter = originalVars.iterator();
                Iterator<LogicalVariable> newVarIter = newVars.iterator();
                while (originalVarIter.hasNext()) {
                    LogicalVariable originalVar = originalVarIter.next();
                    LogicalVariable newVar = newVarIter.next();
                    for (SimilarAggregatesInfo sai : toReplaceSet) {
                        for (AggregateExprInfo aei : sai.simAggs) {
                            ILogicalExpression afce = aei.aggExprRef.getValue();
                            afce.substituteVar(originalVar, newVar);
                        }
                    }
                }
                return new Pair<Boolean, ILogicalPlan>(true, null);
            }
        }
    }

    private ILogicalPlan fingIdenticalPlan(GroupByOperator newGbyOp, ILogicalPlan plan) throws AlgebricksException {
        for (ILogicalPlan nestedPlan : newGbyOp.getNestedPlans()) {
            if (IsomorphismUtilities.isOperatorIsomorphicPlan(plan, nestedPlan)) {
                return nestedPlan;
            }
        }
        return null;
    }

    private boolean tryToPushRoot(Mutable<ILogicalOperator> root, GroupByOperator oldGbyOp, GroupByOperator newGbyOp,
            BookkeepingInfo bi, List<LogicalVariable> gbyVars, IOptimizationContext context,
            List<Mutable<ILogicalOperator>> toPushAccumulate, Set<SimilarAggregatesInfo> toReplaceSet)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) root.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        // Finds nested group-by if any.
        AbstractLogicalOperator op3 = op2;
        while (op3.getOperatorTag() != LogicalOperatorTag.GROUP && op3.getInputs().size() == 1) {
            op3 = (AbstractLogicalOperator) op3.getInputs().get(0).getValue();
        }

        if (op3.getOperatorTag() != LogicalOperatorTag.GROUP) {
            AggregateOperator initAgg = (AggregateOperator) op1;
            Pair<Boolean, Mutable<ILogicalOperator>> pOpRef = tryToPushAgg(initAgg, newGbyOp, toReplaceSet, context);
            if (!pOpRef.first) {
                return false;
            }
            Mutable<ILogicalOperator> opRef = pOpRef.second;
            if (opRef != null) {
                toPushAccumulate.add(opRef);
            }
            bi.modifyGbyMap.put(oldGbyOp, gbyVars);
            return true;
        } else {
            GroupByOperator nestedGby = (GroupByOperator) op3;
            List<LogicalVariable> gbyVars2 = nestedGby.getGroupByVarList();
            Set<LogicalVariable> freeVars = new HashSet<>();
            // Removes non-free variables defined in the nested plan.
            OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(nestedGby, freeVars);
            gbyVars2.retainAll(freeVars);

            List<LogicalVariable> concatGbyVars = new ArrayList<LogicalVariable>(gbyVars);
            concatGbyVars.addAll(gbyVars2);
            for (ILogicalPlan p : nestedGby.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r2 : p.getRoots()) {
                    if (!tryToPushRoot(r2, nestedGby, newGbyOp, bi, concatGbyVars, context, toPushAccumulate,
                            toReplaceSet)) {
                        return false;
                    }
                }
            }

            /***
             * Push the nested pipeline which provides the input to the nested group operator into newGbyOp (the combined gby op).
             * The change is to fix asterixdb issue 782.
             */
            // Finds the reference of the bottom-most operator in the pipeline that
            // should not be pushed to the combiner group-by.
            Mutable<ILogicalOperator> currentOpRef = new MutableObject<ILogicalOperator>(nestedGby);
            Mutable<ILogicalOperator> bottomOpRef = findBottomOpRefStayInOldGby(nestedGby, currentOpRef);

            // Adds the used variables in the pipeline from <code>currentOpRef</code> to <code>bottomOpRef</code>
            // into the group-by keys for the introduced combiner group-by operator.
            Set<LogicalVariable> usedVars = collectUsedFreeVariables(currentOpRef, bottomOpRef);
            for (LogicalVariable usedVar : usedVars) {
                if (!concatGbyVars.contains(usedVar)) {
                    concatGbyVars.add(usedVar);
                }
            }

            // Retains the nested pipeline above the identified operator in the old group-by operator.
            // Pushes the nested pipeline under the select operator into the new group-by operator.
            Mutable<ILogicalOperator> oldNtsRef = findNtsRef(currentOpRef);
            ILogicalOperator opToCombiner = bottomOpRef.getValue().getInputs().get(0).getValue();
            if (opToCombiner.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                // No pipeline other than the aggregate operator needs to push to combiner.
                return true;
            }
            bottomOpRef.getValue().getInputs().set(0, new MutableObject<ILogicalOperator>(oldNtsRef.getValue()));
            Mutable<ILogicalOperator> newGbyNestedOpRef = findNtsRef(toPushAccumulate.get(0));
            NestedTupleSourceOperator newNts = (NestedTupleSourceOperator) newGbyNestedOpRef.getValue();
            newGbyNestedOpRef.setValue(opToCombiner);
            oldNtsRef.setValue(newNts);
            return true;
        }
    }

    /**
     * Find the set of used free variables along the pipeline from <code>topOpRef</code> (exclusive)
     * to <code>bottomOpRef</code> (inclusive).
     *
     * @param topOpRef,
     *            the top root of the pipeline.
     * @param bottomOpRef,
     *            the bottom of the pipeline.
     * @return the set of used variables.
     * @throws AlgebricksException
     */
    private Set<LogicalVariable> collectUsedFreeVariables(Mutable<ILogicalOperator> topOpRef,
            Mutable<ILogicalOperator> bottomOpRef) throws AlgebricksException {
        Set<LogicalVariable> usedVars = new HashSet<>();
        Mutable<ILogicalOperator> currentOpRef = topOpRef;
        while (currentOpRef != bottomOpRef) {
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
            VariableUtilities.getUsedVariables(currentOpRef.getValue(), usedVars);
        }
        Set<LogicalVariable> freeVars = new HashSet<>();
        OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) topOpRef.getValue(), freeVars);
        usedVars.retainAll(freeVars);
        return usedVars;
    }

    /**
     * Find the reference of a nested tuple source operator in the query pipeline rooted at <code>currentOpRef</code>
     *
     * @param currentOpRef
     * @return the reference of a nested tuple source operator
     */
    private Mutable<ILogicalOperator> findNtsRef(Mutable<ILogicalOperator> currentOpRef) {
        while (currentOpRef.getValue().getInputs().size() > 0) {
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
        }
        return currentOpRef;
    }

    /**
     * Find the bottom-most nested operator reference in the query pipeline rooted at <code>currentOpRef</code>
     * that cannot be pushed into the combiner group-by operator.
     *
     * @param nestedGby
     *            the nested group-by operator.
     * @param currentOpRef,the
     *            reference of the current op.
     * @return the bottom-most reference of a select operator
     */
    private Mutable<ILogicalOperator> findBottomOpRefStayInOldGby(GroupByOperator nestedGby,
            Mutable<ILogicalOperator> currentOpRef) throws AlgebricksException {
        Set<LogicalVariable> usedVarsInNestedGby = new HashSet<>();
        // Collects used variables in nested pipelines.
        for (ILogicalPlan nestedPlan : nestedGby.getNestedPlans()) {
            for (Mutable<ILogicalOperator> rootOpRef : nestedPlan.getRoots()) {
                VariableUtilities.getUsedVariablesInDescendantsAndSelf(rootOpRef.getValue(), usedVarsInNestedGby);
            }
        }
        Mutable<ILogicalOperator> bottomOpRef = currentOpRef;
        while (currentOpRef.getValue().getInputs().size() > 0) {
            // Used for checking the dependency between nestedGby and the current operator
            Set<LogicalVariable> dependingVars = new HashSet<>();
            VariableUtilities.getProducedVariables(currentOpRef.getValue(), dependingVars);
            dependingVars.removeAll(usedVarsInNestedGby);
            if (currentOpRef.getValue().getOperatorTag() == LogicalOperatorTag.SELECT || !dependingVars.isEmpty()) {
                bottomOpRef = currentOpRef;
            }
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
        }
        return bottomOpRef;
    }

    /**
     * Deal with the case where the nested plan in the combiner gby operator has a null-test before invoking aggregation functions.
     *
     * @param context
     *            The optimization context.
     * @param nestedGby
     *            The nested gby operator in the global gby operator's subplan.
     * @param aggregateVarsProducedByCombiner
     *            The aggregation variables produced by the combiner gby.
     */
    protected abstract void processNullTest(IOptimizationContext context, GroupByOperator nestedGby,
            List<LogicalVariable> aggregateVarsProducedByCombiner);
}
