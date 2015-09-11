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
import java.util.LinkedList;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

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
        if (gbyOp.getExecutionMode() != ExecutionMode.PARTITIONED) {
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
            if (!newGbyLiveVars.contains(usedDecorVars.get(0))) {
                // Let the left-hand side of gbyOp's decoration expressions populated through the combiner group-by without
                // any intermediate assignment.
                newGbyOp.addDecorExpression(null, p.second.getValue());
            }
        }
        newGbyOp.setExecutionMode(ExecutionMode.LOCAL);
        Object v = gbyOp.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, v);

        Object v2 = gbyOp.getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_EXTERNAL_GROUP_BY, v2);

        List<LogicalVariable> propagatedVars = new LinkedList<LogicalVariable>();
        VariableUtilities.getProducedVariables(newGbyOp, propagatedVars);

        Set<LogicalVariable> freeVars = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(gbyOp, freeVars);

        for (LogicalVariable var : freeVars) {
            if (!propagatedVars.contains(var)) {
                LogicalVariable newDecorVar = context.newVar();
                newGbyOp.addDecorExpression(newDecorVar, new VariableReferenceExpression(var));
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
        // Hook up input to new group-by.
        Mutable<ILogicalOperator> opRef3 = gbyOp.getInputs().get(0);
        ILogicalOperator op3 = opRef3.getValue();
        GroupByOperator newGbyOp = new GroupByOperator();
        newGbyOp.getInputs().add(new MutableObject<ILogicalOperator>(op3));
        // Copy annotations.
        Map<String, Object> annotations = newGbyOp.getAnnotations();
        annotations.putAll(gbyOp.getAnnotations());

        List<LogicalVariable> gbyVars = gbyOp.getGbyVarList();
        for (ILogicalPlan p : gbyOp.getNestedPlans()) {
            Pair<Boolean, ILogicalPlan> bip = tryToPushSubplan(p, gbyOp, newGbyOp, bi, gbyVars, context);
            if (!bip.first) {
                // For now, if we cannot push everything, give up.
                return null;
            }
            ILogicalPlan pushedSubplan = bip.second;
            if (pushedSubplan != null) {
                newGbyOp.getNestedPlans().add(pushedSubplan);
            }
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
            newGbyOp.addGbyExpression(replGbyList.get(i), new VariableReferenceExpression(newOpGbyList.get(i)));
            VariableUtilities.substituteVariables(gbyOp, newOpGbyList.get(i), replGbyList.get(i), false, context);
        }
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
            List<LogicalVariable> gbyVars2 = nestedGby.getGbyVarList();
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
            Mutable<ILogicalOperator> nestedGbyInputRef = nestedGby.getInputs().get(0);
            Mutable<ILogicalOperator> startOfPipelineRef = nestedGbyInputRef;
            if (startOfPipelineRef.getValue().getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                return true;
            }

            // move down the nested pipeline to find the start of the pipeline right upon the nested-tuple-source
            boolean hasIsNullFunction = OperatorPropertiesUtil.isNullTest((AbstractLogicalOperator) startOfPipelineRef
                    .getValue());
            while (startOfPipelineRef.getValue().getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                startOfPipelineRef = startOfPipelineRef.getValue().getInputs().get(0);
                hasIsNullFunction = OperatorPropertiesUtil.isNullTest((AbstractLogicalOperator) startOfPipelineRef
                        .getValue());
            }
            //keep the old nested-tuple-source
            Mutable<ILogicalOperator> oldNts = startOfPipelineRef.getValue().getInputs().get(0);

            //move down the nested op in the new gby operator
            Mutable<ILogicalOperator> newGbyNestedOpRef = toPushAccumulate.get(0);
            while (newGbyNestedOpRef.getValue().getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                newGbyNestedOpRef = newGbyNestedOpRef.getValue().getInputs().get(0);
            }

            //insert the pipeline before nested gby into the new (combiner) gby's nested plan on top of the nested-tuple-source
            startOfPipelineRef.getValue().getInputs().set(0, newGbyNestedOpRef.getValue().getInputs().get(0));
            newGbyNestedOpRef.getValue().getInputs().set(0, nestedGbyInputRef);

            //in the old gby operator, remove the nested pipeline since it is already pushed to the combiner gby
            nestedGby.getInputs().set(0, oldNts);
            List<LogicalVariable> aggProducedVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getProducedVariables(toPushAccumulate.get(0).getValue(), aggProducedVars);

            if (hasIsNullFunction && aggProducedVars.size() != 0) {
                // if the old nested pipeline contains a not-null-check, we need to convert it to a not-system-null-check in the non-local gby
                processNullTest(context, nestedGby, aggProducedVars);
            }

            return true;
        }
    }

    /**
     * Deal with the case where the nested plan in the combiner gby operator has a null-test before invoking aggregation functions.
     * 
     * @param context
     *            The optimization context.
     * @param nestedGby
     *            The nested gby operator in the global gby operator's subplan.
     * @param firstAggVar
     *            The first aggregation variable produced by the combiner gby.
     */
    protected abstract void processNullTest(IOptimizationContext context, GroupByOperator nestedGby,
            List<LogicalVariable> aggregateVarsProducedByCombiner);
}