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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public class IntroduceGroupByCombinerRule extends AbstractIntroduceCombinerRule {

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

        replaceOriginalAggFuncs(bi.toReplaceMap);

        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyOp.getDecorList()) {
            LogicalVariable newDecorVar = context.newVar();
            newGbyOp.addDecorExpression(newDecorVar, p.second.getValue());
            p.second.setValue(new VariableReferenceExpression(newDecorVar));
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
        for (Mutable<ILogicalOperator> r : nestedPlan.getRoots()) {
            if (!tryToPushRoot(r, oldGbyOp, newGbyOp, bi, gbyVars, context, pushedRoots)) {
                // For now, if we cannot push everything, give up.
                return new Pair<Boolean, ILogicalPlan>(false, null);
            }
        }
        if (pushedRoots.isEmpty()) {
            return new Pair<Boolean, ILogicalPlan>(true, null);
        } else {
            return new Pair<Boolean, ILogicalPlan>(true, new ALogicalPlanImpl(pushedRoots));
        }
    }

    private boolean tryToPushRoot(Mutable<ILogicalOperator> root, GroupByOperator oldGbyOp, GroupByOperator newGbyOp,
            BookkeepingInfo bi, List<LogicalVariable> gbyVars, IOptimizationContext context,
            List<Mutable<ILogicalOperator>> toPushAccumulate) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) root.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            AggregateOperator initAgg = (AggregateOperator) op1;
            Pair<Boolean, Mutable<ILogicalOperator>> pOpRef = tryToPushAgg(initAgg, newGbyOp, bi.toReplaceMap, context);
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
            while (op2.getOperatorTag() != LogicalOperatorTag.GROUP && op2.getInputs().size() == 1) {
                op2 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
            }
            if (op2.getOperatorTag() != LogicalOperatorTag.GROUP) {
                return false;
            }
            GroupByOperator nestedGby = (GroupByOperator) op2;
            List<LogicalVariable> gbyVars2 = nestedGby.getGbyVarList();
            List<LogicalVariable> concatGbyVars = new ArrayList<LogicalVariable>(gbyVars);
            concatGbyVars.addAll(gbyVars2);
            for (ILogicalPlan p : nestedGby.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r2 : p.getRoots()) {
                    if (!tryToPushRoot(r2, nestedGby, newGbyOp, bi, concatGbyVars, context, toPushAccumulate)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }
}
