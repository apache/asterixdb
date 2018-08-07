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
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes projections through its input operator, provided that operator does
 * not produce the projected variables.
 *
 * @author Nicola
 */
public class PushProjectDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }
        ProjectOperator pi = (ProjectOperator) op;
        Mutable<ILogicalOperator> opRef2 = pi.getInputs().get(0);

        Set<LogicalVariable> toPush = new LinkedHashSet<LogicalVariable>();
        toPush.addAll(pi.getVariables());

        Pair<Boolean, Boolean> p = pushThroughOp(toPush, opRef2, op, context);
        boolean smthWasPushed = p.first;
        if (p.second) { // the original projection is redundant
            opRef.setValue(op.getInputs().get(0).getValue());
            smthWasPushed = true;
        }

        return smthWasPushed;
    }

    private static Pair<Boolean, Boolean> pushThroughOp(Set<LogicalVariable> toPush, Mutable<ILogicalOperator> opRef2,
            ILogicalOperator initialOp, IOptimizationContext context) throws AlgebricksException {
        List<LogicalVariable> initProjectList = new ArrayList<LogicalVariable>(toPush);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        do {
            if (op2.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                    || op2.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE
                    || op2.getOperatorTag() == LogicalOperatorTag.PROJECT
                    || op2.getOperatorTag() == LogicalOperatorTag.REPLICATE
                    || op2.getOperatorTag() == LogicalOperatorTag.SPLIT
                    || op2.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                return new Pair<Boolean, Boolean>(false, false);
            }
            if (!op2.isMap()) {
                break;
            }
            LinkedList<LogicalVariable> usedVars = new LinkedList<LogicalVariable>();
            VariableUtilities.getUsedVariables(op2, usedVars);
            toPush.addAll(usedVars);
            LinkedList<LogicalVariable> producedVars = new LinkedList<LogicalVariable>();
            VariableUtilities.getProducedVariables(op2, producedVars);
            toPush.removeAll(producedVars);
            // we assume pipelineable ops. have only one input
            opRef2 = op2.getInputs().get(0);
            op2 = (AbstractLogicalOperator) opRef2.getValue();
        } while (true);

        LinkedList<LogicalVariable> produced2 = new LinkedList<LogicalVariable>();
        VariableUtilities.getProducedVariables(op2, produced2);
        LinkedList<LogicalVariable> used2 = new LinkedList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op2, used2);

        boolean canCommuteProjection = initProjectList.containsAll(toPush) && initProjectList.containsAll(produced2)
                && initProjectList.containsAll(used2);
        // if true, we can get rid of the initial projection

        // get rid of useless decor vars.
        if (!canCommuteProjection && op2.getOperatorTag() == LogicalOperatorTag.GROUP) {
            boolean gbyChanged = false;
            GroupByOperator gby = (GroupByOperator) op2;
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> newDecorList =
                    new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList()) {
                LogicalVariable decorVar = GroupByOperator.getDecorVariable(p);
                if (!toPush.contains(decorVar)) {
                    used2.remove(decorVar);
                    gbyChanged = true;
                } else {
                    newDecorList.add(p);
                }
            }
            gby.getDecorList().clear();
            gby.getDecorList().addAll(newDecorList);
            if (gbyChanged) {
                context.computeAndSetTypeEnvironmentForOperator(gby);
            }
        }
        used2.clear();
        VariableUtilities.getUsedVariables(op2, used2);

        toPush.addAll(used2); // remember that toPush is a Set
        toPush.removeAll(produced2);

        if (toPush.isEmpty()) {
            return new Pair<Boolean, Boolean>(false, false);
        }

        boolean smthWasPushed = false;
        for (Mutable<ILogicalOperator> c : op2.getInputs()) {
            if (pushNeededProjections(toPush, c, context, initialOp)) {
                smthWasPushed = true;
            }
        }
        if (op2.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans n = (AbstractOperatorWithNestedPlans) op2;
            for (ILogicalPlan p : n.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    if (pushNeededProjections(toPush, r, context, initialOp)) {
                        smthWasPushed = true;
                    }
                }
            }
        }
        return new Pair<Boolean, Boolean>(smthWasPushed, canCommuteProjection);
    }

    // It does not try to push above another Projection.
    private static boolean pushNeededProjections(Set<LogicalVariable> toPush, Mutable<ILogicalOperator> opRef,
            IOptimizationContext context, ILogicalOperator initialOp) throws AlgebricksException {
        Set<LogicalVariable> allP = new LinkedHashSet<LogicalVariable>();
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        VariableUtilities.getSubplanLocalLiveVariables(op, allP);

        Set<LogicalVariable> toProject = new LinkedHashSet<LogicalVariable>();
        for (LogicalVariable v : toPush) {
            if (allP.contains(v)) {
                toProject.add(v);
            }
        }
        if (toProject.equals(allP)) {
            // projection would be redundant, since we would project everything
            // but we can try with the children
            boolean push = false;
            if (pushThroughOp(toProject, opRef, initialOp, context).first) {
                push = true;
            }
            return push;
        } else {
            return pushAllProjectionsOnTopOf(toProject, opRef, context, initialOp);
        }
    }

    // It does not try to push above another Projection.
    private static boolean pushAllProjectionsOnTopOf(Collection<LogicalVariable> toPush,
            Mutable<ILogicalOperator> opRef, IOptimizationContext context, ILogicalOperator initialOp)
            throws AlgebricksException {
        if (toPush.isEmpty()) {
            return false;
        }
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (context.checkAndAddToAlreadyCompared(initialOp, op)) {
            return false;
        }

        if (op.getOperatorTag() == LogicalOperatorTag.PROJECT) {
            return false;
        }

        ProjectOperator pi2 = new ProjectOperator(new ArrayList<LogicalVariable>(toPush));
        pi2.setSourceLocation(op.getSourceLocation());
        pi2.getInputs().add(new MutableObject<ILogicalOperator>(op));
        opRef.setValue(pi2);
        pi2.setExecutionMode(op.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(pi2);
        return true;
    }

}
