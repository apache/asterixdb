/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

/**
 * 
 * Pushes projections through its input operator, provided that operator does
 * not produce the projected variables.
 * 
 * @author Nicola
 * 
 */
public class PushProjectDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }
        ProjectOperator pi = (ProjectOperator) op;
        LogicalOperatorReference opRef2 = pi.getInputs().get(0);

        HashSet<LogicalVariable> toPush = new HashSet<LogicalVariable>();
        toPush.addAll(pi.getVariables());

        Pair<Boolean, Boolean> p = pushThroughOp(toPush, opRef2, op, context);
        boolean smthWasPushed = p.first;
        if (p.second) { // the original projection is redundant
            opRef.setOperator(op.getInputs().get(0).getOperator());
            smthWasPushed = true;
        }

        return smthWasPushed;
    }

    private static Pair<Boolean, Boolean> pushThroughOp(HashSet<LogicalVariable> toPush,
            LogicalOperatorReference opRef2, ILogicalOperator initialOp, IOptimizationContext context)
            throws AlgebricksException {
        List<LogicalVariable> initProjectList = new ArrayList<LogicalVariable>(toPush);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getOperator();
        do {
            if (op2.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                    || op2.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE
                    || op2.getOperatorTag() == LogicalOperatorTag.PROJECT
                    || op2.getOperatorTag() == LogicalOperatorTag.REPLICATE) {
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
            op2 = (AbstractLogicalOperator) opRef2.getOperator();
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
            List<Pair<LogicalVariable, LogicalExpressionReference>> newDecorList = new ArrayList<Pair<LogicalVariable, LogicalExpressionReference>>();
            for (Pair<LogicalVariable, LogicalExpressionReference> p : gby.getDecorList()) {
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
        for (LogicalOperatorReference c : op2.getInputs()) {
            if (pushNeededProjections(toPush, c, context, initialOp)) {
                smthWasPushed = true;
            }
        }
        if (op2.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans n = (AbstractOperatorWithNestedPlans) op2;
            for (ILogicalPlan p : n.getNestedPlans()) {
                for (LogicalOperatorReference r : p.getRoots()) {
                    if (pushNeededProjections(toPush, r, context, initialOp)) {
                        smthWasPushed = true;
                    }
                }
            }
        }
        return new Pair<Boolean, Boolean>(smthWasPushed, canCommuteProjection);
    }

    // It does not try to push above another Projection.
    private static boolean pushNeededProjections(HashSet<LogicalVariable> toPush, LogicalOperatorReference opRef,
            IOptimizationContext context, ILogicalOperator initialOp) throws AlgebricksException {
        HashSet<LogicalVariable> allP = new HashSet<LogicalVariable>();
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        VariableUtilities.getLiveVariables(op, allP);

        HashSet<LogicalVariable> toProject = new HashSet<LogicalVariable>();
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
            LogicalOperatorReference opRef, IOptimizationContext context, ILogicalOperator initialOp)
            throws AlgebricksException {
        if (toPush.isEmpty()) {
            return false;
        }
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();

        if (context.checkAndAddToAlreadyCompared(initialOp, op)) {
            return false;
        }

        switch (op.getOperatorTag()) {
            case EXCHANGE: {
                opRef = opRef.getOperator().getInputs().get(0);
                op = (AbstractLogicalOperator) opRef.getOperator();
                break;
            }
            case PROJECT: {
                return false;
            }
        }

        ProjectOperator pi2 = new ProjectOperator(new ArrayList<LogicalVariable>(toPush));
        pi2.getInputs().add(new LogicalOperatorReference(op));
        opRef.setOperator(pi2);
        pi2.setExecutionMode(op.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(pi2);
        return true;
    }

}
