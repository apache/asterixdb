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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushSelectIntoJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        Collection<LogicalVariable> joinLiveVarsLeft = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> joinLiveVarsRight = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> liveInOpsToPushLeft = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> liveInOpsToPushRight = new HashSet<LogicalVariable>();

        List<ILogicalOperator> pushedOnLeft = new ArrayList<ILogicalOperator>();
        List<ILogicalOperator> pushedOnRight = new ArrayList<ILogicalOperator>();
        LinkedList<ILogicalOperator> notPushedStack = new LinkedList<ILogicalOperator>();
        Collection<LogicalVariable> usedVars = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> producedVars = new HashSet<LogicalVariable>();

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;
        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator son = (AbstractLogicalOperator) opRef2.getValue();
        AbstractLogicalOperator op2 = son;
        boolean needToPushOps = false;
        while (son.isMap()) {
            needToPushOps = true;
            Mutable<ILogicalOperator> opRefLink = son.getInputs().get(0);
            son = (AbstractLogicalOperator) opRefLink.getValue();
        }

        if (son.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && son.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        boolean isLoj = son.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN;
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) son;

        Mutable<ILogicalOperator> joinBranchLeftRef = join.getInputs().get(0);
        Mutable<ILogicalOperator> joinBranchRightRef = join.getInputs().get(1);

        if (needToPushOps) {
            ILogicalOperator joinBranchLeft = joinBranchLeftRef.getValue();
            ILogicalOperator joinBranchRight = joinBranchRightRef.getValue();
            VariableUtilities.getLiveVariables(joinBranchLeft, joinLiveVarsLeft);
            VariableUtilities.getLiveVariables(joinBranchRight, joinLiveVarsRight);
            Mutable<ILogicalOperator> opIterRef = opRef2;
            ILogicalOperator opIter = op2;
            while (opIter != join) {
                LogicalOperatorTag tag = ((AbstractLogicalOperator) opIter).getOperatorTag();
                if (tag == LogicalOperatorTag.PROJECT) {
                    notPushedStack.addFirst(opIter);
                } else {
                    VariableUtilities.getUsedVariables(opIter, usedVars);
                    VariableUtilities.getProducedVariables(opIter, producedVars);
                    if (joinLiveVarsLeft.containsAll(usedVars)) {
                        pushedOnLeft.add(opIter);
                        liveInOpsToPushLeft.addAll(producedVars);
                    } else if (joinLiveVarsRight.containsAll(usedVars)) {
                        pushedOnRight.add(opIter);
                        liveInOpsToPushRight.addAll(producedVars);
                    } else {
                        return false;
                    }
                }
                opIterRef = opIter.getInputs().get(0);
                opIter = opIterRef.getValue();
            }
            if (isLoj && pushedOnLeft.isEmpty()) {
                return false;
            }
        }

        boolean intersectsAllBranches = true;
        boolean[] intersectsBranch = new boolean[join.getInputs().size()];
        LinkedList<LogicalVariable> selectVars = new LinkedList<LogicalVariable>();
        select.getCondition().getValue().getUsedVariables(selectVars);
        int i = 0;
        for (Mutable<ILogicalOperator> branch : join.getInputs()) {
            LinkedList<LogicalVariable> branchVars = new LinkedList<LogicalVariable>();
            VariableUtilities.getLiveVariables(branch.getValue(), branchVars);
            if (i == 0) {
                branchVars.addAll(liveInOpsToPushLeft);
            } else {
                branchVars.addAll(liveInOpsToPushRight);
            }
            if (OperatorPropertiesUtil.disjoint(selectVars, branchVars)) {
                intersectsAllBranches = false;
            } else {
                intersectsBranch[i] = true;
            }
            i++;
        }
        if (!intersectsBranch[0] && !intersectsBranch[1]) {
            return false;
        }
        if (needToPushOps) {
            pushOps(pushedOnLeft, joinBranchLeftRef, context);
            pushOps(pushedOnRight, joinBranchRightRef, context);
        }
        if (intersectsAllBranches) {
            addCondToJoin(select, join, context);
        } else { // push down
            Iterator<Mutable<ILogicalOperator>> branchIter = join.getInputs().iterator();

            for (int j = 0; j < intersectsBranch.length; j++) {
                Mutable<ILogicalOperator> branch = branchIter.next();
                boolean inter = intersectsBranch[j];
                if (inter) {
                    copySelectToBranch(select, branch, context);
                }

                // if a left outer join, we can only push conditions into the
                // outer branch.
                if (j == 0 && isLoj) {
                    // stop at this branch
                    break;
                }
            }
        }
        ILogicalOperator top = join;
        for (ILogicalOperator npOp : notPushedStack) {
            List<Mutable<ILogicalOperator>> npInpList = npOp.getInputs();
            npInpList.clear();
            npInpList.add(new MutableObject<ILogicalOperator>(top));
            context.computeAndSetTypeEnvironmentForOperator(npOp);
            top = npOp;
        }
        opRef.setValue(top);
        return true;

    }

    private void pushOps(List<ILogicalOperator> opList, Mutable<ILogicalOperator> joinBranch,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator topOp = joinBranch.getValue();
        ListIterator<ILogicalOperator> iter = opList.listIterator(opList.size());
        while (iter.hasPrevious()) {
            ILogicalOperator op = iter.previous();
            List<Mutable<ILogicalOperator>> opInpList = op.getInputs();
            opInpList.clear();
            opInpList.add(new MutableObject<ILogicalOperator>(topOp));
            topOp = op;
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        joinBranch.setValue(topOp);
    }

    private static void addCondToJoin(SelectOperator select, AbstractBinaryJoinOperator join,
            IOptimizationContext context) {
        ILogicalExpression cond = join.getCondition().getValue();
        if (OperatorPropertiesUtil.isAlwaysTrueCond(cond)) { // the join was a product
            join.getCondition().setValue(select.getCondition().getValue());
        } else {
            boolean bAddedToConj = false;
            if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression fcond = (AbstractFunctionCallExpression) cond;
                if (fcond.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
                    AbstractFunctionCallExpression newCond = new ScalarFunctionCallExpression(context
                            .getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND));
                    newCond.getArguments().add(select.getCondition());
                    newCond.getArguments().addAll(fcond.getArguments());
                    join.getCondition().setValue(newCond);
                    bAddedToConj = true;
                }
            }
            if (!bAddedToConj) {
                AbstractFunctionCallExpression newCond = new ScalarFunctionCallExpression(context.getMetadataProvider()
                        .lookupFunction(AlgebricksBuiltinFunctions.AND), select.getCondition(),
                        new MutableObject<ILogicalExpression>(join.getCondition().getValue()));
                join.getCondition().setValue(newCond);
            }
        }
    }

    private static void copySelectToBranch(SelectOperator select, Mutable<ILogicalOperator> branch,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator newSelect = new SelectOperator(select.getCondition());
        Mutable<ILogicalOperator> newRef = new MutableObject<ILogicalOperator>(branch.getValue());
        newSelect.getInputs().add(newRef);
        branch.setValue(newSelect);
        context.computeAndSetTypeEnvironmentForOperator(newSelect);
    }
}