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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ExtractCommonOperatorsRule implements IAlgebraicRewriteRule {

    private HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> childrenToParents = new HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>>();
    private List<Mutable<ILogicalOperator>> roots = new ArrayList<Mutable<ILogicalOperator>>();
    private List<Mutable<ILogicalOperator>> joins = new ArrayList<Mutable<ILogicalOperator>>();
    private List<List<Mutable<ILogicalOperator>>> equivalenceClasses = new ArrayList<List<Mutable<ILogicalOperator>>>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return false;
        }
        if (!roots.contains(op))
            roots.add(new MutableObject<ILogicalOperator>(op));
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return false;
        }
        boolean rewritten = false;
        boolean changed = false;
        if (roots.size() > 0) {
            do {
                changed = false;
                // applying the rewriting until fixpoint
                topDownMaterialization(roots);
                removeNonJoinBuildBranchCandidates();
                genCandidates(context);
                removeTrivialShare();
                removeNonJoinBuildBranchCandidates();
                if (equivalenceClasses.size() > 0)
                    changed = rewrite(context);
                if (!rewritten)
                    rewritten = changed;
                equivalenceClasses.clear();
                childrenToParents.clear();
                joins.clear();
            } while (changed);
            roots.clear();
        }
        return rewritten;
    }

    private void removeTrivialShare() {
        for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
            for (int i = candidates.size() - 1; i >= 0; i--) {
                Mutable<ILogicalOperator> opRef = candidates.get(i);
                AbstractLogicalOperator aop = (AbstractLogicalOperator) opRef.getValue();
                if (aop.getOperatorTag() == LogicalOperatorTag.EXCHANGE)
                    aop = (AbstractLogicalOperator) aop.getInputs().get(0).getValue();
                if (aop.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE)
                    candidates.remove(i);
            }
        }
        for (int i = equivalenceClasses.size() - 1; i >= 0; i--)
            if (equivalenceClasses.get(i).size() < 2)
                equivalenceClasses.remove(i);
    }

    private void removeNonJoinBuildBranchCandidates() {
        for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
            for (int i = candidates.size() - 1; i >= 0; i--) {
                Mutable<ILogicalOperator> opRef = candidates.get(i);
                boolean reserve = false;
                for (Mutable<ILogicalOperator> join : joins)
                    if (isInJoinBuildBranch(join, opRef)) {
                        reserve = true;
                    }
                if (!reserve)
                    candidates.remove(i);
            }
        }
        for (int i = equivalenceClasses.size() - 1; i >= 0; i--)
            if (equivalenceClasses.get(i).size() < 2)
                equivalenceClasses.remove(i);
    }

    private boolean isInJoinBuildBranch(Mutable<ILogicalOperator> joinRef, Mutable<ILogicalOperator> opRef) {
        Mutable<ILogicalOperator> buildBranch = joinRef.getValue().getInputs().get(1);
        do {
            if (buildBranch.equals(opRef)) {
                return true;
            } else {
                AbstractLogicalOperator aop = (AbstractLogicalOperator) buildBranch.getValue();
                if (aop.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                        || aop.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN
                        || buildBranch.getValue().getInputs().size() == 0)
                    return false;
                else
                    buildBranch = buildBranch.getValue().getInputs().get(0);
            }
        } while (true);
    }

    private boolean rewrite(IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        for (List<Mutable<ILogicalOperator>> members : equivalenceClasses) {
            if (rewriteForOneEquivalentClass(members, context))
                changed = true;
        }
        return changed;
    }

    private boolean rewriteForOneEquivalentClass(List<Mutable<ILogicalOperator>> members, IOptimizationContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> group = new ArrayList<Mutable<ILogicalOperator>>();
        boolean rewritten = false;
        while (members.size() > 0) {
            group.clear();
            Mutable<ILogicalOperator> candidate = members.remove(members.size() - 1);
            group.add(candidate);
            for (int i = members.size() - 1; i >= 0; i--) {
                Mutable<ILogicalOperator> peer = members.get(i);
                if (IsomorphismUtilities.isOperatorIsomorphic(candidate.getValue(), peer.getValue())) {
                    group.add(peer);
                    members.remove(i);
                }
            }
            AbstractLogicalOperator rop = new ReplicateOperator(group.size());
            rop.setPhysicalOperator(new ReplicatePOperator());
            rop.setExecutionMode(ExecutionMode.PARTITIONED);
            Mutable<ILogicalOperator> ropRef = new MutableObject<ILogicalOperator>(rop);
            AbstractLogicalOperator aopCandidate = (AbstractLogicalOperator) candidate.getValue();

            if (aopCandidate.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                rop.getInputs().add(candidate);
            } else {
                AbstractLogicalOperator beforeExchange = new ExchangeOperator();
                beforeExchange.setPhysicalOperator(new OneToOneExchangePOperator());
                beforeExchange.getInputs().add(candidate);
                context.computeAndSetTypeEnvironmentForOperator(beforeExchange);
                rop.getInputs().add(new MutableObject<ILogicalOperator>(beforeExchange));
            }
            context.computeAndSetTypeEnvironmentForOperator(rop);

            List<Mutable<ILogicalOperator>> parents = childrenToParents.get(candidate);
            for (Mutable<ILogicalOperator> parentRef : parents) {
                AbstractLogicalOperator parent = (AbstractLogicalOperator) parentRef.getValue();
                int index = parent.getInputs().indexOf(candidate);
                if (parent.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                    parent.getInputs().set(index, ropRef);
                } else {
                    AbstractLogicalOperator exchange = new ExchangeOperator();
                    exchange.setPhysicalOperator(new OneToOneExchangePOperator());
                    exchange.getInputs().add(ropRef);
                    context.computeAndSetTypeEnvironmentForOperator(exchange);
                    // parent.getInputs().get(index).setValue(exchange);
                    parent.getInputs().set(index, new MutableObject<ILogicalOperator>(exchange));
                    context.computeAndSetTypeEnvironmentForOperator(parent);
                }
            }

            List<LogicalVariable> liveVarsNew = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(candidate.getValue(), liveVarsNew);
            ArrayList<Mutable<ILogicalExpression>> assignExprs = new ArrayList<Mutable<ILogicalExpression>>();
            for (LogicalVariable liveVar : liveVarsNew)
                assignExprs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(liveVar)));
            for (Mutable<ILogicalOperator> ref : group) {
                if (ref.equals(candidate))
                    continue;
                ArrayList<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
                Map<LogicalVariable, LogicalVariable> variableMappingBack = new HashMap<LogicalVariable, LogicalVariable>();
                IsomorphismUtilities.mapVariablesTopDown(ref.getValue(), candidate.getValue(), variableMappingBack);
                for (int i = 0; i < liveVarsNew.size(); i++) {
                    liveVars.add(variableMappingBack.get(liveVarsNew.get(i)));
                }

                AbstractLogicalOperator assignOperator = new AssignOperator(liveVars, assignExprs);
                assignOperator.setPhysicalOperator(new AssignPOperator());
                AbstractLogicalOperator projectOperator = new ProjectOperator(liveVars);
                projectOperator.setPhysicalOperator(new StreamProjectPOperator());
                AbstractLogicalOperator exchOp = new ExchangeOperator();
                exchOp.setPhysicalOperator(new OneToOneExchangePOperator());
                exchOp.getInputs().add(ropRef);

                assignOperator.getInputs().add(new MutableObject<ILogicalOperator>(exchOp));
                projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));
                // set the types
                context.computeAndSetTypeEnvironmentForOperator(exchOp);
                context.computeAndSetTypeEnvironmentForOperator(assignOperator);
                context.computeAndSetTypeEnvironmentForOperator(projectOperator);

                List<Mutable<ILogicalOperator>> parentOpList = childrenToParents.get(ref);
                for (Mutable<ILogicalOperator> parentOpRef : parentOpList) {
                    AbstractLogicalOperator parentOp = (AbstractLogicalOperator) parentOpRef.getValue();
                    int index = parentOp.getInputs().indexOf(ref);
                    if (parentOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                        AbstractLogicalOperator parentOpNext = (AbstractLogicalOperator) childrenToParents
                                .get(parentOpRef).get(0).getValue();
                        if (parentOpNext.isMap()) {
                            index = parentOpNext.getInputs().indexOf(parentOpRef);
                            parentOp = parentOpNext;
                        }
                    }

                    AbstractLogicalOperator exchg = new ExchangeOperator();
                    exchg.setPhysicalOperator(new OneToOneExchangePOperator());

                    ILogicalOperator childOp = parentOp.getOperatorTag() == LogicalOperatorTag.PROJECT ? assignOperator
                            : projectOperator;
                    if (parentOp.isMap()) {
                        parentOp.getInputs().set(index, new MutableObject<ILogicalOperator>(childOp));
                    } else {
                        exchg.getInputs().add(new MutableObject<ILogicalOperator>(childOp));
                        parentOp.getInputs().set(index, new MutableObject<ILogicalOperator>(exchg));
                    }
                    context.computeAndSetTypeEnvironmentForOperator(exchg);
                }
            }
            rewritten = true;
        }
        return rewritten;
    }

    private void genCandidates(IOptimizationContext context) throws AlgebricksException {
        List<List<Mutable<ILogicalOperator>>> previousEquivalenceClasses = new ArrayList<List<Mutable<ILogicalOperator>>>();
        while (equivalenceClasses.size() > 0) {
            previousEquivalenceClasses.clear();
            for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
                List<Mutable<ILogicalOperator>> candidatesCopy = new ArrayList<Mutable<ILogicalOperator>>();
                candidatesCopy.addAll(candidates);
                previousEquivalenceClasses.add(candidatesCopy);
            }
            List<Mutable<ILogicalOperator>> currentLevelOpRefs = new ArrayList<Mutable<ILogicalOperator>>();
            for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
                if (candidates.size() > 0) {
                    for (Mutable<ILogicalOperator> opRef : candidates) {
                        List<Mutable<ILogicalOperator>> refs = childrenToParents.get(opRef);
                        if (refs != null)
                            currentLevelOpRefs.addAll(refs);
                    }
                }
                if (currentLevelOpRefs.size() == 0)
                    continue;
                candidatesGrow(currentLevelOpRefs, candidates);
            }
            if (currentLevelOpRefs.size() == 0)
                break;
            prune(context);
        }
        if (equivalenceClasses.size() < 1 && previousEquivalenceClasses.size() > 0) {
            equivalenceClasses.addAll(previousEquivalenceClasses);
            prune(context);
        }
    }

    private void topDownMaterialization(List<Mutable<ILogicalOperator>> tops) {
        List<Mutable<ILogicalOperator>> candidates = new ArrayList<Mutable<ILogicalOperator>>();
        List<Mutable<ILogicalOperator>> nextLevel = new ArrayList<Mutable<ILogicalOperator>>();
        for (Mutable<ILogicalOperator> op : tops) {
            AbstractLogicalOperator aop = (AbstractLogicalOperator) op.getValue();
            if ((aop.getOperatorTag() == LogicalOperatorTag.INNERJOIN || aop.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN)
                    && !joins.contains(op)) {
                joins.add(op);
            }
            for (Mutable<ILogicalOperator> opRef : op.getValue().getInputs()) {
                List<Mutable<ILogicalOperator>> opRefList = childrenToParents.get(opRef);
                if (opRefList == null) {
                    opRefList = new ArrayList<Mutable<ILogicalOperator>>();
                    childrenToParents.put(opRef, opRefList);
                    nextLevel.add(opRef);
                }
                opRefList.add(op);
            }
            if (op.getValue().getInputs().size() == 0)
                candidates.add(op);
        }
        if (equivalenceClasses.size() > 0) {
            equivalenceClasses.get(0).addAll(candidates);
        } else {
            equivalenceClasses.add(candidates);
        }
        if (nextLevel.size() > 0) {
            topDownMaterialization(nextLevel);
        }
    }

    private void candidatesGrow(List<Mutable<ILogicalOperator>> opList, List<Mutable<ILogicalOperator>> candidates) {
        List<Mutable<ILogicalOperator>> previousCandidates = new ArrayList<Mutable<ILogicalOperator>>();
        previousCandidates.addAll(candidates);
        candidates.clear();
        boolean validCandidate = false;
        for (Mutable<ILogicalOperator> op : opList) {
            for (Mutable<ILogicalOperator> inputRef : op.getValue().getInputs()) {
                validCandidate = false;
                // if current input is in candidates
                for (Mutable<ILogicalOperator> candidate : previousCandidates)
                    if (inputRef.getValue().equals(candidate.getValue()))
                        validCandidate = true;
                // if one input is not in candidates
                if (!validCandidate)
                    break;
            }
            if (!validCandidate)
                continue;
            candidates.add(op);
        }
    }

    private void prune(IOptimizationContext context) throws AlgebricksException {
        List<List<Mutable<ILogicalOperator>>> previousEquivalenceClasses = new ArrayList<List<Mutable<ILogicalOperator>>>();
        for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
            List<Mutable<ILogicalOperator>> candidatesCopy = new ArrayList<Mutable<ILogicalOperator>>();
            candidatesCopy.addAll(candidates);
            previousEquivalenceClasses.add(candidatesCopy);
        }
        equivalenceClasses.clear();
        for (List<Mutable<ILogicalOperator>> candidates : previousEquivalenceClasses) {
            boolean[] reserved = new boolean[candidates.size()];
            for (int i = 0; i < reserved.length; i++)
                reserved[i] = false;
            for (int i = candidates.size() - 1; i >= 0; i--) {
                if (reserved[i] == false) {
                    List<Mutable<ILogicalOperator>> equivalentClass = new ArrayList<Mutable<ILogicalOperator>>();
                    ILogicalOperator candidate = candidates.get(i).getValue();
                    equivalentClass.add(candidates.get(i));
                    for (int j = i - 1; j >= 0; j--) {
                        ILogicalOperator peer = candidates.get(j).getValue();
                        if (IsomorphismUtilities.isOperatorIsomorphic(candidate, peer)) {
                            reserved[i] = true;
                            reserved[j] = true;
                            equivalentClass.add(candidates.get(j));
                        }
                    }
                    if (equivalentClass.size() > 1) {
                        equivalenceClasses.add(equivalentClass);
                        Collections.reverse(equivalentClass);
                    }
                }
            }
            for (int i = candidates.size() - 1; i >= 0; i--) {
                if (!reserved[i]) {
                    candidates.remove(i);
                }
            }
        }
    }

}
