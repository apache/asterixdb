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
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Pre-conditions:
 *      FixReplicateOperatorOutputsRule should be fired
 */
public class ExtractCommonOperatorsRule implements IAlgebraicRewriteRule {

    private final HashMap<Mutable<ILogicalOperator>, List<Mutable<ILogicalOperator>>> childrenToParents =
            new HashMap<>();
    private final List<Mutable<ILogicalOperator>> roots = new ArrayList<>();
    private final List<List<Mutable<ILogicalOperator>>> equivalenceClasses = new ArrayList<>();
    private final HashMap<Mutable<ILogicalOperator>, BitSet> opToCandidateInputs = new HashMap<>();
    private final HashMap<Mutable<ILogicalOperator>, MutableInt> clusterMap = new HashMap<>();
    private final HashMap<Integer, BitSet> clusterWaitForMap = new HashMap<>();
    private int lastUsedClusterId = 0;
    private final Map<Mutable<ILogicalOperator>, BitSet> replicateToOutputs = new HashMap<>();
    private final List<Pair<Mutable<ILogicalOperator>, Boolean>> newOutputs = new ArrayList<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.WRITE && op.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return false;
        }
        MutableObject<ILogicalOperator> mutableOp = new MutableObject<>(op);
        if (!roots.contains(mutableOp)) {
            roots.add(mutableOp);
        }
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
        boolean changed;
        if (!roots.isEmpty()) {
            do {
                changed = false;
                // applying the rewriting until fixpoint
                topDownMaterialization(roots);
                genCandidates(context);
                removeTrivialShare();
                if (!equivalenceClasses.isEmpty()) {
                    changed = rewrite(context);
                }
                if (!rewritten) {
                    rewritten = changed;
                }
                equivalenceClasses.clear();
                childrenToParents.clear();
                opToCandidateInputs.clear();
                clusterMap.clear();
                clusterWaitForMap.clear();
                lastUsedClusterId = 0; // Resets lastUsedClusterId to 0.
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
                if (aop.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                    aop = (AbstractLogicalOperator) aop.getInputs().get(0).getValue();
                }
                if (aop.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    candidates.remove(i);
                }
            }
        }
        for (int i = equivalenceClasses.size() - 1; i >= 0; i--) {
            if (equivalenceClasses.get(i).size() < 2) {
                equivalenceClasses.remove(i);
            }
        }
    }

    private boolean rewrite(IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        for (List<Mutable<ILogicalOperator>> members : equivalenceClasses) {
            if (rewriteForOneEquivalentClass(members, context)) {
                changed = true;
            }
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
            boolean[] materializationFlags = computeMaterilizationFlags(group);
            if (group.isEmpty()) {
                continue;
            }
            candidate = group.get(0);
            SourceLocation candidateSourceLoc = candidate.getValue().getSourceLocation();
            ReplicateOperator rop = new ReplicateOperator(group.size(), materializationFlags);
            rop.setSourceLocation(candidateSourceLoc);
            rop.setPhysicalOperator(new ReplicatePOperator());
            Mutable<ILogicalOperator> ropRef = new MutableObject<ILogicalOperator>(rop);
            AbstractLogicalOperator aopCandidate = (AbstractLogicalOperator) candidate.getValue();
            List<Mutable<ILogicalOperator>> originalCandidateParents = childrenToParents.get(candidate);

            rop.setExecutionMode(((AbstractLogicalOperator) candidate.getValue()).getExecutionMode());
            if (aopCandidate.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                rop.getInputs().add(candidate);
            } else {
                AbstractLogicalOperator beforeExchange = new ExchangeOperator();
                beforeExchange.setPhysicalOperator(new OneToOneExchangePOperator());
                beforeExchange.setExecutionMode(rop.getExecutionMode());
                Mutable<ILogicalOperator> beforeExchangeRef = new MutableObject<ILogicalOperator>(beforeExchange);
                beforeExchange.getInputs().add(candidate);
                context.computeAndSetTypeEnvironmentForOperator(beforeExchange);
                rop.getInputs().add(beforeExchangeRef);
            }
            context.computeAndSetTypeEnvironmentForOperator(rop);

            for (Mutable<ILogicalOperator> parentRef : originalCandidateParents) {
                AbstractLogicalOperator parent = (AbstractLogicalOperator) parentRef.getValue();
                int index = parent.getInputs().indexOf(candidate);
                if (parent.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                    parent.getInputs().set(index, ropRef);
                    rop.getOutputs().add(parentRef);
                } else {
                    AbstractLogicalOperator exchange = new ExchangeOperator();
                    exchange.setPhysicalOperator(new OneToOneExchangePOperator());
                    exchange.setExecutionMode(rop.getExecutionMode());
                    MutableObject<ILogicalOperator> exchangeRef = new MutableObject<ILogicalOperator>(exchange);
                    exchange.getInputs().add(ropRef);
                    rop.getOutputs().add(exchangeRef);
                    context.computeAndSetTypeEnvironmentForOperator(exchange);
                    parent.getInputs().set(index, exchangeRef);
                    context.computeAndSetTypeEnvironmentForOperator(parent);
                }
            }
            List<LogicalVariable> liveVarsNew = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(candidate.getValue(), liveVarsNew);
            ArrayList<Mutable<ILogicalExpression>> assignExprs = new ArrayList<Mutable<ILogicalExpression>>();
            for (LogicalVariable liveVar : liveVarsNew) {
                VariableReferenceExpression liveVarRef = new VariableReferenceExpression(liveVar);
                liveVarRef.setSourceLocation(candidateSourceLoc);
                assignExprs.add(new MutableObject<ILogicalExpression>(liveVarRef));
            }
            for (Mutable<ILogicalOperator> ref : group) {
                if (ref.equals(candidate)) {
                    continue;
                }
                ArrayList<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
                Map<LogicalVariable, LogicalVariable> variableMappingBack =
                        new HashMap<LogicalVariable, LogicalVariable>();
                IsomorphismUtilities.mapVariablesTopDown(ref.getValue(), candidate.getValue(), variableMappingBack);
                for (int i = 0; i < liveVarsNew.size(); i++) {
                    liveVars.add(variableMappingBack.get(liveVarsNew.get(i)));
                }

                SourceLocation refSourceLoc = ref.getValue().getSourceLocation();

                AbstractLogicalOperator assignOperator = new AssignOperator(liveVars, assignExprs);
                assignOperator.setSourceLocation(refSourceLoc);
                assignOperator.setExecutionMode(rop.getExecutionMode());
                assignOperator.setPhysicalOperator(new AssignPOperator());
                AbstractLogicalOperator projectOperator = new ProjectOperator(liveVars);
                projectOperator.setSourceLocation(refSourceLoc);
                projectOperator.setPhysicalOperator(new StreamProjectPOperator());
                projectOperator.setExecutionMode(rop.getExecutionMode());
                AbstractLogicalOperator exchOp = new ExchangeOperator();
                exchOp.setPhysicalOperator(new OneToOneExchangePOperator());
                exchOp.setExecutionMode(rop.getExecutionMode());
                exchOp.getInputs().add(ropRef);
                MutableObject<ILogicalOperator> exchOpRef = new MutableObject<ILogicalOperator>(exchOp);
                rop.getOutputs().add(exchOpRef);
                assignOperator.getInputs().add(exchOpRef);
                projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));

                // set the types
                context.computeAndSetTypeEnvironmentForOperator(exchOp);
                context.computeAndSetTypeEnvironmentForOperator(assignOperator);
                context.computeAndSetTypeEnvironmentForOperator(projectOperator);

                List<Mutable<ILogicalOperator>> parentOpList = childrenToParents.get(ref);
                for (Mutable<ILogicalOperator> parentOpRef : parentOpList) {
                    AbstractLogicalOperator parentOp = (AbstractLogicalOperator) parentOpRef.getValue();
                    int index = parentOp.getInputs().indexOf(ref);
                    ILogicalOperator childOp =
                            parentOp.getOperatorTag() == LogicalOperatorTag.PROJECT ? assignOperator : projectOperator;
                    if (parentOp.getPhysicalOperator().isMicroOperator()
                            || parentOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
                        parentOp.getInputs().set(index, new MutableObject<ILogicalOperator>(childOp));
                    } else {
                        // If the parent operator is a hyracks operator,
                        // an extra one-to-one exchange is needed.
                        AbstractLogicalOperator exchg = new ExchangeOperator();
                        exchg.setPhysicalOperator(new OneToOneExchangePOperator());
                        exchg.setExecutionMode(childOp.getExecutionMode());
                        exchg.getInputs().add(new MutableObject<ILogicalOperator>(childOp));
                        parentOp.getInputs().set(index, new MutableObject<ILogicalOperator>(exchg));
                        context.computeAndSetTypeEnvironmentForOperator(exchg);
                    }
                    context.computeAndSetTypeEnvironmentForOperator(parentOp);
                }
            }
            cleanupPlan();
            rewritten = true;
        }
        return rewritten;
    }

    /**
     * Cleans up the plan after combining similar branches into one branch making sure parents & children point to
     * each other correctly.
     */
    private void cleanupPlan() {
        for (Mutable<ILogicalOperator> root : roots) {
            replicateToOutputs.clear();
            newOutputs.clear();
            findReplicateOp(root, replicateToOutputs);
            cleanup(replicateToOutputs, newOutputs);
        }
    }

    /**
     * Updates the outputs references of a replicate operator to points to the valid parents.
     * @param replicateToOutputs where the replicate operators are stored with its valid parents.
     * @param newOutputs the valid parents of replicate operator.
     */
    private void cleanup(Map<Mutable<ILogicalOperator>, BitSet> replicateToOutputs,
            List<Pair<Mutable<ILogicalOperator>, Boolean>> newOutputs) {
        replicateToOutputs.forEach((repRef, allOutputs) -> {
            newOutputs.clear();
            // get the indexes that are set in the BitSet
            allOutputs.stream().forEach(outIndex -> {
                newOutputs.add(new Pair<>(((AbstractReplicateOperator) repRef.getValue()).getOutputs().get(outIndex),
                        ((AbstractReplicateOperator) repRef.getValue()).getOutputMaterializationFlags()[outIndex]));
            });
            ((AbstractReplicateOperator) repRef.getValue()).setOutputs(newOutputs);
        });
    }

    /**
     * Collects all replicate operator starting from {@param parent} and all its descendants and keeps track of the
     * valid parents of a replicate operator. The indexes of valid parents will be set in the BitSet.
     * @param parent the current operator in consideration for which we want to find replicate op children.
     * @param replicateToOutputs where the replicate operators will be stored with all its parents (valid & invalid).
     */
    private void findReplicateOp(Mutable<ILogicalOperator> parent,
            Map<Mutable<ILogicalOperator>, BitSet> replicateToOutputs) {
        List<Mutable<ILogicalOperator>> children = parent.getValue().getInputs();
        for (Mutable<ILogicalOperator> childRef : children) {
            AbstractLogicalOperator child = (AbstractLogicalOperator) childRef.getValue();
            if (child.getOperatorTag() == LogicalOperatorTag.REPLICATE
                    || child.getOperatorTag() == LogicalOperatorTag.SPLIT) {
                AbstractReplicateOperator replicateChild = (AbstractReplicateOperator) child;
                int parentIndex = replicateChild.getOutputs().indexOf(parent);
                if (parentIndex >= 0) {
                    BitSet replicateValidOutputs = replicateToOutputs.get(childRef);
                    if (replicateValidOutputs == null) {
                        replicateValidOutputs = new BitSet();
                        replicateToOutputs.put(childRef, replicateValidOutputs);
                    }
                    replicateValidOutputs.set(parentIndex);
                }
            }
            findReplicateOp(childRef, replicateToOutputs);
        }
    }

    private void genCandidates(IOptimizationContext context) throws AlgebricksException {
        List<List<Mutable<ILogicalOperator>>> previousEquivalenceClasses =
                new ArrayList<List<Mutable<ILogicalOperator>>>();
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
                        if (refs != null) {
                            currentLevelOpRefs.addAll(refs);
                        }
                    }
                }
                if (currentLevelOpRefs.size() == 0) {
                    continue;
                }
                candidatesGrow(currentLevelOpRefs, candidates);
            }
            if (currentLevelOpRefs.size() == 0) {
                break;
            }
            prune(context);
        }
        if (equivalenceClasses.size() < 1 && previousEquivalenceClasses.size() > 0) {
            equivalenceClasses.addAll(previousEquivalenceClasses);
            prune(context);
        }
    }

    private void topDownMaterialization(List<Mutable<ILogicalOperator>> tops) {
        List<Mutable<ILogicalOperator>> candidates = new ArrayList<>();
        List<Mutable<ILogicalOperator>> nextLevel = new ArrayList<>();
        for (Mutable<ILogicalOperator> op : tops) {
            for (Mutable<ILogicalOperator> opRef : op.getValue().getInputs()) {
                List<Mutable<ILogicalOperator>> opRefList = childrenToParents.get(opRef);
                if (opRefList == null) {
                    opRefList = new ArrayList<>();
                    childrenToParents.put(opRef, opRefList);
                    nextLevel.add(opRef);
                }
                opRefList.add(op);
            }
            if (op.getValue().getInputs().isEmpty()) {
                candidates.add(op);
            }
        }
        if (!equivalenceClasses.isEmpty()) {
            equivalenceClasses.get(0).addAll(candidates);
        } else {
            equivalenceClasses.add(candidates);
        }
        if (!nextLevel.isEmpty()) {
            topDownMaterialization(nextLevel);
        }
    }

    private void candidatesGrow(List<Mutable<ILogicalOperator>> opList, List<Mutable<ILogicalOperator>> candidates) {
        List<Mutable<ILogicalOperator>> previousCandidates = new ArrayList<Mutable<ILogicalOperator>>();
        previousCandidates.addAll(candidates);
        candidates.clear();
        boolean validCandidate = false;
        for (Mutable<ILogicalOperator> op : opList) {
            List<Mutable<ILogicalOperator>> inputs = op.getValue().getInputs();
            for (int i = 0; i < inputs.size(); i++) {
                Mutable<ILogicalOperator> inputRef = inputs.get(i);
                validCandidate = false;
                for (Mutable<ILogicalOperator> candidate : previousCandidates) {
                    // if current input is in candidates
                    if (inputRef.getValue().equals(candidate.getValue())) {
                        if (inputs.size() == 1) {
                            validCandidate = true;
                        } else {
                            BitSet candidateInputBitMap = opToCandidateInputs.get(op);
                            if (candidateInputBitMap == null) {
                                candidateInputBitMap = new BitSet(inputs.size());
                                opToCandidateInputs.put(op, candidateInputBitMap);
                            }
                            candidateInputBitMap.set(i);
                            if (candidateInputBitMap.cardinality() == inputs.size()) {
                                validCandidate = true;
                            }
                        }
                        break;
                    }
                }
            }
            if (!validCandidate) {
                continue;
            }
            if (!candidates.contains(op)) {
                candidates.add(op);
            }
        }
    }

    private void prune(IOptimizationContext context) throws AlgebricksException {
        List<List<Mutable<ILogicalOperator>>> previousEquivalenceClasses =
                new ArrayList<List<Mutable<ILogicalOperator>>>();
        for (List<Mutable<ILogicalOperator>> candidates : equivalenceClasses) {
            List<Mutable<ILogicalOperator>> candidatesCopy = new ArrayList<Mutable<ILogicalOperator>>();
            candidatesCopy.addAll(candidates);
            previousEquivalenceClasses.add(candidatesCopy);
        }
        equivalenceClasses.clear();
        for (List<Mutable<ILogicalOperator>> candidates : previousEquivalenceClasses) {
            boolean[] reserved = new boolean[candidates.size()];
            for (int i = 0; i < reserved.length; i++) {
                reserved[i] = false;
            }
            for (int i = candidates.size() - 1; i >= 0; i--) {
                if (reserved[i] == false) {
                    List<Mutable<ILogicalOperator>> equivalentClass = new ArrayList<Mutable<ILogicalOperator>>();
                    ILogicalOperator candidate = candidates.get(i).getValue();
                    equivalentClass.add(candidates.get(i));
                    for (int j = i - 1; j >= 0; j--) {
                        ILogicalOperator peer = candidates.get(j).getValue();
                        boolean isomorphic = candidate.getInputs().size() > 1
                                ? IsomorphismUtilities.isOperatorIsomorphicPlanSegment(candidate, peer)
                                : IsomorphismUtilities.isOperatorIsomorphic(candidate, peer);
                        if (isomorphic) {
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

    private boolean[] computeMaterilizationFlags(List<Mutable<ILogicalOperator>> group) {
        for (Mutable<ILogicalOperator> root : roots) {
            computeClusters(null, root, new MutableInt(++lastUsedClusterId));
        }
        boolean[] materializationFlags = new boolean[group.size()];
        boolean worthMaterialization = worthMaterialization(group.get(0));
        boolean requiresMaterialization;
        // get clusterIds for each candidate in the group
        List<Integer> groupClusterIds = new ArrayList<Integer>(group.size());
        for (int i = 0; i < group.size(); i++) {
            groupClusterIds.add(clusterMap.get(group.get(i)).getValue());
        }
        for (int i = group.size() - 1; i >= 0; i--) {
            requiresMaterialization = requiresMaterialization(groupClusterIds, i);
            if (requiresMaterialization && !worthMaterialization) {
                group.remove(i);
                groupClusterIds.remove(i);
            }
            materializationFlags[i] = requiresMaterialization;
        }
        if (group.size() < 2) {
            group.clear();
        }
        // if does not worth materialization, the flags for the remaining candidates should be false
        return worthMaterialization ? materializationFlags : new boolean[group.size()];
    }

    private boolean requiresMaterialization(List<Integer> groupClusterIds, int index) {
        Integer clusterId = groupClusterIds.get(index);
        BitSet blockingClusters = new BitSet();
        getAllBlockingClusterIds(clusterId, blockingClusters);
        if (!blockingClusters.isEmpty()) {
            for (int i = 0; i < groupClusterIds.size(); i++) {
                if (i == index) {
                    continue;
                }
                if (blockingClusters.get(groupClusterIds.get(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private void getAllBlockingClusterIds(int clusterId, BitSet blockingClusters) {
        BitSet waitFor = clusterWaitForMap.get(clusterId);
        if (waitFor != null) {
            for (int i = waitFor.nextSetBit(0); i >= 0; i = waitFor.nextSetBit(i + 1)) {
                getAllBlockingClusterIds(i, blockingClusters);
            }
            blockingClusters.or(waitFor);
        }
    }

    private void computeClusters(Mutable<ILogicalOperator> parentRef, Mutable<ILogicalOperator> opRef,
            MutableInt currentClusterId) {
        // only replicate or split operator has multiple outputs
        int outputIndex = 0;
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.REPLICATE
                || opRef.getValue().getOperatorTag() == LogicalOperatorTag.SPLIT) {
            AbstractReplicateOperator rop = (AbstractReplicateOperator) opRef.getValue();
            List<Mutable<ILogicalOperator>> outputs = rop.getOutputs();
            for (outputIndex = 0; outputIndex < outputs.size(); outputIndex++) {
                if (outputs.get(outputIndex).equals(parentRef)) {
                    break;
                }
            }
        }
        AbstractLogicalOperator aop = (AbstractLogicalOperator) opRef.getValue();
        Pair<int[], int[]> labels = aop.getPhysicalOperator().getInputOutputDependencyLabels(opRef.getValue());
        List<Mutable<ILogicalOperator>> inputs = opRef.getValue().getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            Mutable<ILogicalOperator> inputRef = inputs.get(i);
            if (labels.second[outputIndex] == 1 && labels.first[i] == 0) { // 1 -> 0
                if (labels.second.length == 1) {
                    clusterMap.put(opRef, currentClusterId);
                    // start a new cluster
                    MutableInt newClusterId = new MutableInt(++lastUsedClusterId);
                    computeClusters(opRef, inputRef, newClusterId);
                    BitSet waitForList = clusterWaitForMap.get(currentClusterId.getValue());
                    if (waitForList == null) {
                        waitForList = new BitSet();
                        clusterWaitForMap.put(currentClusterId.getValue(), waitForList);
                    }
                    waitForList.set(newClusterId.getValue());
                }
            } else { // 0 -> 0 and 1 -> 1
                MutableInt prevClusterId = clusterMap.get(opRef);
                if (prevClusterId == null || prevClusterId.getValue().equals(currentClusterId.getValue())) {
                    clusterMap.put(opRef, currentClusterId);
                    computeClusters(opRef, inputRef, currentClusterId);
                } else {
                    // merge prevClusterId and currentClusterId: update all the map entries that has currentClusterId to prevClusterId
                    for (BitSet bs : clusterWaitForMap.values()) {
                        if (bs.get(currentClusterId.getValue())) {
                            bs.clear(currentClusterId.getValue());
                            bs.set(prevClusterId.getValue());
                        }
                    }
                    clusterWaitForMap.remove(currentClusterId.getValue());
                    currentClusterId.setValue(prevClusterId.getValue());
                }
            }
        }
    }

    protected boolean worthMaterialization(Mutable<ILogicalOperator> candidate) {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) candidate.getValue();
        if (aop.getPhysicalOperator().expensiveThanMaterialization()) {
            return true;
        }
        List<Mutable<ILogicalOperator>> inputs = candidate.getValue().getInputs();
        for (Mutable<ILogicalOperator> inputRef : inputs) {
            if (worthMaterialization(inputRef)) {
                return true;
            }
        }
        return false;
    }
}
