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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.util.LogRedactionUtil;

public class FDsAndEquivClassesVisitor implements ILogicalOperatorVisitor<Void, IOptimizationContext> {

    @Override
    public Void visitAggregateOperator(AggregateOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ctx.putEquivalenceClassMap(op, new HashMap<LogicalVariable, EquivalenceClass>());
        ctx.putFDList(op, new ArrayList<FunctionalDependency>());
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ILogicalOperator inp1 = op.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> eqClasses = getOrComputeEqClasses(inp1, ctx);
        ctx.putEquivalenceClassMap(op, eqClasses);

        // Propagates equivalence classes that from expressions.
        // Note that an equivalence class can also contain expression members.
        propagateEquivalenceFromExpressionsToVariables(eqClasses, op.getExpressions(), op.getVariables());

        // Generates FDs.
        List<LogicalVariable> used = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, used);
        List<FunctionalDependency> fds1 = getOrComputeFDs(inp1, ctx);
        List<FunctionalDependency> eFds = new ArrayList<FunctionalDependency>(fds1.size());
        for (FunctionalDependency fd : fds1) {
            if (fd.getTail().containsAll(used)) {
                List<LogicalVariable> hd = new ArrayList<LogicalVariable>(fd.getHead());
                List<LogicalVariable> tl = new ArrayList<LogicalVariable>(fd.getTail());
                tl.addAll(op.getVariables());
                FunctionalDependency fd2 = new FunctionalDependency(hd, tl);
                eFds.add(fd2);
            } else {
                eFds.add(fd);
            }
        }
        ctx.putFDList(op, eFds);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ILogicalOperator inp1 = op.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> eqClasses = getOrCreateEqClasses(op, ctx);
        Map<LogicalVariable, EquivalenceClass> propagatedEqClasses = getOrComputeEqClasses(inp1, ctx);
        eqClasses.putAll(propagatedEqClasses);
        ctx.putEquivalenceClassMap(op, eqClasses);
        List<FunctionalDependency> fds = new ArrayList<FunctionalDependency>(getOrComputeFDs(inp1, ctx));
        ctx.putFDList(op, fds);
        op.getDataSource().computeFDs(op.getVariables(), fds);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ILogicalOperator op0 = op.getInputs().get(0).getValue();
        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putFDList(op, functionalDependencies);
        for (FunctionalDependency inherited : getOrComputeFDs(op0, ctx)) {
            boolean isCoveredByDistinctByVars = true;
            for (LogicalVariable v : inherited.getHead()) {
                if (!op.isDistinctByVar(v)) {
                    isCoveredByDistinctByVars = false;
                }
            }
            if (isCoveredByDistinctByVars) {
                List<LogicalVariable> newTail = new ArrayList<LogicalVariable>();
                for (LogicalVariable v2 : inherited.getTail()) {
                    if (op.isDistinctByVar(v2)) {
                        newTail.add(v2);
                    }
                }
                if (!newTail.isEmpty()) {
                    List<LogicalVariable> newHead = new ArrayList<LogicalVariable>(inherited.getHead());
                    FunctionalDependency newFd = new FunctionalDependency(newHead, newTail);
                    functionalDependencies.add(newFd);
                }
            }
        }
        Set<LogicalVariable> gbySet = new HashSet<LogicalVariable>();
        List<Mutable<ILogicalExpression>> expressions = op.getExpressions();
        for (Mutable<ILogicalExpression> pRef : expressions) {
            ILogicalExpression p = pRef.getValue();
            if (p.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) p;
                gbySet.add(v.getVariableReference());
            }
        }
        LocalGroupingProperty lgp = new LocalGroupingProperty(gbySet);

        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = getOrComputeEqClasses(op0, ctx);
        ctx.putEquivalenceClassMap(op, equivalenceClasses);

        ILocalStructuralProperty normalizedLgp = lgp.normalize(equivalenceClasses, functionalDependencies);
        Set<LogicalVariable> normSet = new ListSet<>();
        normalizedLgp.getColumns(normSet);
        List<Mutable<ILogicalExpression>> newDistinctByList = new ArrayList<Mutable<ILogicalExpression>>();
        for (Mutable<ILogicalExpression> p2Ref : expressions) {
            ILogicalExpression p2 = p2Ref.getValue();
            if (p2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression var2 = (VariableReferenceExpression) p2;
                LogicalVariable v2 = var2.getVariableReference();
                if (normSet.contains(v2)) {
                    newDistinctByList.add(p2Ref);
                }
            } else {
                newDistinctByList.add(p2Ref);
            }
        }
        expressions.clear();
        expressions.addAll(newDistinctByList);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        ctx.putEquivalenceClassMap(op, new HashMap<LogicalVariable, EquivalenceClass>());
        ctx.putFDList(op, new ArrayList<FunctionalDependency>());
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, IOptimizationContext ctx) throws AlgebricksException {
        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = new HashMap<LogicalVariable, EquivalenceClass>();
        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putEquivalenceClassMap(op, equivalenceClasses);
        ctx.putFDList(op, functionalDependencies);

        List<FunctionalDependency> inheritedFDs = new ArrayList<FunctionalDependency>();
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                ILogicalOperator op2 = r.getValue();
                equivalenceClasses.putAll(getOrComputeEqClasses(op2, ctx));
                inheritedFDs.addAll(getOrComputeFDs(op2, ctx));
            }
        }

        ILogicalOperator op0 = op.getInputs().get(0).getValue();
        inheritedFDs.addAll(getOrComputeFDs(op0, ctx));
        Map<LogicalVariable, EquivalenceClass> inheritedEcs = getOrComputeEqClasses(op0, ctx);
        for (FunctionalDependency inherited : inheritedFDs) {
            boolean isCoveredByGbyOrDecorVars = true;
            List<LogicalVariable> newHead = new ArrayList<LogicalVariable>(inherited.getHead().size());
            for (LogicalVariable v : inherited.getHead()) {
                LogicalVariable vnew = getNewGbyVar(op, v);
                if (vnew == null) {
                    vnew = getNewDecorVar(op, v);
                    if (vnew == null) {
                        isCoveredByGbyOrDecorVars = false;
                    }
                    break;
                }
                newHead.add(vnew);
            }

            if (isCoveredByGbyOrDecorVars) {
                List<LogicalVariable> newTail = new ArrayList<LogicalVariable>();
                for (LogicalVariable v2 : inherited.getTail()) {
                    LogicalVariable v3 = getNewGbyVar(op, v2);
                    if (v3 != null) {
                        newTail.add(v3);
                    }
                }
                if (!newTail.isEmpty()) {
                    FunctionalDependency newFd = new FunctionalDependency(newHead, newTail);
                    functionalDependencies.add(newFd);
                }
            }
        }

        List<LogicalVariable> premiseGby = new LinkedList<LogicalVariable>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gByList = op.getGroupByList();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            premiseGby.add(p.first);
        }

        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList = op.getDecorList();

        LinkedList<LogicalVariable> conclDecor = new LinkedList<LogicalVariable>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            conclDecor.add(GroupByOperator.getDecorVariable(p));
        }
        if (!conclDecor.isEmpty()) {
            functionalDependencies.add(new FunctionalDependency(premiseGby, conclDecor));
        }

        Set<LogicalVariable> gbySet = new HashSet<LogicalVariable>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                gbySet.add(v.getVariableReference());
            }
        }
        LocalGroupingProperty lgp = new LocalGroupingProperty(gbySet);
        ILocalStructuralProperty normalizedLgp = lgp.normalize(inheritedEcs, inheritedFDs);
        Set<LogicalVariable> normSet = new ListSet<>();
        normalizedLgp.getColumns(normSet);
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> newGbyList = new ArrayList<>();
        boolean changed = false;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                LogicalVariable v2 = varRef.getVariableReference();
                EquivalenceClass ec2 = inheritedEcs.get(v2);
                LogicalVariable v3;
                if (ec2 != null && !ec2.representativeIsConst()) {
                    v3 = ec2.getVariableRepresentative();
                } else {
                    v3 = v2;
                }
                if (normSet.contains(v3)) {
                    newGbyList.add(p);
                } else {
                    changed = true;
                    decorList.add(p);
                }
            } else {
                newGbyList.add(p);
            }
        }
        if (changed && AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">>>> Group-by list changed from {} to {}.\n",
                    LogRedactionUtil.userData(GroupByOperator.veListToString(gByList)),
                    LogRedactionUtil.userData(GroupByOperator.veListToString(newGbyList)));
        }
        gByList.clear();
        gByList.addAll(newGbyList);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, IOptimizationContext ctx) throws AlgebricksException {
        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = new HashMap<LogicalVariable, EquivalenceClass>();
        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putEquivalenceClassMap(op, equivalenceClasses);
        ctx.putFDList(op, functionalDependencies);
        ILogicalOperator op0 = op.getInputs().get(0).getValue();
        ILogicalOperator op1 = op.getInputs().get(1).getValue();
        functionalDependencies.addAll(getOrComputeFDs(op0, ctx));
        functionalDependencies.addAll(getOrComputeFDs(op1, ctx));
        equivalenceClasses.putAll(getOrComputeEqClasses(op0, ctx));
        equivalenceClasses.putAll(getOrComputeEqClasses(op1, ctx));
        ILogicalExpression expr = op.getCondition().getValue();
        expr.getConstraintsAndEquivClasses(functionalDependencies, equivalenceClasses);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = new HashMap<LogicalVariable, EquivalenceClass>();
        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putEquivalenceClassMap(op, equivalenceClasses);
        ctx.putFDList(op, functionalDependencies);
        ILogicalOperator opLeft = op.getInputs().get(0).getValue();
        ILogicalOperator opRight = op.getInputs().get(1).getValue();
        functionalDependencies.addAll(getOrComputeFDs(opLeft, ctx));
        functionalDependencies.addAll(getOrComputeFDs(opRight, ctx));
        equivalenceClasses.putAll(getOrComputeEqClasses(opLeft, ctx));
        equivalenceClasses.putAll(getOrComputeEqClasses(opRight, ctx));

        Collection<LogicalVariable> leftSideVars;
        if (opLeft.getSchema() == null) {
            leftSideVars = new LinkedList<LogicalVariable>();
            VariableUtilities.getLiveVariables(opLeft, leftSideVars);
            // actually, not all produced vars. are visible (due to projection)
            // so using cached schema is better and faster
        } else {
            leftSideVars = opLeft.getSchema();
        }
        ILogicalExpression expr = op.getCondition().getValue();
        expr.getConstraintsForOuterJoin(functionalDependencies, leftSideVars);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op.getDataSourceReference().getValue();
        ILogicalOperator inp1 = op1.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> eqClasses = getOrComputeEqClasses(inp1, ctx);
        ctx.putEquivalenceClassMap(op, eqClasses);
        List<FunctionalDependency> fds = new ArrayList<FunctionalDependency>(getOrComputeFDs(inp1, ctx));
        if (op1.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator gby = (GroupByOperator) op1;
            LinkedList<LogicalVariable> tail = new LinkedList<LogicalVariable>();
            for (LogicalVariable v : gby.getGroupByVarList()) {
                tail.add(v);
                // all values for gby vars. are the same
            }
            FunctionalDependency gbyfd = new FunctionalDependency(new LinkedList<LogicalVariable>(), tail);
            fds.add(gbyfd);
        }
        ctx.putFDList(op, fds);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClassesForUsedVars(op, ctx, op.getVariables());
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        ctx.putEquivalenceClassMap(op, new HashMap<LogicalVariable, EquivalenceClass>());
        ctx.putFDList(op, new ArrayList<FunctionalDependency>());
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClassesForUsedVars(op, ctx, op.getInputVariables());
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ILogicalOperator childOp = op.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = getOrComputeEqClasses(childOp, ctx);
        ctx.putEquivalenceClassMap(op, equivalenceClasses);

        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putFDList(op, functionalDependencies);
        functionalDependencies.addAll(getOrComputeFDs(childOp, ctx));
        equivalenceClasses.putAll(getOrComputeEqClasses(childOp, ctx));
        ILogicalExpression expr = op.getCondition().getValue();
        expr.getConstraintsAndEquivClasses(functionalDependencies, equivalenceClasses);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, IOptimizationContext ctx) throws AlgebricksException {
        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = new HashMap<LogicalVariable, EquivalenceClass>();
        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putEquivalenceClassMap(op, equivalenceClasses);
        ctx.putFDList(op, functionalDependencies);
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                ILogicalOperator op2 = r.getValue();
                equivalenceClasses.putAll(getOrComputeEqClasses(op2, ctx));
                functionalDependencies.addAll(getOrComputeFDs(op2, ctx));
            }
        }
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, IOptimizationContext ctx) throws AlgebricksException {
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, IOptimizationContext ctx) throws AlgebricksException {
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, IOptimizationContext ctx) throws AlgebricksException {
        fdsEqClassesForAbstractUnnestOperator(op, ctx);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        // Unlike the unnest-map operator, we propagate all inputs since
        // propagateInuput is always true.
        Map<LogicalVariable, EquivalenceClass> equivalenceClasses = new HashMap<LogicalVariable, EquivalenceClass>();
        List<FunctionalDependency> functionalDependencies = new ArrayList<FunctionalDependency>();
        ctx.putEquivalenceClassMap(op, equivalenceClasses);
        ctx.putFDList(op, functionalDependencies);
        ILogicalOperator childOp = op.getInputs().get(0).getValue();
        functionalDependencies.addAll(getOrComputeFDs(childOp, ctx));
        equivalenceClasses.putAll(getOrComputeEqClasses(childOp, ctx));

        // Like Left-Outer join case, we add functional dependencies.
        List<LogicalVariable> leftSideVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op, leftSideVars);
        VariableUtilities.getProducedVariables(op, leftSideVars);
        functionalDependencies.add(new FunctionalDependency(leftSideVars, producedVars));
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, IOptimizationContext ctx) throws AlgebricksException {
        fdsEqClassesForAbstractUnnestOperator(op, ctx);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, IOptimizationContext ctx) throws AlgebricksException {
        // propagateFDsAndEquivClasses(op, ctx);
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        // propagateFDsAndEquivClasses(op, ctx);
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, IOptimizationContext ctx) throws AlgebricksException {
        // propagateFDsAndEquivClasses(op, ctx);
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, IOptimizationContext ctx) throws AlgebricksException {
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, IOptimizationContext ctx) throws AlgebricksException {
        setEmptyFDsEqClasses(op, ctx);
        return null;
    }

    private void propagateFDsAndEquivClasses(ILogicalOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ILogicalOperator inp1 = op.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> eqClasses = getOrComputeEqClasses(inp1, ctx);
        ctx.putEquivalenceClassMap(op, eqClasses);
        List<FunctionalDependency> fds = getOrComputeFDs(inp1, ctx);
        ctx.putFDList(op, fds);
    }

    private Map<LogicalVariable, EquivalenceClass> getOrComputeEqClasses(ILogicalOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        Map<LogicalVariable, EquivalenceClass> eqClasses = ctx.getEquivalenceClassMap(op);
        if (eqClasses == null) {
            op.accept(this, ctx);
            eqClasses = ctx.getEquivalenceClassMap(op);
        }
        return eqClasses;
    }

    private Map<LogicalVariable, EquivalenceClass> getOrCreateEqClasses(ILogicalOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        Map<LogicalVariable, EquivalenceClass> eqClasses = ctx.getEquivalenceClassMap(op);
        if (eqClasses == null) {
            eqClasses = new HashMap<LogicalVariable, EquivalenceClass>();
            ctx.putEquivalenceClassMap(op, eqClasses);
        }
        return eqClasses;
    }

    private List<FunctionalDependency> getOrComputeFDs(ILogicalOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        List<FunctionalDependency> fds = ctx.getFDList(op);
        if (fds == null) {
            op.accept(this, ctx);
            fds = ctx.getFDList(op);
        }
        return fds;
    }

    /***
     * Propagated equivalent classes from the child to the current operator,
     * based on the used variables of the current operator.
     *
     * @param op
     *            , the current operator
     * @param ctx
     *            , the optimization context which keeps track of all equivalent
     *            classes.
     * @param usedVariables
     *            , used variables.
     * @throws AlgebricksException
     */
    private void propagateFDsAndEquivClassesForUsedVars(ILogicalOperator op, IOptimizationContext ctx,
            List<LogicalVariable> usedVariables) throws AlgebricksException {
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> eqClasses = getOrCreateEqClasses(op, ctx);
        List<FunctionalDependency> fds = new ArrayList<FunctionalDependency>();
        ctx.putFDList(op, fds);

        Map<LogicalVariable, EquivalenceClass> chldClasses = getOrComputeEqClasses(op2, ctx);

        // Propagate equivalent classes that contain the used variables.
        for (LogicalVariable v : usedVariables) {
            EquivalenceClass ec = eqClasses.get(v);
            if (ec == null) {
                EquivalenceClass oc = chldClasses.get(v);
                if (oc == null) {
                    continue;
                }
                List<LogicalVariable> m = new LinkedList<LogicalVariable>();
                for (LogicalVariable v2 : oc.getMembers()) {
                    if (usedVariables.contains(v2)) {
                        m.add(v2);
                    }
                }
                EquivalenceClass nc;
                if (oc.representativeIsConst()) {
                    nc = new EquivalenceClass(m, oc.getConstRepresentative());
                } else if (m.contains(oc.getVariableRepresentative())) {
                    nc = new EquivalenceClass(m, oc.getVariableRepresentative());
                } else {
                    nc = new EquivalenceClass(m, v);
                }
                for (LogicalVariable v3 : m) {
                    eqClasses.put(v3, nc);
                }
            }
        }

        // Propagates equivalent classes that contain expressions that use the
        // used variables.
        // Note that for the case variable $v is not in the used variables but
        // it is
        // equivalent to field-access($t, i) and $t is a used variable, the
        // equivalent
        // class should still be propagated (kept).
        Set<LogicalVariable> usedVarSet = new HashSet<LogicalVariable>(usedVariables);
        chldClasses.forEach((key, ec) -> {
            for (ILogicalExpression expr : ec.getExpressionMembers()) {
                Set<LogicalVariable> exprUsedVars = new HashSet<>();
                expr.getUsedVariables(exprUsedVars);
                exprUsedVars.retainAll(usedVarSet);
                // Check if the expression member uses a used variable.
                if (!exprUsedVars.isEmpty()) {
                    // If variable members contain a used variable, the representative variable should be a used
                    // variable.
                    ec.getMembers().forEach(v -> {
                        eqClasses.put(v, ec);
                        if (usedVarSet.contains(v)) {
                            ec.setVariableRepresentative(v);
                        }
                    });
                }
            }
        });

        List<FunctionalDependency> chldFds = getOrComputeFDs(op2, ctx);
        for (FunctionalDependency fd : chldFds) {
            if (!usedVariables.containsAll(fd.getHead())) {
                continue;
            }
            List<LogicalVariable> tl = new LinkedList<LogicalVariable>();
            for (LogicalVariable v : fd.getTail()) {
                if (usedVariables.contains(v)) {
                    tl.add(v);
                }
            }
            if (!tl.isEmpty()) {
                FunctionalDependency newFd = new FunctionalDependency(fd.getHead(), tl);
                fds.add(newFd);
            }
        }
    }

    private void fdsEqClassesForAbstractUnnestOperator(AbstractUnnestOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        ILogicalOperator inp1 = op.getInputs().get(0).getValue();
        Map<LogicalVariable, EquivalenceClass> eqClasses = getOrCreateEqClasses(op, ctx);
        Map<LogicalVariable, EquivalenceClass> propagatedEqClasses = getOrComputeEqClasses(inp1, ctx);
        /**
         * The original eq classes of unnest-map are only for produced
         * variables, therefore eqClasses and propagatedEqClasses do not have
         * overlaps.
         */
        eqClasses.putAll(propagatedEqClasses);
        ctx.putEquivalenceClassMap(op, eqClasses);
        List<FunctionalDependency> fds = getOrComputeFDs(inp1, ctx);
        ctx.putFDList(op, fds);

        ILogicalExpression expr = op.getExpressionRef().getValue();
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression afe = (AbstractFunctionCallExpression) expr;
            if (afe.getKind() == FunctionKind.UNNEST && ((UnnestingFunctionCallExpression) afe).returnsUniqueValues()) {
                List<LogicalVariable> vars = new ArrayList<LogicalVariable>();
                VariableUtilities.getLiveVariables(op, vars);
                ArrayList<LogicalVariable> h = new ArrayList<LogicalVariable>();
                h.addAll(op.getVariables());
                FunctionalDependency fd = new FunctionalDependency(h, vars);
                fds.add(fd);
            }
        }
    }

    public static void setEmptyFDsEqClasses(ILogicalOperator op, IOptimizationContext ctx) {
        Map<LogicalVariable, EquivalenceClass> eqClasses = new HashMap<LogicalVariable, EquivalenceClass>();
        ctx.putEquivalenceClassMap(op, eqClasses);
        List<FunctionalDependency> fds = new ArrayList<FunctionalDependency>();
        ctx.putFDList(op, fds);
    }

    private LogicalVariable getNewGbyVar(GroupByOperator g, LogicalVariable v) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : g.getGroupByList()) {
            ILogicalExpression e = p.second.getValue();
            if (e.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable v2 = ((VariableReferenceExpression) e).getVariableReference();
                if (v2 == v) {
                    return p.first;
                }
            }
        }
        return null;
    }

    private LogicalVariable getNewDecorVar(GroupByOperator g, LogicalVariable v) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : g.getDecorList()) {
            ILogicalExpression e = p.second.getValue();
            if (e.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                LogicalVariable v2 = ((VariableReferenceExpression) e).getVariableReference();
                if (v2 == v) {
                    return (p.first != null) ? p.first : v2;
                }
            }
        }
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    /**
     * Propagate equivalences that carried in expressions to the variables that
     * they are assigned to.
     *
     * @param eqClasses
     *            an equivalent class map
     * @param assignExprs
     *            expressions on the right-hand-side of assignments
     * @param assignVars
     *            variables on the left-hand-side of assignments
     */
    private void propagateEquivalenceFromExpressionsToVariables(Map<LogicalVariable, EquivalenceClass> eqClasses,
            List<Mutable<ILogicalExpression>> assignExprs, List<LogicalVariable> assignVars) {
        for (int assignVarIndex = 0; assignVarIndex < assignVars.size(); ++assignVarIndex) {
            LogicalVariable var = assignVars.get(assignVarIndex);
            ILogicalExpression expr = assignExprs.get(assignVarIndex).getValue();
            Map<LogicalVariable, EquivalenceClass> newVarEqcMap = new HashMap<LogicalVariable, EquivalenceClass>();
            eqClasses.forEach((key, eqc) -> {
                // If the equivalence class contains the right-hand-side
                // expression,
                // the left-hand-side variable is added into the equivalence
                // class.
                if (eqc.contains(expr)) {
                    eqc.addMember(var);
                    newVarEqcMap.put(var, eqc); // Add var as a map key for the
                    // equivalence class.
                }
            });
            eqClasses.putAll(newVarEqcMap);
        }
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, IOptimizationContext ctx) throws AlgebricksException {
        propagateFDsAndEquivClasses(op, ctx);
        return null;
    }
}
