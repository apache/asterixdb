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
package org.apache.hyracks.algebricks.rewriter.rules.subplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.visitors.IQueryOperatorVisitor;

/**
 * This visitor replaces NTS' in a subplan with its input operator or its deep
 * copies. Note that this visitor can only be used in the rule
 * EliminateSubplanWithInputCardinalityOneRule, for cases where the Subplan
 * input operator is of cardinality one and its variables are not needed after
 * the Subplan.
 */
class ReplaceNtsWithSubplanInputOperatorVisitor implements IQueryOperatorVisitor<ILogicalOperator, Void> {
    // The optimization context.
    private final IOptimizationContext ctx;

    // The input operator to the subplan.
    private final ILogicalOperator subplanInputOperator;

    // The map that maps the input variables to the subplan to their deep-copied
    // variables.
    private final LinkedHashMap<LogicalVariable, LogicalVariable> varMap = new LinkedHashMap<>();

    // Whether the original copy has been used.
    private boolean isOriginalCopyUsed = false;

    /**
     * @param context
     *            the optimization context
     * @param subplanOperator
     *            the subplan operator this visitor deals with.
     * @throws AlgebricksException
     */
    public ReplaceNtsWithSubplanInputOperatorVisitor(IOptimizationContext context, ILogicalOperator subplanOperator)
            throws AlgebricksException {
        this.ctx = context;
        this.subplanInputOperator = subplanOperator.getInputs().get(0).getValue();
    }

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, Void arg)
            throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        if (!isOriginalCopyUsed) {
            isOriginalCopyUsed = true;
            return subplanInputOperator;
        }
        LogicalOperatorDeepCopyWithNewVariablesVisitor visitor =
                new LogicalOperatorDeepCopyWithNewVariablesVisitor(ctx, ctx);
        ILogicalOperator copiedSubplanInputOperator = visitor.deepCopy(subplanInputOperator);
        varMap.putAll(visitor.getInputToOutputVariableMapping());
        return copiedSubplanInputOperator;
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg)
            throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
            throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    @Override
    public ILogicalOperator visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        return visit(op);
    }

    private ILogicalOperator visit(ILogicalOperator op) throws AlgebricksException {
        List<Map<LogicalVariable, LogicalVariable>> varMapSnapshots = new ArrayList<>();
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            ILogicalOperator newChild = childRef.getValue().accept(this, null);
            childRef.setValue(newChild);
            // Replaces variables in op with the mapping obtained from one
            // child.
            VariableUtilities.substituteVariables(op, varMap, ctx);
            // Keep the map from current child and move to the next child.
            varMapSnapshots.add(new HashMap<LogicalVariable, LogicalVariable>(varMap));
            varMap.clear();
        }

        // Combine mappings from all children.
        for (Map<LogicalVariable, LogicalVariable> map : varMapSnapshots) {
            varMap.putAll(map);
        }

        // Only propagates necessary mappings to the parent operator.
        Set<LogicalVariable> liveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(op, liveVars);
        varMap.values().retainAll(liveVars);
        return op;
    }

}
