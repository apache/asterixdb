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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
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
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * This visitor propagates primary key information for each operator.
 * NOTE: since the primary key information in data-source-scan is determined by the specific
 * data source, this visitor, at the Algebricks level, could not generate that information.
 */
public class PrimaryKeyVariablesVisitor implements ILogicalOperatorVisitor<Void, IOptimizationContext> {

    @Override
    public Void visitAggregateOperator(AggregateOperator op, IOptimizationContext ctx) throws AlgebricksException {
        ctx.addPrimaryKey(new FunctionalDependency(op.getVariables(), op.getVariables()));
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, IOptimizationContext ctx) throws AlgebricksException {
        List<LogicalVariable> header = new ArrayList<>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gbyTerm : op.getGroupByList()) {
            header.add(gbyTerm.first);
        }
        List<LogicalVariable> liveVars = new ArrayList<>();
        VariableUtilities.getSubplanLocalLiveVariables(op, liveVars);
        ctx.addPrimaryKey(new FunctionalDependency(header, liveVars));
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, IOptimizationContext ctx) throws AlgebricksException {
        // Obtain used variables on the right-hand side of an assign.
        Set<LogicalVariable> usedVars = new HashSet<>();
        VariableUtilities.getUsedVariables(op, usedVars);
        Set<LogicalVariable> primaryKeyVars = null;
        for (LogicalVariable usedVar : usedVars) {
            List<LogicalVariable> keyVars = ctx.findPrimaryKey(usedVar);
            if (keyVars == null) {
                // No key variables can uniquely identify usedVar.
                return null;
            }
            if (primaryKeyVars == null) {
                primaryKeyVars = new HashSet<>(keyVars);
            } else {
                // The primary key is the union of all the key header variables.
                primaryKeyVars.addAll(keyVars);
            }
        }
        if (primaryKeyVars != null && !primaryKeyVars.isEmpty()) {
            List<LogicalVariable> producedVars = new ArrayList<>();
            VariableUtilities.getProducedVariables(op, producedVars);
            // Generates new primary keys.
            ctx.addPrimaryKey(new FunctionalDependency(new ArrayList<LogicalVariable>(primaryKeyVars), producedVars));
        }
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, IOptimizationContext arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, IOptimizationContext ctx) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, IOptimizationContext arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, IOptimizationContext arg) throws AlgebricksException {
        return null;
    }
}
