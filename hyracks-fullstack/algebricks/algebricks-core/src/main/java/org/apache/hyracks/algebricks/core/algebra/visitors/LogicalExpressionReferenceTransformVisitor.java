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

package org.apache.hyracks.algebricks.core.algebra.visitors;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
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

/**
 * This visitor performs expression transformation on each operator by calling
 * {@link ILogicalOperator#acceptExpressionTransform(ILogicalExpressionReferenceTransform)}.
 * Subclasses can override individual {@code visit*} methods to customize which expressions must be transformed
 * based on the operator kind. This functionality is required in cases when only a subset of operator's expressions
 * must be transformed.
 *
 * @see WindowOperator#acceptExpressionTransform(ILogicalExpressionReferenceTransform, boolean)
 */
public abstract class LogicalExpressionReferenceTransformVisitor
        implements ILogicalOperatorVisitor<Boolean, ILogicalExpressionReferenceTransform> {

    protected boolean visitOperator(ILogicalOperator op, ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        return op.acceptExpressionTransform(transform);
    }

    @Override
    public Boolean visitAggregateOperator(AggregateOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitGroupByOperator(GroupByOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitLimitOperator(LimitOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitInnerJoinOperator(InnerJoinOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitNestedTupleSourceOperator(NestedTupleSourceOperator op,
            ILogicalExpressionReferenceTransform arg) throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitOrderOperator(OrderOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitAssignOperator(AssignOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitSelectOperator(SelectOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitDelegateOperator(DelegateOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitProjectOperator(ProjectOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitReplicateOperator(ReplicateOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitSplitOperator(SplitOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitMaterializeOperator(MaterializeOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitScriptOperator(ScriptOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitSubplanOperator(SubplanOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitSinkOperator(SinkOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitUnionOperator(UnionAllOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitIntersectOperator(IntersectOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitUnnestOperator(UnnestOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitUnnestMapOperator(UnnestMapOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op,
            ILogicalExpressionReferenceTransform arg) throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitDataScanOperator(DataSourceScanOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitDistinctOperator(DistinctOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitExchangeOperator(ExchangeOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitWriteOperator(WriteOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitDistributeResultOperator(DistributeResultOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitWriteResultOperator(WriteResultOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op,
            ILogicalExpressionReferenceTransform arg) throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op,
            ILogicalExpressionReferenceTransform arg) throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitTokenizeOperator(TokenizeOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitForwardOperator(ForwardOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }

    @Override
    public Boolean visitWindowOperator(WindowOperator op, ILogicalExpressionReferenceTransform arg)
            throws AlgebricksException {
        return visitOperator(op, arg);
    }
}
