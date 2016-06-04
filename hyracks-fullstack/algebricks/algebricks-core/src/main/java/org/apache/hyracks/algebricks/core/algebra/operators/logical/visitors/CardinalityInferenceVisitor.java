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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * A visitor that provides the basic inference of tuple cardinalities of an
 * operator's output. There are only two cases: 1. the cardinality is one in the
 * worst case; 2. the cardinality is some unknown value.
 */
public class CardinalityInferenceVisitor implements ILogicalOperatorVisitor<Long, Void> {
    private static final Long ONE = 1L;
    private static final Long UNKNOWN = 1000L;

    @Override
    public Long visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        return ONE;
    }

    @Override
    public Long visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        // Empty tuple source sends one empty tuple to kick off the pipeline.
        return ONE;
    }

    @Override
    public Long visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        // This is only a worst-case estimate
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        return visitJoin(op, arg);
    }

    @Override
    public Long visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        return visitJoin(op, arg);
    }

    @Override
    public Long visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        return ONE;
    }

    @Override
    public Long visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        // This is only a worst-case inference.
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitExtensionOperator(ExtensionOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitPartitioningSplitOperator(PartitioningSplitOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        long cardinality = UNKNOWN;
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            Long branchCardinality = inputOpRef.getValue().accept(this, arg);
            if (branchCardinality < cardinality) {
                cardinality = branchCardinality;
            }
        }
        return cardinality;
    }

    private long visitJoin(ILogicalOperator op, Void arg) throws AlgebricksException {
        long cardinality = 1L;
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            cardinality *= inputOpRef.getValue().accept(this, arg);
        }
        if (cardinality > ONE) {
            cardinality = UNKNOWN;
        }
        return cardinality;
    }

}
