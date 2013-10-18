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
package edu.uci.ics.hyracks.algebricks.core.algebra.visitors;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;

public interface ILogicalOperatorVisitor<R, T> {

    public R visitAggregateOperator(AggregateOperator op, T arg) throws AlgebricksException;

    public R visitRunningAggregateOperator(RunningAggregateOperator op, T arg) throws AlgebricksException;

    public R visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, T arg) throws AlgebricksException;

    public R visitGroupByOperator(GroupByOperator op, T arg) throws AlgebricksException;

    public R visitLimitOperator(LimitOperator op, T arg) throws AlgebricksException;

    public R visitInnerJoinOperator(InnerJoinOperator op, T arg) throws AlgebricksException;

    public R visitLeftOuterJoinOperator(LeftOuterJoinOperator op, T arg) throws AlgebricksException;

    public R visitNestedTupleSourceOperator(NestedTupleSourceOperator op, T arg) throws AlgebricksException;

    public R visitOrderOperator(OrderOperator op, T arg) throws AlgebricksException;

    public R visitAssignOperator(AssignOperator op, T arg) throws AlgebricksException;

    public R visitSelectOperator(SelectOperator op, T arg) throws AlgebricksException;

    public R visitExtensionOperator(ExtensionOperator op, T arg) throws AlgebricksException;

    public R visitProjectOperator(ProjectOperator op, T arg) throws AlgebricksException;

    public R visitPartitioningSplitOperator(PartitioningSplitOperator op, T arg) throws AlgebricksException;

    public R visitReplicateOperator(ReplicateOperator op, T arg) throws AlgebricksException;

    public R visitScriptOperator(ScriptOperator op, T arg) throws AlgebricksException;

    public R visitSubplanOperator(SubplanOperator op, T arg) throws AlgebricksException;

    public R visitSinkOperator(SinkOperator op, T arg) throws AlgebricksException;

    public R visitUnionOperator(UnionAllOperator op, T arg) throws AlgebricksException;

    public R visitUnnestOperator(UnnestOperator op, T arg) throws AlgebricksException;

    public R visitUnnestMapOperator(UnnestMapOperator op, T arg) throws AlgebricksException;

    public R visitDataScanOperator(DataSourceScanOperator op, T arg) throws AlgebricksException;

    public R visitDistinctOperator(DistinctOperator op, T arg) throws AlgebricksException;

    public R visitExchangeOperator(ExchangeOperator op, T arg) throws AlgebricksException;

    public R visitWriteOperator(WriteOperator op, T arg) throws AlgebricksException;

    public R visitDistributeResultOperator(DistributeResultOperator op, T arg) throws AlgebricksException;

    public R visitWriteResultOperator(WriteResultOperator op, T arg) throws AlgebricksException;

    public R visitInsertDeleteOperator(InsertDeleteOperator op, T tag) throws AlgebricksException;

    public R visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, T tag) throws AlgebricksException;

}
