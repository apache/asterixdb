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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
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
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LogicalPropertiesVectorImpl;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;

public class LogicalPropertiesVisitor implements ILogicalOperatorVisitor<Void, IOptimizationContext> {

    public static void computeLogicalPropertiesDFS(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        LogicalPropertiesVisitor visitor = new LogicalPropertiesVisitor();
        computeLogicalPropertiesRec(op, visitor, context);
    }

    private static void computeLogicalPropertiesRec(ILogicalOperator op, LogicalPropertiesVisitor visitor,
            IOptimizationContext context) throws AlgebricksException {
        for (Mutable<ILogicalOperator> ref : op.getInputs()) {
            computeLogicalPropertiesRec(ref.getValue(), visitor, context);
        }
        op.accept(visitor, context);
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(
                    "Logical properties visitor for " + op + ": " + context.getLogicalPropertiesVector(op) + "\n");
        }
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, IOptimizationContext context) throws AlgebricksException {
        visitAssignment(op, op.getExpressions(), context);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, IOptimizationContext arg) throws AlgebricksException {
        propagateCardinalityAndFrameNumber(op, arg);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, IOptimizationContext arg) throws AlgebricksException {
        Object annot1 = op.getAnnotations().get(OperatorAnnotations.CARDINALITY);
        if (annot1 == null) {
            return null;
        }
        Integer m = (Integer) annot1;
        LogicalPropertiesVectorImpl v = new LogicalPropertiesVectorImpl();
        v.setNumberOfTuples(m);
        Object annot2 = op.getAnnotations().get(OperatorAnnotations.MAX_NUMBER_FRAMES);
        if (annot2 != null) {
            Integer f = (Integer) annot2;
            v.setMaxOutputFrames(f);
        }
        arg.putLogicalPropertiesVector(op, v);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, IOptimizationContext context) throws AlgebricksException {
        propagateCardinalityAndFrameNumber(op, context);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, IOptimizationContext arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, IOptimizationContext context)
            throws AlgebricksException {
        visitAssignment(op, op.getExpressions(), context);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, IOptimizationContext context) throws AlgebricksException {
        visitAssignment(op, op.getExpressions(), context);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, IOptimizationContext arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    private LogicalPropertiesVectorImpl propagateCardinality(ILogicalOperator op, IOptimizationContext context) {
        ILogicalOperator op0 = op.getInputs().get(0).getValue();
        ILogicalPropertiesVector v0 = context.getLogicalPropertiesVector(op0);
        if (v0 == null) {
            return null;
        }
        LogicalPropertiesVectorImpl v = new LogicalPropertiesVectorImpl();
        v.setNumberOfTuples(v0.getNumberOfTuples());
        context.putLogicalPropertiesVector(op, v);
        return v;
    }

    private void visitAssignment(ILogicalOperator op, List<Mutable<ILogicalExpression>> exprList,
            IOptimizationContext context) throws AlgebricksException {
        LogicalPropertiesVectorImpl v = propagateCardinality(op, context);
        if (v != null && v.getNumberOfTuples() != null) {
            IVariableEvalSizeEnvironment varSizeEnv = context.getVariableEvalSizeEnvironment();
            IExpressionEvalSizeComputer evalSize = context.getExpressionEvalSizeComputer();
            if (evalSize != null) {
                ILogicalOperator op0 = op.getInputs().get(0).getValue();
                ILogicalPropertiesVector v0 = context.getLogicalPropertiesVector(op0);
                if (v0 != null) {
                    long frames0 = v0.getMaxOutputFrames();
                    long overhead = 0; // added per tuple
                    for (Mutable<ILogicalExpression> exprRef : exprList) {
                        int sz = evalSize.getEvalSize(exprRef.getValue(), varSizeEnv);
                        if (sz == -1) {
                            return;
                        }
                        overhead += sz;
                    }
                    int frameSize = context.getPhysicalOptimizationConfig().getFrameSize();
                    if (frameSize > 0) {
                        long sz = frames0 * frameSize + overhead * v.getNumberOfTuples();
                        int frames1 = (int) (sz / frameSize);
                        if (sz % frameSize > 0) {
                            frames1++;
                        }
                        v.setMaxOutputFrames(frames1);
                    }
                }
            }
        }
    }

    public void propagateCardinalityAndFrameNumber(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        LogicalPropertiesVectorImpl v = propagateCardinality(op, context);
        // propagate also max number of frames (conservatively)
        ILogicalOperator op0 = op.getInputs().get(0).getValue();
        ILogicalPropertiesVector v0 = context.getLogicalPropertiesVector(op0);
        if (v0 != null) {
            v.setMaxOutputFrames(v0.getMaxOutputFrames());
        }
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        return null;
    }
}
