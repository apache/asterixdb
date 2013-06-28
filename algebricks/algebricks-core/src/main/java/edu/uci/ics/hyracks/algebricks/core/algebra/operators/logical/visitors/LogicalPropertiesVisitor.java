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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LogicalPropertiesVectorImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;

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
        if (AlgebricksConfig.DEBUG) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.finest("Logical properties visitor for " + op + ": "
                    + context.getLogicalPropertiesVector(op) + "\n");
        }
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, IOptimizationContext context) throws AlgebricksException {
        visitAssignment(op, context);
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
    public Void visitPartitioningSplitOperator(PartitioningSplitOperator op, IOptimizationContext arg)
            throws AlgebricksException {
        // TODO Auto-generated method stub
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
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, IOptimizationContext context)
            throws AlgebricksException {
        visitAssignment(op, context);
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
    public Void visitUnnestMapOperator(UnnestMapOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
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
    public Void visitInsertDeleteOperator(InsertDeleteOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, IOptimizationContext arg)
            throws AlgebricksException {
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

    private void visitAssignment(AbstractAssignOperator op, IOptimizationContext context) throws AlgebricksException {
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
                    for (Mutable<ILogicalExpression> exprRef : op.getExpressions()) {
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
    public Void visitExtensionOperator(ExtensionOperator op, IOptimizationContext arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

}
