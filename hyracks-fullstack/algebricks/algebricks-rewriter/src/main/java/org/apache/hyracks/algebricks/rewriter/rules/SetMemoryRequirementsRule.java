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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
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
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * Set memory requirements for all operators as follows:
 * <ol>
 * <li>First call {@link IPhysicalOperator#createLocalMemoryRequirements(ILogicalOperator)}
 *     to initialize each operator's {@link LocalMemoryRequirements} with minimal memory budget required by
 *     that operator</li>
 * <li>Then increase memory requirements for certain operators as specified by {@link PhysicalOptimizationConfig}</li>
 * </ol>
 */
public class SetMemoryRequirementsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        IPhysicalOperator physOp = op.getPhysicalOperator();
        if (physOp.getLocalMemoryRequirements() != null) {
            return false;
        }
        computeLocalMemoryRequirements(op, createMemoryRequirementsConfigurator(context));
        return true;
    }

    private void computeLocalMemoryRequirements(AbstractLogicalOperator op,
            ILogicalOperatorVisitor<Void, Void> memoryRequirementsVisitor) throws AlgebricksException {
        IPhysicalOperator physOp = op.getPhysicalOperator();
        if (physOp.getLocalMemoryRequirements() == null) {
            physOp.createLocalMemoryRequirements(op);
            if (physOp.getLocalMemoryRequirements() == null) {
                throw new IllegalStateException(physOp.getOperatorTag().toString());
            }
            if (memoryRequirementsVisitor != null) {
                op.accept(memoryRequirementsVisitor, null);
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                for (Mutable<ILogicalOperator> root : p.getRoots()) {
                    computeLocalMemoryRequirements((AbstractLogicalOperator) root.getValue(),
                            memoryRequirementsVisitor);
                }
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            computeLocalMemoryRequirements((AbstractLogicalOperator) opRef.getValue(), memoryRequirementsVisitor);
        }
    }

    protected ILogicalOperatorVisitor<Void, Void> createMemoryRequirementsConfigurator(IOptimizationContext context) {
        return new MemoryRequirementsConfigurator(context);
    }

    protected static class MemoryRequirementsConfigurator implements ILogicalOperatorVisitor<Void, Void> {

        protected final IOptimizationContext context;

        protected final PhysicalOptimizationConfig physConfig;

        protected MemoryRequirementsConfigurator(IOptimizationContext context) {
            this.context = context;
            this.physConfig = context.getPhysicalOptimizationConfig();
        }

        // helper methods

        protected void setOperatorMemoryBudget(AbstractLogicalOperator op, int memBudgetInFrames)
                throws AlgebricksException {
            LocalMemoryRequirements memoryReqs = op.getPhysicalOperator().getLocalMemoryRequirements();
            int minBudgetInFrames = memoryReqs.getMinMemoryBudgetInFrames();
            if (memBudgetInFrames < minBudgetInFrames) {
                throw AlgebricksException.create(ErrorCode.ILLEGAL_MEMORY_BUDGET, op.getSourceLocation(),
                        op.getOperatorTag().toString(), memBudgetInFrames * physConfig.getFrameSize(),
                        minBudgetInFrames * physConfig.getFrameSize());
            }
            memoryReqs.setMemoryBudgetInFrames(memBudgetInFrames);
        }

        // variable memory operators

        @Override
        public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
            setOperatorMemoryBudget(op, physConfig.getMaxFramesExternalSort());
            return null;
        }

        @Override
        public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
            setOperatorMemoryBudget(op, physConfig.getMaxFramesForGroupBy());
            return null;
        }

        @Override
        public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
            if (op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.WINDOW) {
                setOperatorMemoryBudget(op, physConfig.getMaxFramesForWindow());
            }
            return null;
        }

        @Override
        public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
            return visitJoinOperator(op, arg);
        }

        @Override
        public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
            return visitJoinOperator(op, arg);
        }

        protected Void visitJoinOperator(AbstractBinaryJoinOperator op, Void arg) throws AlgebricksException {
            setOperatorMemoryBudget(op, physConfig.getMaxFramesForJoin());
            return null;
        }

        @Override
        public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
            return visitAbstractUnnestMapOperator(op, arg);
        }

        @Override
        public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
                throws AlgebricksException {
            return visitAbstractUnnestMapOperator(op, arg);
        }

        protected Void visitAbstractUnnestMapOperator(AbstractUnnestMapOperator op, Void arg)
                throws AlgebricksException {
            IPhysicalOperator physOp = op.getPhysicalOperator();
            if (physOp.getOperatorTag() == PhysicalOperatorTag.LENGTH_PARTITIONED_INVERTED_INDEX_SEARCH
                    || physOp.getOperatorTag() == PhysicalOperatorTag.SINGLE_PARTITION_INVERTED_INDEX_SEARCH) {
                setOperatorMemoryBudget(op, physConfig.getMaxFramesForTextSearch());
            }
            return null;
        }

        // fixed memory operators

        @Override
        public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
            return null;
        }
    }
}