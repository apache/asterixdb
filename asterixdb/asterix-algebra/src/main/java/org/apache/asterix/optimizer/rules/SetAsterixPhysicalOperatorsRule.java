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
package org.apache.asterix.optimizer.rules;

import static org.apache.asterix.common.utils.IdentifierUtil.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.algebra.operators.physical.AssignBatchPOperator;
import org.apache.asterix.algebra.operators.physical.BTreeSearchPOperator;
import org.apache.asterix.algebra.operators.physical.InvertedIndexPOperator;
import org.apache.asterix.algebra.operators.physical.RTreeSearchPOperator;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.asterix.optimizer.cost.CostMethods;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.asterix.optimizer.cost.ICostMethods;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
import org.apache.asterix.optimizer.rules.util.AsterixJoinUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractWindowPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowStreamPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;

public class SetAsterixPhysicalOperatorsRule extends SetAlgebricksPhysicalOperatorsRule {

    // Disable ASSIGN_BATCH physical operator if this option is set to 'false'
    public static final String REWRITE_ATTEMPT_BATCH_ASSIGN = "rewrite_attempt_batch_assign";
    static final boolean REWRITE_ATTEMPT_BATCH_ASSIGN_DEFAULT = true;

    private final CostMethodsFactory costMethodsFactory;

    @Override
    protected ILogicalOperatorVisitor<IPhysicalOperator, Boolean> createPhysicalOperatorFactoryVisitor(
            IOptimizationContext context) {
        return new AsterixPhysicalOperatorFactoryVisitor(context, costMethodsFactory.createCostMethods(context));
    }

    @FunctionalInterface
    public interface CostMethodsFactory {
        CostMethods createCostMethods(IOptimizationContext ctx);
    }

    public SetAsterixPhysicalOperatorsRule(CostMethodsFactory costMethodsFactory) {
        this.costMethodsFactory = costMethodsFactory;
    }

    static boolean isBatchAssignEnabled(IOptimizationContext context) {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        return metadataProvider.getBooleanProperty(REWRITE_ATTEMPT_BATCH_ASSIGN, REWRITE_ATTEMPT_BATCH_ASSIGN_DEFAULT);
    }

    protected static class AsterixPhysicalOperatorFactoryVisitor extends AlgebricksPhysicalOperatorFactoryVisitor {

        private final boolean isBatchAssignEnabled;
        protected ICostMethods costMethods;
        protected boolean cboMode;
        protected boolean cboTestMode;

        protected AsterixPhysicalOperatorFactoryVisitor(IOptimizationContext context, ICostMethods cm) {
            super(context);
            costMethods = cm;
            isBatchAssignEnabled = isBatchAssignEnabled(context);
            cboMode = physConfig.getCBOMode();
            cboTestMode = physConfig.getCBOTestMode();
        }

        protected Enum groupByAlgorithm(GroupByOperator gby, Boolean topLevelOp) {
            boolean hashGroupPossible = hashGroupPossible(gby, topLevelOp);
            boolean hashGroupHint = hashGroupHint(gby);

            if (hashGroupPossible && hashGroupHint) {
                return GroupByAlgorithm.HASH_GROUP_BY;
            }

            if (!(cboMode || cboTestMode)) {
                return GroupByAlgorithm.SORT_GROUP_BY;
            }

            Map<String, Object> annotations = gby.getAnnotations();
            if (annotations != null && annotations.containsKey(OperatorAnnotations.OP_INPUT_CARDINALITY)
                    && annotations.containsKey(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                // We should make costing decisions between hash and sort group by
                // only if the input and output cardinalities of the group by operator
                // were computed earlier during CBO. Otherwise, default to sort group by.
                ICost costHashGroupBy = costMethods.costHashGroupBy(gby);
                ICost costSortGroupBy = costMethods.costSortGroupBy(gby);

                if (hashGroupPossible && costHashGroupBy.costLE(costSortGroupBy)) {
                    addAnnotations(gby, (double) Math.round(costHashGroupBy.computeTotalCost() * 100) / 100);
                    return GroupByAlgorithm.HASH_GROUP_BY;
                }

                addAnnotations(gby, (double) Math.round(costSortGroupBy.computeTotalCost() * 100) / 100);
                return GroupByAlgorithm.SORT_GROUP_BY;
            }

            addAnnotations(gby, 0.0);
            return GroupByAlgorithm.SORT_GROUP_BY;
        }

        private void addAnnotations(GroupByOperator gby, double cost) {
            gby.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL, cost);
        }

        public IPhysicalOperator visitDistinctOperator(DistinctOperator distinct, Boolean topLevelOp) {
            addAnnotations(distinct);
            return super.visitDistinctOperator(distinct, topLevelOp);
        }

        protected void addAnnotations(DistinctOperator distinct) {
            ICost costDistinct = costMethods.costDistinct(distinct);
            distinct.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL,
                    (double) Math.round(costDistinct.computeTotalCost() * 100) / 100);
        }

        public IPhysicalOperator visitOrderOperator(OrderOperator oo, Boolean topLevelOp) throws AlgebricksException {
            addAnnotations(oo);
            return super.visitOrderOperator(oo, topLevelOp);
        }

        protected void addAnnotations(OrderOperator order) {
            ICost costOrder = costMethods.costOrderBy(order);
            order.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL,
                    (double) Math.round(costOrder.computeTotalCost() * 100) / 100);
        }

        @Override
        public IPhysicalOperator visitAssignOperator(AssignOperator op, Boolean topLevelOp) throws AlgebricksException {
            List<Mutable<ILogicalExpression>> exprList = op.getExpressions();
            boolean batchMode = isBatchAssignEnabled && exprList.size() > 0 && allBatchableFunctionCalls(exprList);
            if (batchMode) {
                // disable inlining of variable arguments
                for (Mutable<ILogicalExpression> exprRef : exprList) {
                    AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) exprRef.getValue();
                    for (Mutable<ILogicalExpression> argRef : callExpr.getArguments()) {
                        LogicalVariable var = ((VariableReferenceExpression) argRef.getValue()).getVariableReference();
                        context.addNotToBeInlinedVar(var);
                    }
                }
                return new AssignBatchPOperator();
            } else {
                return super.visitAssignOperator(op, topLevelOp);
            }
        }

        @Override
        public IPhysicalOperator visitInnerJoinOperator(InnerJoinOperator op, Boolean topLevelOp)
                throws AlgebricksException {
            AsterixJoinUtils.setJoinAlgorithmAndExchangeAlgo(op, topLevelOp, context);
            if (op.getPhysicalOperator() != null) {
                return op.getPhysicalOperator();
            }
            return super.visitInnerJoinOperator(op, topLevelOp);
        }

        @Override
        public IPhysicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Boolean topLevelOp)
                throws AlgebricksException {
            AsterixJoinUtils.setJoinAlgorithmAndExchangeAlgo(op, topLevelOp, context);
            if (op.getPhysicalOperator() != null) {
                return op.getPhysicalOperator();
            }
            return super.visitLeftOuterJoinOperator(op, topLevelOp);
        }

        @Override
        public ExternalGroupByPOperator createExternalGroupByPOperator(GroupByOperator gby) throws AlgebricksException {
            Mutable<ILogicalOperator> r0 = gby.getNestedPlans().get(0).getRoots().get(0);
            if (!r0.getValue().getOperatorTag().equals(LogicalOperatorTag.AGGREGATE)) {
                return null;
            }
            AggregateOperator aggOp = (AggregateOperator) r0.getValue();
            boolean serializable = aggOp.getExpressions().stream()
                    .allMatch(exprRef -> exprRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                            && BuiltinFunctions.isAggregateFunctionSerializable(
                                    ((AbstractFunctionCallExpression) exprRef.getValue()).getFunctionIdentifier()));
            if (!serializable) {
                return null;
            }

            // if serializable, use external group-by
            // now check whether the serialized version aggregation function has corresponding intermediate agg
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory =
                    context.getMergeAggregationExpressionFactory();
            List<LogicalVariable> originalVariables = aggOp.getVariables();
            List<Mutable<ILogicalExpression>> aggExprs = aggOp.getExpressions();
            int aggNum = aggExprs.size();
            for (int i = 0; i < aggNum; i++) {
                AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) aggExprs.get(i).getValue();
                AggregateFunctionCallExpression serialAggExpr = BuiltinFunctions
                        .makeSerializableAggregateFunctionExpression(expr.getFunctionIdentifier(), expr.getArguments());
                serialAggExpr.setSourceLocation(expr.getSourceLocation());
                if (mergeAggregationExpressionFactory.createMergeAggregation(originalVariables.get(i), serialAggExpr,
                        context) == null) {
                    return null;
                }
            }

            // Check whether there are multiple aggregates in the sub plan.
            // Currently, we don't support multiple aggregates in one external group-by.
            ILogicalOperator r1Logical = aggOp;
            while (r1Logical.hasInputs()) {
                r1Logical = r1Logical.getInputs().get(0).getValue();
                if (r1Logical.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                    return null;
                }
            }

            for (int i = 0; i < aggNum; i++) {
                AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) aggExprs.get(i).getValue();
                AggregateFunctionCallExpression serialAggExpr = BuiltinFunctions
                        .makeSerializableAggregateFunctionExpression(expr.getFunctionIdentifier(), expr.getArguments());
                serialAggExpr.setSourceLocation(expr.getSourceLocation());
                aggOp.getExpressions().get(i).setValue(serialAggExpr);
            }

            generateMergeAggregationExpressions(gby);
            return new ExternalGroupByPOperator(gby.getGroupByVarList());
        }

        private void generateMergeAggregationExpressions(GroupByOperator gby) throws AlgebricksException {
            if (gby.getNestedPlans().size() != 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, gby.getSourceLocation(),
                        "External group-by currently works only for one nested plan with one root containing"
                                + "an aggregate and a nested-tuple-source.");
            }
            ILogicalPlan p0 = gby.getNestedPlans().get(0);
            if (p0.getRoots().size() != 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, gby.getSourceLocation(),
                        "External group-by currently works only for one nested plan with one root containing"
                                + "an aggregate and a nested-tuple-source.");
            }
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory =
                    context.getMergeAggregationExpressionFactory();
            Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
            AbstractLogicalOperator r0Logical = (AbstractLogicalOperator) r0.getValue();
            if (r0Logical.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, gby.getSourceLocation(),
                        "The merge aggregation expression generation should not process a " + r0Logical.getOperatorTag()
                                + " operator.");
            }
            AggregateOperator aggOp = (AggregateOperator) r0.getValue();
            List<Mutable<ILogicalExpression>> aggFuncRefs = aggOp.getExpressions();
            List<LogicalVariable> aggProducedVars = aggOp.getVariables();
            int n = aggOp.getExpressions().size();
            List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                ILogicalExpression aggFuncExpr = aggFuncRefs.get(i).getValue();
                ILogicalExpression mergeExpr = mergeAggregationExpressionFactory
                        .createMergeAggregation(aggProducedVars.get(i), aggFuncExpr, context);
                if (mergeExpr == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, aggFuncExpr.getSourceLocation(),
                            "The aggregation function "
                                    + ((AbstractFunctionCallExpression) aggFuncExpr).getFunctionIdentifier().getName()
                                    + " does not have a registered intermediate aggregation function.");
                }
                mergeExpressionRefs.add(new MutableObject<>(mergeExpr));
            }
            aggOp.setMergeExpressions(mergeExpressionRefs);
        }

        @Override
        public IPhysicalOperator visitUnnestMapOperator(UnnestMapOperator op, Boolean topLevelOp)
                throws AlgebricksException {
            return visitAbstractUnnestMapOperator(op);
        }

        @Override
        public IPhysicalOperator visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Boolean topLevelOp)
                throws AlgebricksException {
            return visitAbstractUnnestMapOperator(op);
        }

        private IPhysicalOperator visitAbstractUnnestMapOperator(AbstractUnnestMapOperator op)
                throws AlgebricksException {
            ILogicalExpression unnestExpr = op.getExpressionRef().getValue();
            if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, op.getSourceLocation());
            }
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
            if (!f.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, op.getSourceLocation());
            }
            AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
            jobGenParams.readFromFuncArgs(f.getArguments());
            MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
            String database = jobGenParams.getDatabaseName();
            DataSourceId dataSourceId =
                    new DataSourceId(database, jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
            Dataset dataset = mp.findDataset(database, jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
            IDataSourceIndex<String, DataSourceId> dsi =
                    mp.findDataSourceIndex(jobGenParams.getIndexName(), dataSourceId);
            INodeDomain storageDomain = mp.findNodeDomain(dataset.getNodeGroupName());
            if (dsi == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                        "Could not find index " + jobGenParams.getIndexName() + " for " + dataset() + " "
                                + dataSourceId);
            }
            IndexType indexType = jobGenParams.getIndexType();
            boolean requiresBroadcast = jobGenParams.getRequiresBroadcast();
            switch (indexType) {
                case BTREE: {
                    BTreeJobGenParams btreeJobGenParams = new BTreeJobGenParams();
                    btreeJobGenParams.readFromFuncArgs(f.getArguments());
                    return new BTreeSearchPOperator(dsi, storageDomain, requiresBroadcast,
                            btreeJobGenParams.isPrimaryIndex(), btreeJobGenParams.isEqCondition(),
                            btreeJobGenParams.getLowKeyVarList(), btreeJobGenParams.getHighKeyVarList());
                }
                case RTREE: {
                    return new RTreeSearchPOperator(dsi, storageDomain, requiresBroadcast);
                }
                case SINGLE_PARTITION_WORD_INVIX:
                case SINGLE_PARTITION_NGRAM_INVIX: {
                    return new InvertedIndexPOperator(dsi, storageDomain, requiresBroadcast, false);
                }
                case LENGTH_PARTITIONED_WORD_INVIX:
                case LENGTH_PARTITIONED_NGRAM_INVIX: {
                    return new InvertedIndexPOperator(dsi, storageDomain, requiresBroadcast, true);
                }
                default: {
                    throw AlgebricksException.create(
                            org.apache.hyracks.api.exceptions.ErrorCode.OPERATOR_NOT_IMPLEMENTED,
                            op.getSourceLocation(), op.getOperatorTag().toString() + " with " + indexType + " index");
                }
            }
        }

        @Override
        public AbstractWindowPOperator createWindowPOperator(WindowOperator winOp) throws AlgebricksException {
            if (winOp.hasNestedPlans()) {
                boolean frameStartIsMonotonic = AnalysisUtil.isWindowFrameBoundaryMonotonic(
                        winOp.getFrameStartExpressions(), winOp.getFrameValueExpressions());
                boolean frameEndIsMonotonic = AnalysisUtil.isWindowFrameBoundaryMonotonic(
                        winOp.getFrameEndExpressions(), winOp.getFrameValueExpressions());
                boolean nestedTrivialAggregates =
                        winOp.getNestedPlans().stream().allMatch(AnalysisUtil::isTrivialAggregateSubplan);
                return new WindowPOperator(winOp.getPartitionVarList(), winOp.getOrderColumnList(),
                        frameStartIsMonotonic, frameEndIsMonotonic, nestedTrivialAggregates);
            } else if (AnalysisUtil.hasFunctionWithProperty(winOp,
                    BuiltinFunctions.WindowFunctionProperty.MATERIALIZE_PARTITION)) {
                return new WindowPOperator(winOp.getPartitionVarList(), winOp.getOrderColumnList(), false, false,
                        false);
            } else {
                return new WindowStreamPOperator(winOp.getPartitionVarList(), winOp.getOrderColumnList());
            }
        }

        private boolean allBatchableFunctionCalls(List<Mutable<ILogicalExpression>> exprList)
                throws CompilationException {
            for (Mutable<ILogicalExpression> exprRef : exprList) {
                if (!isBatchableFunctionCall(exprRef.getValue())) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isBatchableFunctionCall(ILogicalExpression expr) throws CompilationException {
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }
            AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
            if (!ExternalFunctionCompilerUtil.supportsBatchInvocation(callExpr.getKind(), callExpr.getFunctionInfo())) {
                return false;
            }
            for (Mutable<ILogicalExpression> argRef : callExpr.getArguments()) {
                if (argRef.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    return false;
                }
            }
            return true;
        }
    }
}