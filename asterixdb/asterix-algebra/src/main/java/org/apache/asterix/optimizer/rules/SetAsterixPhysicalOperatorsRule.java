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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.operators.physical.BTreeSearchPOperator;
import org.apache.asterix.algebra.operators.physical.InvertedIndexPOperator;
import org.apache.asterix.algebra.operators.physical.RTreeSearchPOperator;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractWindowPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowStreamPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.rewriter.rules.SetAlgebricksPhysicalOperatorsRule;

public final class SetAsterixPhysicalOperatorsRule extends SetAlgebricksPhysicalOperatorsRule {

    @Override
    protected ILogicalOperatorVisitor<IPhysicalOperator, Boolean> createPhysicalOperatorFactoryVisitor(
            IOptimizationContext context) {
        return new AsterixPhysicalOperatorFactoryVisitor(context);
    }

    private static class AsterixPhysicalOperatorFactoryVisitor extends AlgebricksPhysicalOperatorFactoryVisitor {

        private AsterixPhysicalOperatorFactoryVisitor(IOptimizationContext context) {
            super(context);
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
            DataSourceId dataSourceId =
                    new DataSourceId(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
            Dataset dataset = mp.findDataset(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
            IDataSourceIndex<String, DataSourceId> dsi =
                    mp.findDataSourceIndex(jobGenParams.getIndexName(), dataSourceId);
            INodeDomain storageDomain = mp.findNodeDomain(dataset.getNodeGroupName());
            if (dsi == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                        "Could not find index " + jobGenParams.getIndexName() + " for dataset " + dataSourceId);
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
    }
}