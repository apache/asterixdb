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
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.rewriter.util.JoinUtils;

public class SetAsterixPhysicalOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        computeDefaultPhysicalOp(op, context);
        context.addToDontApplySet(this, op);
        return true;
    }

    private static void setPhysicalOperators(ILogicalPlan plan, IOptimizationContext context)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) root.getValue(), context);
        }
    }

    private static void computeDefaultPhysicalOp(AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        PhysicalOptimizationConfig physicalOptimizationConfig = context.getPhysicalOptimizationConfig();
        if (op.getOperatorTag().equals(LogicalOperatorTag.GROUP)) {
            GroupByOperator gby = (GroupByOperator) op;
            if (gby.getNestedPlans().size() == 1) {
                ILogicalPlan p0 = gby.getNestedPlans().get(0);
                if (p0.getRoots().size() == 1) {
                    Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
                    if (((AbstractLogicalOperator) (r0.getValue())).getOperatorTag()
                            .equals(LogicalOperatorTag.AGGREGATE)) {
                        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
                        boolean serializable = true;
                        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
                            AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) exprRef.getValue();
                            if (!AsterixBuiltinFunctions
                                    .isAggregateFunctionSerializable(expr.getFunctionIdentifier())) {
                                serializable = false;
                                break;
                            }
                        }

                        if ((gby.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY) == Boolean.TRUE || gby
                                .getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY) == Boolean.TRUE)) {
                            boolean setToExternalGby = false;
                            if (serializable) {
                                // if serializable, use external group-by
                                // now check whether the serialized version aggregation function has corresponding intermediate agg
                                boolean hasIntermediateAgg = true;
                                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory = context
                                        .getMergeAggregationExpressionFactory();
                                List<LogicalVariable> originalVariables = aggOp.getVariables();
                                List<Mutable<ILogicalExpression>> aggExprs = aggOp.getExpressions();
                                int aggNum = aggExprs.size();
                                for (int i = 0; i < aggNum; i++) {
                                    AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) aggExprs
                                            .get(i).getValue();
                                    AggregateFunctionCallExpression serialAggExpr = AsterixBuiltinFunctions
                                            .makeSerializableAggregateFunctionExpression(expr.getFunctionIdentifier(),
                                                    expr.getArguments());
                                    if (mergeAggregationExpressionFactory.createMergeAggregation(
                                            originalVariables.get(i), serialAggExpr, context) == null) {
                                        hasIntermediateAgg = false;
                                        break;
                                    }
                                }

                                if (hasIntermediateAgg) {
                                    for (int i = 0; i < aggNum; i++) {
                                        AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) aggExprs
                                                .get(i).getValue();
                                        AggregateFunctionCallExpression serialAggExpr = AsterixBuiltinFunctions
                                                .makeSerializableAggregateFunctionExpression(
                                                        expr.getFunctionIdentifier(), expr.getArguments());
                                        aggOp.getExpressions().get(i).setValue(serialAggExpr);
                                    }
                                    ExternalGroupByPOperator externalGby = new ExternalGroupByPOperator(
                                            gby.getGroupByList(),
                                            physicalOptimizationConfig.getMaxFramesExternalGroupBy(),
                                            physicalOptimizationConfig.getExternalGroupByTableSize(),
                                            (long) physicalOptimizationConfig.getMaxFramesExternalGroupBy()
                                                    * physicalOptimizationConfig.getFrameSize());
                                    generateMergeAggregationExpressions(gby, context);
                                    op.setPhysicalOperator(externalGby);
                                    setToExternalGby = true;
                                }
                            }

                            if (!setToExternalGby) {
                                // if not serializable or no intermediate agg, use pre-clustered group-by
                                List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = gby.getGroupByList();
                                List<LogicalVariable> columnList = new ArrayList<LogicalVariable>(gbyList.size());
                                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
                                    ILogicalExpression expr = p.second.getValue();
                                    if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                        VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                                        columnList.add(varRef.getVariableReference());
                                    }
                                }
                                op.setPhysicalOperator(new PreclusteredGroupByPOperator(columnList));
                            }
                        }
                    } else if (((AbstractLogicalOperator) (r0.getValue())).getOperatorTag()
                            .equals(LogicalOperatorTag.RUNNINGAGGREGATE)) {
                        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = gby.getGroupByList();
                        List<LogicalVariable> columnList = new ArrayList<LogicalVariable>(gbyList.size());
                        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
                            ILogicalExpression expr = p.second.getValue();
                            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                                columnList.add(varRef.getVariableReference());
                            }
                        }
                        op.setPhysicalOperator(new PreclusteredGroupByPOperator(columnList));
                    } else {
                        throw new AlgebricksException("Unsupported nested operator within a group-by: "
                                + ((AbstractLogicalOperator) (r0.getValue())).getOperatorTag().name());
                    }
                }
            }
        }

        if (null != op.getPhysicalOperator()
                && op.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.NESTED_LOOP) {
            AbstractBinaryJoinOperator jop = (AbstractBinaryJoinOperator) op;
            List<LogicalVariable> sideLeft = new ArrayList<LogicalVariable>();
            List<LogicalVariable> sideRight = new ArrayList<LogicalVariable>();
            List<Pair<ILogicalExpression, ILogicalExpression>> bandRange = new ArrayList<Pair<ILogicalExpression, ILogicalExpression>>();
            List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
            List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
            switch (AnalysisUtil.getSortMergeJoinable(op, varsLeft, varsRight, sideLeft, sideRight, bandRange)) {
                case BAND: {
                    /*op.setPhysicalOperator(new BandSortMergeJoinPOperator(jop.getJoinKind(),
                            JoinPartitioningType.PAIRWISE, sideLeft, sideRight, bandRange.get(0),
                            bandRange.get(0).first, context.getPhysicalOptimizationConfig().getMaxFramesHybridHash(),
                            context.getPhysicalOptimizationConfig().getMaxFramesLeftInputHybridHash(), 40, context
                                    .getPhysicalOptimizationConfig().getMaxRecordsPerFrame(), context
                                    .getPhysicalOptimizationConfig().getFudgeFactor()));*/
                    break;
                }
                case THETA: {
                    break;
                }
                case METRIC: {
                    break;
                }
                case SKYLINE: {
                    break;
                }
                case NESTLOOP: {
                    break;
                }
                default: {
                    break;
                }
            }
        }

        if (op.getPhysicalOperator() == null) {
            switch (op.getOperatorTag()) {
                case INNERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((InnerJoinOperator) op, context);
                    break;
                }
                case LEFTOUTERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((LeftOuterJoinOperator) op, context);
                    break;
                }
                case UNNEST_MAP:
                case LEFT_OUTER_UNNEST_MAP: {
                    ILogicalExpression unnestExpr = null;
                    unnestExpr = ((AbstractUnnestMapOperator) op).getExpressionRef().getValue();
                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        FunctionIdentifier fid = f.getFunctionIdentifier();
                        if (!fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                            throw new IllegalStateException();
                        }
                        AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                        jobGenParams.readFromFuncArgs(f.getArguments());
                        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
                        AqlSourceId dataSourceId = new AqlSourceId(jobGenParams.getDataverseName(),
                                jobGenParams.getDatasetName());
                        Dataset dataset = mp.findDataset(jobGenParams.getDataverseName(),
                                jobGenParams.getDatasetName());
                        IDataSourceIndex<String, AqlSourceId> dsi = mp.findDataSourceIndex(jobGenParams.getIndexName(),
                                dataSourceId);
                        INodeDomain storageDomain = mp.findNodeDomain(dataset.getNodeGroupName());
                        if (dsi == null) {
                            throw new AlgebricksException("Could not find index " + jobGenParams.getIndexName()
                                    + " for dataset " + dataSourceId);
                        }
                        IndexType indexType = jobGenParams.getIndexType();
                        boolean requiresBroadcast = jobGenParams.getRequiresBroadcast();
                        switch (indexType) {
                            case BTREE: {
                                BTreeJobGenParams btreeJobGenParams = new BTreeJobGenParams();
                                btreeJobGenParams.readFromFuncArgs(f.getArguments());
                                op.setPhysicalOperator(new BTreeSearchPOperator(dsi, storageDomain, requiresBroadcast,
                                        btreeJobGenParams.isPrimaryIndex(), btreeJobGenParams.isEqCondition(),
                                        btreeJobGenParams.getLowKeyVarList(), btreeJobGenParams.getHighKeyVarList()));
                                break;
                            }
                            case RTREE: {
                                op.setPhysicalOperator(new RTreeSearchPOperator(dsi, storageDomain, requiresBroadcast));
                                break;
                            }
                            case SINGLE_PARTITION_WORD_INVIX:
                            case SINGLE_PARTITION_NGRAM_INVIX: {
                                op.setPhysicalOperator(
                                        new InvertedIndexPOperator(dsi, storageDomain, requiresBroadcast, false));
                                break;
                            }
                            case LENGTH_PARTITIONED_WORD_INVIX:
                            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                                op.setPhysicalOperator(
                                        new InvertedIndexPOperator(dsi, storageDomain, requiresBroadcast, true));
                                break;
                            }
                            default: {
                                throw new NotImplementedException(indexType + " indexes are not implemented.");
                            }
                        }
                    }
                    break;
                }
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                setPhysicalOperators(p, context);
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) opRef.getValue(), context);
        }
    }

    private static void generateMergeAggregationExpressions(GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        IMergeAggregationExpressionFactory mergeAggregationExpressionFactory = context
                .getMergeAggregationExpressionFactory();
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AbstractLogicalOperator r0Logical = (AbstractLogicalOperator) r0.getValue();
        if (r0Logical.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            throw new AlgebricksException("The merge aggregation expression generation should not process a "
                    + r0Logical.getOperatorTag() + " operator.");
        }
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
        List<Mutable<ILogicalExpression>> aggFuncRefs = aggOp.getExpressions();
        List<LogicalVariable> aggProducedVars = aggOp.getVariables();
        int n = aggOp.getExpressions().size();
        List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < n; i++) {
            ILogicalExpression mergeExpr = mergeAggregationExpressionFactory
                    .createMergeAggregation(aggProducedVars.get(i), aggFuncRefs.get(i).getValue(), context);
            if (mergeExpr == null) {
                throw new AlgebricksException("The aggregation function " + aggFuncRefs.get(i).getValue()
                        + " does not have a registered intermediate aggregation function.");
            }
            mergeExpressionRefs.add(new MutableObject<ILogicalExpression>(mergeExpr));
        }
        aggOp.setMergeExpressions(mergeExpressionRefs);
    }

}
