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
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.WindowPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
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

        computeDefaultPhysicalOp(op, true, context);
        context.addToDontApplySet(this, op);
        return true;
    }

    private static void setPhysicalOperators(ILogicalPlan plan, boolean topLevelOp, IOptimizationContext context)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) root.getValue(), topLevelOp, context);
        }
    }

    private static void computeDefaultPhysicalOp(AbstractLogicalOperator op, boolean topLevelOp,
            IOptimizationContext context) throws AlgebricksException {
        PhysicalOptimizationConfig physicalOptimizationConfig = context.getPhysicalOptimizationConfig();
        if (op.getOperatorTag().equals(LogicalOperatorTag.GROUP)) {
            GroupByOperator gby = (GroupByOperator) op;
            if (gby.getNestedPlans().size() == 1) {
                ILogicalPlan p0 = gby.getNestedPlans().get(0);
                if (p0.getRoots().size() == 1) {
                    Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
                    if (r0.getValue().getOperatorTag().equals(LogicalOperatorTag.AGGREGATE)) {
                        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
                        boolean serializable = true;
                        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
                            AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) exprRef.getValue();
                            if (!BuiltinFunctions.isAggregateFunctionSerializable(expr.getFunctionIdentifier())) {
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
                                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory =
                                        context.getMergeAggregationExpressionFactory();
                                List<LogicalVariable> originalVariables = aggOp.getVariables();
                                List<Mutable<ILogicalExpression>> aggExprs = aggOp.getExpressions();
                                int aggNum = aggExprs.size();
                                for (int i = 0; i < aggNum; i++) {
                                    AbstractFunctionCallExpression expr =
                                            (AbstractFunctionCallExpression) aggExprs.get(i).getValue();
                                    AggregateFunctionCallExpression serialAggExpr =
                                            BuiltinFunctions.makeSerializableAggregateFunctionExpression(
                                                    expr.getFunctionIdentifier(), expr.getArguments());
                                    serialAggExpr.setSourceLocation(expr.getSourceLocation());
                                    if (mergeAggregationExpressionFactory.createMergeAggregation(
                                            originalVariables.get(i), serialAggExpr, context) == null) {
                                        hasIntermediateAgg = false;
                                        break;
                                    }
                                }

                                // Check whether there are multiple aggregates in the sub plan.
                                // Currently, we don't support multiple aggregates in one external group-by.
                                boolean multipleAggOpsFound = false;
                                ILogicalOperator r1Logical = aggOp;
                                while (r1Logical.hasInputs()) {
                                    r1Logical = r1Logical.getInputs().get(0).getValue();
                                    if (r1Logical.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                                        multipleAggOpsFound = true;
                                        break;
                                    }
                                }

                                if (hasIntermediateAgg && !multipleAggOpsFound) {
                                    for (int i = 0; i < aggNum; i++) {
                                        AbstractFunctionCallExpression expr =
                                                (AbstractFunctionCallExpression) aggExprs.get(i).getValue();
                                        AggregateFunctionCallExpression serialAggExpr =
                                                BuiltinFunctions.makeSerializableAggregateFunctionExpression(
                                                        expr.getFunctionIdentifier(), expr.getArguments());
                                        serialAggExpr.setSourceLocation(expr.getSourceLocation());
                                        aggOp.getExpressions().get(i).setValue(serialAggExpr);
                                    }
                                    ExternalGroupByPOperator externalGby = new ExternalGroupByPOperator(
                                            gby.getGroupByList(), physicalOptimizationConfig.getMaxFramesForGroupBy(),
                                            (long) physicalOptimizationConfig.getMaxFramesForGroupBy()
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
                                op.setPhysicalOperator(new PreclusteredGroupByPOperator(columnList, gby.isGroupAll(),
                                        context.getPhysicalOptimizationConfig().getMaxFramesForGroupBy()));
                            }
                        }
                    } else if (r0.getValue().getOperatorTag().equals(LogicalOperatorTag.RUNNINGAGGREGATE)) {
                        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = gby.getGroupByList();
                        List<LogicalVariable> columnList = new ArrayList<LogicalVariable>(gbyList.size());
                        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
                            ILogicalExpression expr = p.second.getValue();
                            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                                columnList.add(varRef.getVariableReference());
                            }
                        }
                        op.setPhysicalOperator(new PreclusteredGroupByPOperator(columnList, gby.isGroupAll(),
                                context.getPhysicalOptimizationConfig().getMaxFramesForGroupBy()));
                    } else {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, gby.getSourceLocation(),
                                "Unsupported nested operator within a group-by: "
                                        + r0.getValue().getOperatorTag().name());
                    }
                }
            }
        }
        if (op.getPhysicalOperator() == null) {
            switch (op.getOperatorTag()) {
                case INNERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((InnerJoinOperator) op, topLevelOp, context);
                    break;
                }
                case LEFTOUTERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((LeftOuterJoinOperator) op, topLevelOp, context);
                    break;
                }
                case UNNEST_MAP:
                case LEFT_OUTER_UNNEST_MAP: {
                    ILogicalExpression unnestExpr = null;
                    unnestExpr = ((AbstractUnnestMapOperator) op).getExpressionRef().getValue();
                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        FunctionIdentifier fid = f.getFunctionIdentifier();
                        if (!fid.equals(BuiltinFunctions.INDEX_SEARCH)) {
                            throw new IllegalStateException();
                        }
                        AccessMethodJobGenParams jobGenParams = new AccessMethodJobGenParams();
                        jobGenParams.readFromFuncArgs(f.getArguments());
                        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
                        DataSourceId dataSourceId =
                                new DataSourceId(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
                        Dataset dataset =
                                mp.findDataset(jobGenParams.getDataverseName(), jobGenParams.getDatasetName());
                        IDataSourceIndex<String, DataSourceId> dsi =
                                mp.findDataSourceIndex(jobGenParams.getIndexName(), dataSourceId);
                        INodeDomain storageDomain = mp.findNodeDomain(dataset.getNodeGroupName());
                        if (dsi == null) {
                            throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                                    "Could not find index " + jobGenParams.getIndexName() + " for dataset "
                                            + dataSourceId);
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
                case WINDOW: {
                    WindowOperator winOp = (WindowOperator) op;
                    WindowPOperator physOp = createWindowPOperator(winOp);
                    op.setPhysicalOperator(physOp);
                    break;
                }
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                setPhysicalOperators(p, false, context);
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) opRef.getValue(), topLevelOp, context);
        }
    }

    private static void generateMergeAggregationExpressions(GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
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
        List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < n; i++) {
            ILogicalExpression aggFuncExpr = aggFuncRefs.get(i).getValue();
            ILogicalExpression mergeExpr = mergeAggregationExpressionFactory
                    .createMergeAggregation(aggProducedVars.get(i), aggFuncExpr, context);
            if (mergeExpr == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, aggFuncExpr.getSourceLocation(),
                        "The aggregation function " + aggFuncExpr
                                + " does not have a registered intermediate aggregation function.");
            }
            mergeExpressionRefs.add(new MutableObject<ILogicalExpression>(mergeExpr));
        }
        aggOp.setMergeExpressions(mergeExpressionRefs);
    }

    private static WindowPOperator createWindowPOperator(WindowOperator winOp) throws CompilationException {
        List<Mutable<ILogicalExpression>> partitionExprs = winOp.getPartitionExpressions();
        List<LogicalVariable> partitionColumns = new ArrayList<>(partitionExprs.size());
        for (Mutable<ILogicalExpression> pe : partitionExprs) {
            ILogicalExpression partExpr = pe.getValue();
            if (partExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, winOp.getSourceLocation(),
                        "Window partition/order expression has not been normalized");
            }
            LogicalVariable var = ((VariableReferenceExpression) partExpr).getVariableReference();
            partitionColumns.add(var);
        }
        List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExprs = winOp.getOrderExpressions();
        List<OrderColumn> orderColumns = new ArrayList<>(orderExprs.size());
        for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExprs) {
            ILogicalExpression orderExpr = p.second.getValue();
            if (orderExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, winOp.getSourceLocation(),
                        "Window partition/order expression has not been normalized");
            }
            LogicalVariable var = ((VariableReferenceExpression) orderExpr).getVariableReference();
            orderColumns.add(new OrderColumn(var, p.first.getKind()));
        }
        boolean partitionMaterialization = winOp.hasNestedPlans();
        if (!partitionMaterialization) {
            for (Mutable<ILogicalExpression> exprRef : winOp.getExpressions()) {
                ILogicalExpression expr = exprRef.getValue();
                if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, winOp.getSourceLocation(),
                            expr.getExpressionTag());
                }
                AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
                if (BuiltinFunctions.windowFunctionHasProperty(callExpr.getFunctionIdentifier(),
                        BuiltinFunctions.WindowFunctionProperty.MATERIALIZE_PARTITION)) {
                    partitionMaterialization = true;
                    break;
                }
            }
        }

        return new WindowPOperator(partitionColumns, partitionMaterialization, orderColumns);
    }
}
