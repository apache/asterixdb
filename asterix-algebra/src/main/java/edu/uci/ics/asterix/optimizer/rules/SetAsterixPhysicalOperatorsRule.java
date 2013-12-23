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
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.operators.physical.BTreeSearchPOperator;
import edu.uci.ics.asterix.algebra.operators.physical.InvertedIndexPOperator;
import edu.uci.ics.asterix.algebra.operators.physical.RTreeSearchPOperator;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import edu.uci.ics.asterix.optimizer.rules.am.BTreeJobGenParams;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.rewriter.util.JoinUtils;

public class SetAsterixPhysicalOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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
                    if (((AbstractLogicalOperator) (r0.getValue())).getOperatorTag().equals(
                            LogicalOperatorTag.AGGREGATE)) {
                        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
                        boolean serializable = true;
                        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
                            AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) exprRef.getValue();
                            if (!AsterixBuiltinFunctions.isAggregateFunctionSerializable(expr.getFunctionIdentifier())) {
                                serializable = false;
                                break;
                            }
                        }

                        if ((gby.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY) == Boolean.TRUE || gby
                                .getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY) == Boolean.TRUE)) {
                            if (serializable) {
                                // if not serializable, use external group-by
                                int i = 0;
                                for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
                                    AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) exprRef
                                            .getValue();
                                    AggregateFunctionCallExpression serialAggExpr = AsterixBuiltinFunctions
                                            .makeSerializableAggregateFunctionExpression(expr.getFunctionIdentifier(),
                                                    expr.getArguments());
                                    aggOp.getExpressions().get(i).setValue(serialAggExpr);
                                    i++;
                                }

                                ExternalGroupByPOperator externalGby = new ExternalGroupByPOperator(
                                        gby.getGroupByList(), physicalOptimizationConfig.getMaxFramesExternalGroupBy(),
                                        physicalOptimizationConfig.getExternalGroupByTableSize());
                                op.setPhysicalOperator(externalGby);
                                generateMergeAggregationExpressions(gby, context);
                            } else {
                                // if not serializable, use pre-clustered group-by
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
                    } else if (((AbstractLogicalOperator) (r0.getValue())).getOperatorTag().equals(
                            LogicalOperatorTag.RUNNINGAGGREGATE)) {
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
                case UNNEST_MAP: {
                    UnnestMapOperator unnestMap = (UnnestMapOperator) op;
                    ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
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
                        IDataSourceIndex<String, AqlSourceId> dsi = mp.findDataSourceIndex(jobGenParams.getIndexName(),
                                dataSourceId);
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
                                op.setPhysicalOperator(new BTreeSearchPOperator(dsi, requiresBroadcast,
                                        btreeJobGenParams.isPrimaryIndex(), btreeJobGenParams.isEqCondition(),
                                        btreeJobGenParams.getLowKeyVarList(), btreeJobGenParams.getHighKeyVarList()));
                                break;
                            }
                            case RTREE: {
                                op.setPhysicalOperator(new RTreeSearchPOperator(dsi, requiresBroadcast));
                                break;
                            }
                            case SINGLE_PARTITION_WORD_INVIX:
                            case SINGLE_PARTITION_NGRAM_INVIX: {
                                op.setPhysicalOperator(new InvertedIndexPOperator(dsi, requiresBroadcast, false));
                                break;
                            }
                            case LENGTH_PARTITIONED_WORD_INVIX:
                            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                                op.setPhysicalOperator(new InvertedIndexPOperator(dsi, requiresBroadcast, true));
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
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
        List<Mutable<ILogicalExpression>> aggFuncRefs = aggOp.getExpressions();
        int n = aggOp.getExpressions().size();
        List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < n; i++) {
            ILogicalExpression mergeExpr = mergeAggregationExpressionFactory.createMergeAggregation(aggFuncRefs.get(i)
                    .getValue(), context);
            mergeExpressionRefs.add(new MutableObject<ILogicalExpression>(mergeExpr));
        }
        aggOp.setMergeExpressions(mergeExpressionRefs);
    }

}