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

package org.apache.asterix.optimizer.rules.cbo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.asterix.optimizer.rules.am.array.AbstractOperatorFromSubplanRewrite;
import org.apache.asterix.translator.ConstantHelper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinProductivityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Stats {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IOptimizationContext optCtx;
    private final JoinEnum joinEnum;

    private long totalCardFromSample;
    private double distinctCardFromSample;

    private final long MIN_TOTAL_SAMPLES = 1L;

    public void setTotalCardFromSample(long numSamples) {
        totalCardFromSample = numSamples;
    }

    public void setDistinctCardFromSample(double numDistinctSamples) {
        distinctCardFromSample = numDistinctSamples;
    }

    public Stats(IOptimizationContext context, JoinEnum joinE) {
        optCtx = context;
        joinEnum = joinE;
    }

    protected Index findSampleIndex(DataSourceScanOperator scanOp, IOptimizationContext context)
            throws AlgebricksException {
        DataSource ds = (DataSource) scanOp.getDataSource();
        if (ds.getDatasourceType() != DataSource.Type.INTERNAL_DATASET)
            return null;
        DataSourceId dsid = ds.getId();
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();
        return mdp.findSampleIndex(dsid.getDatabaseName(), dsid.getDataverseName(), dsid.getDatasourceName());
    }

    private double findJoinSelectivity(JoinProductivityAnnotation anno, AbstractFunctionCallExpression joinExpr)
            throws AlgebricksException {
        List<LogicalVariable> exprUsedVars = new ArrayList<>();
        joinExpr.getUsedVariables(exprUsedVars);
        if (exprUsedVars.size() != 2) {
            // Since there is a left and right dataset here, expecting only two variables.
            return 1.0;
        }
        int idx1, idx2;
        if (joinEnum.varLeafInputIds.containsKey(exprUsedVars.get(0))) {
            idx1 = joinEnum.varLeafInputIds.get(exprUsedVars.get(0));
        } else
            return 1.0;
        if (joinEnum.varLeafInputIds.containsKey(exprUsedVars.get(1))) {
            idx2 = joinEnum.varLeafInputIds.get(exprUsedVars.get(1));
        } else
            return 1.0;

        double card1 = joinEnum.getJnArray()[idx1].origCardinality;
        double card2 = joinEnum.getJnArray()[idx2].origCardinality;
        if (card1 == 0.0 || card2 == 0.0) // should not happen
        {
            return 1.0;
        }

        // join sel  = leftside * productivity/(card1 * card2);
        if (anno != null) {
            int leftIndex = joinEnum.findJoinNodeIndexByName(anno.getLeftSideDataSet());
            if (leftIndex != idx1 && leftIndex != idx2) {
                // should not happen
                IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(joinExpr.getSourceLocation(), ErrorCode.INAPPLICABLE_HINT,
                            "productivity", "Invalid collection name/alias: " + anno.getLeftSideDataSet()));
                }
                return 1.0;
            }
            double productivity = anno.getJoinProductivity();
            if (productivity <= 0) {
                IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(joinExpr.getSourceLocation(), ErrorCode.INAPPLICABLE_HINT,
                            "productivity",
                            "Productivity specified: " + productivity + ", has to be a decimal value greater than 0"));
                }
                return 1.0;
            }
            if (leftIndex == idx1) {
                return productivity / card2;
            } else {
                return productivity / card1;
            }
        } else {
            if (card1 < card2) {
                // we are assuming that the smaller side is the primary side and that the join is Pk-Fk join.
                return 1.0 / card1;
            }
            return 1.0 / card2;
        }
    }

    // The expression we get may not be a base condition. It could be comprised of ors and ands and nots. So have to
    //recursively find the overall selectivity.
    private double getSelectivityFromAnnotation(AbstractFunctionCallExpression afcExpr, boolean join,
            boolean singleDatasetPreds) throws AlgebricksException {
        double sel = 1.0;

        if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.OR)) {
            double orSel = 0.0;
            for (int i = 0; i < afcExpr.getArguments().size(); i++) {
                ILogicalExpression lexpr = afcExpr.getArguments().get(i).getValue();
                if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    sel = getSelectivityFromAnnotation(
                            (AbstractFunctionCallExpression) afcExpr.getArguments().get(i).getValue(), join,
                            singleDatasetPreds);
                    orSel = orSel + sel - orSel * sel;
                }
            }
            return orSel;
        } else if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
            double andSel = 1.0;
            for (int i = 0; i < afcExpr.getArguments().size(); i++) {
                ILogicalExpression lexpr = afcExpr.getArguments().get(i).getValue();
                if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    sel = getSelectivityFromAnnotation(
                            (AbstractFunctionCallExpression) afcExpr.getArguments().get(i).getValue(), join,
                            singleDatasetPreds);
                    andSel *= sel;
                }
            }
            return andSel;
        } else if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
            ILogicalExpression lexpr = afcExpr.getArguments().get(0).getValue();
            if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                sel = getSelectivityFromAnnotation(
                        (AbstractFunctionCallExpression) afcExpr.getArguments().get(0).getValue(), join,
                        singleDatasetPreds);
                // We want to return 1.0 and not 0.0 if there was no annotation
                return (sel == 1.0) ? 1.0 : 1.0 - sel;
            }
        }

        double s;
        PredicateCardinalityAnnotation pca = afcExpr.getAnnotation(PredicateCardinalityAnnotation.class);
        if (!join) {
            if (pca != null) {
                s = pca.getSelectivity();
                if (s <= 0 || s >= 1) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(afcExpr.getSourceLocation(), ErrorCode.INAPPLICABLE_HINT,
                                "selectivity", "Selectivity specified: " + s
                                        + ", has to be a decimal value greater than 0 and less than 1"));
                    }
                } else {
                    sel *= s;
                }
            } else if (singleDatasetPreds) {
                // Single dataset predicates inside join predicates will have a selectivity annotation.
                // If the annotation is not present, return -1, so we don't overwrite previously
                // computed selectivity.
                return -1;
            }
        } else {
            JoinProductivityAnnotation jpa = afcExpr.getAnnotation(JoinProductivityAnnotation.class);
            s = findJoinSelectivity(jpa, afcExpr);
            sel *= s;
        }

        List<LogicalVariable> usedVars = new ArrayList<>();
        usedVars.clear();
        afcExpr.getUsedVariables(usedVars);
        if (join && sel == 1.0 && usedVars.size() == 1) {
            // assume no selectivity was assigned
            joinEnum.singleDatasetPreds.add(afcExpr);
        }
        return sel;
    }

    protected double getSelectivityFromAnnotationMain(ILogicalExpression leExpr, boolean join,
            boolean singleDatasetPreds) throws AlgebricksException {
        double sel = 1.0;

        if (leExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) leExpr;
            sel = getSelectivityFromAnnotation(afcExpr, join, singleDatasetPreds);
        }

        return sel;
    }

    // The next two routines should be combined and made more general
    protected double getSelectivity(ILogicalOperator op, boolean join) throws AlgebricksException {
        double sel = 1.0; // safe to return 1 if there is no annotation

        if (op == null) {
            return sel;
        }

        // find all the selectOperators here.
        while (op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOper = (SelectOperator) op;
                sel *= getSelectivityFromAnnotationMain(selOper.getCondition().getValue(), join, false);
            }
            if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                sel *= getSelectivity((SubplanOperator) op);
            }
            op = op.getInputs().get(0).getValue();
        }
        return sel;
    }

    private double getSelectivity(SubplanOperator subplanOp) throws AlgebricksException {
        double sel = 1.0; // safe to return 1 if there is no annotation
        //ILogicalOperator op = subplanOp;
        ILogicalOperator op = subplanOp.getNestedPlans().get(0).getRoots().get(0).getValue();
        while (true) {
            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOper = (SelectOperator) op;
                sel *= getSelectivityFromAnnotationMain(selOper.getCondition().getValue(), false, false);
            }
            if (op.getInputs().size() > 0) {
                op = op.getInputs().get(0).getValue();
            } else {
                break;
            }
        }
        return sel;
    }

    private List<ILogicalOperator> countOps(ILogicalOperator op, LogicalOperatorTag tag) {
        List<ILogicalOperator> ops = new ArrayList<>();

        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(tag)) {
                ops.add(op);
            }
            op = op.getInputs().get(0).getValue();
        }
        return ops;
    }

    private AggregateOperator findAggOp(ILogicalOperator op, ILogicalExpression exp) throws AlgebricksException {
        /*private final */ ContainsExpressionVisitor visitor = new ContainsExpressionVisitor();
        SubplanOperator subOp;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                subOp = (SubplanOperator) op;
                ILogicalOperator nextOp = subOp.getNestedPlans().get(0).getRoots().get(0).getValue();
                if (nextOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE)
                    return (AggregateOperator) nextOp;
            }
            op = op.getInputs().get(0).getValue();
        }
        return null;
    }

    private SubplanOperator findSubplanWithExpr(ILogicalOperator op, ILogicalExpression exp)
            throws AlgebricksException {
        /*private final */ ContainsExpressionVisitor visitor = new ContainsExpressionVisitor();
        SubplanOperator subOp;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                subOp = (SubplanOperator) op;
                ILogicalOperator nextOp = subOp.getNestedPlans().get(0).getRoots().get(0).getValue();

                while (nextOp != null) {
                    visitor.setExpression(exp);
                    if (nextOp.acceptExpressionTransform(visitor)) {
                        return subOp;
                    }

                    if (nextOp.getInputs().isEmpty()) {
                        break;
                    }
                    nextOp = nextOp.getInputs().get(0).getValue();
                }
            }
            op = op.getInputs().get(0).getValue();
        }
        return null;
    }

    private List<ILogicalExpression> storeSubplanSelectsAndMakeThemTrue(ILogicalOperator op) {
        List<ILogicalExpression> selExprs = new ArrayList<>();
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                if (op.getInputs().get(0).getValue().getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                    SelectOperator selOp = (SelectOperator) op;
                    selExprs.add(selOp.getCondition().getValue());
                    selOp.getCondition().setValue(ConstantExpression.TRUE);
                }
            }
            op = op.getInputs().get(0).getValue();
        }
        return selExprs;
    }

    private void restoreAllSubplanSelects(ILogicalOperator op, List<ILogicalExpression> selExprs) {
        int i = 0;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                if (op.getInputs().get(0).getValue().getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                    SelectOperator selOp = (SelectOperator) op;
                    selOp.getCondition().setValue(selExprs.get(i));
                    i++;
                }
            }
            op = op.getInputs().get(0).getValue();
        }
    }

    // For the SubOp subplan, leave the selection condition the same but all other selects and subsplan selects should be marked true
    private List<ILogicalExpression> storeSubplanSelectsAndMakeThemTrue(ILogicalOperator op, SubplanOperator subOp) {
        List<ILogicalExpression> selExprs = new ArrayList<>();
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                ILogicalOperator op2 = op.getInputs().get(0).getValue();
                if (op2.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                    SubplanOperator subOp2 = (SubplanOperator) op2;
                    if (subOp2 != subOp) {
                        SelectOperator selOp = (SelectOperator) op;
                        selExprs.add(selOp.getCondition().getValue());
                        selOp.getCondition().setValue(ConstantExpression.TRUE);
                    } // else leave expression as is.
                } else { // a non subplan select
                    SelectOperator selOp = (SelectOperator) op;
                    selExprs.add(selOp.getCondition().getValue());
                    selOp.getCondition().setValue(ConstantExpression.TRUE);
                }
            }
            op = op.getInputs().get(0).getValue();
        }
        return selExprs;
    }

    private void restoreAllSubplanSelectConditions(ILogicalOperator op, List<ILogicalExpression> selExprs,
            SubplanOperator subOp) {
        int i = 0;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                ILogicalOperator op2 = op.getInputs().get(0).getValue();
                if (op2.getOperatorTag().equals(LogicalOperatorTag.SUBPLAN)) {
                    SubplanOperator subOp2 = (SubplanOperator) op2;
                    if (subOp2 != subOp) {
                        SelectOperator selOp = (SelectOperator) op;
                        selOp.getCondition().setValue(selExprs.get(i));
                        i++;
                    }
                } else { // a non subplan select
                    SelectOperator selOp = (SelectOperator) op;
                    selOp.getCondition().setValue(selExprs.get(i));
                }
            }
            op = op.getInputs().get(0).getValue();
        }
    }

    protected double findSelectivityForThisPredicate(SelectOperator selOp, AbstractFunctionCallExpression exp,
            boolean arrayIndex) throws AlgebricksException {
        // replace the SelOp.condition with the new exp and replace it at the end
        // The Selop here is the start of the leafInput.

        ILogicalOperator parent = joinEnum.findDataSourceScanOperatorParent(selOp);
        DataSourceScanOperator scanOp = (DataSourceScanOperator) parent.getInputs().get(0).getValue();

        if (scanOp == null) {
            return 1.0; // what happens to the cards and sizes then? this may happen in case of in lists
        }

        Index index = findSampleIndex(scanOp, optCtx);
        if (index == null) {
            return 1.0;
        }

        Index.SampleIndexDetails idxDetails = (Index.SampleIndexDetails) index.getIndexDetails();
        double origDatasetCard = idxDetails.getSourceCardinality();
        double sampleCard = Math.min(idxDetails.getSampleCardinalityTarget(), origDatasetCard);
        if (sampleCard == 0) {
            sampleCard = 1;
            IWarningCollector warningCollector = optCtx.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(scanOp.getSourceLocation(),
                        org.apache.asterix.common.exceptions.ErrorCode.SAMPLE_HAS_ZERO_ROWS));
            }
        }

        // replace the dataScanSourceOperator with the sampling source
        SampleDataSource sampledatasource = joinEnum.getSampleDataSource(scanOp);
        DataSourceScanOperator deepCopyofScan =
                (DataSourceScanOperator) OperatorManipulationUtil.bottomUpCopyOperators(scanOp);
        deepCopyofScan.setDataSource(sampledatasource);

        List<ILogicalOperator> subPlans = countOps(selOp, LogicalOperatorTag.SUBPLAN);
        int numSubplans = subPlans.size();
        List<List<IAObject>> result;

        // insert this in place of the scandatasourceOp.
        parent.getInputs().get(0).setValue(deepCopyofScan);
        if (numSubplans == 0) { // just switch the predicates; the simplest case. There should be no other case if subplans were canonical
            ILogicalExpression saveExprs = selOp.getCondition().getValue();
            selOp.getCondition().setValue(exp);
            result = runSamplingQuery(optCtx, selOp);
            selOp.getCondition().setValue(saveExprs);
        } else {
            List<ILogicalOperator> selOps = countOps(selOp, LogicalOperatorTag.SELECT);
            int numSelects = selOps.size();
            int nonSubplanSelects = numSelects - numSubplans;

            if (numSubplans == 1 && nonSubplanSelects == 0) {
                AggregateOperator aggOp = findAggOp(selOp, exp);
                if (aggOp.getExpressions().size() > 1) {
                    // ANY and EVERY IN query; for selectivity purposes, we need to transform this into a ANY IN query
                    SelectOperator newSelOp = (SelectOperator) OperatorManipulationUtil.bottomUpCopyOperators(selOp);
                    aggOp = findAggOp(newSelOp, exp);
                    ILogicalOperator input = aggOp.getInputs().get(0).getValue();
                    SelectOperator condition = (SelectOperator) OperatorManipulationUtil
                            .bottomUpCopyOperators(AbstractOperatorFromSubplanRewrite.getSelectFromPlan(aggOp));
                    //push this condition below aggOp.
                    aggOp.getInputs().get(0).setValue(condition);
                    condition.getInputs().get(0).setValue(input);
                    ILogicalExpression newExp2 = newSelOp.getCondition().getValue();
                    if (newExp2.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) newExp2;
                        afce.getArguments().get(1).setValue(ConstantExpression.TRUE);
                    }
                    result = runSamplingQuery(optCtx, newSelOp); // no need to switch anything
                } else
                    result = runSamplingQuery(optCtx, selOp);
            } else { // the painful part; have to find where exp that is passed in is coming from. >= 1 and >= 1 case
                // Assumption is that there is exaclty one select condition above each subplan.
                // This was ensured before this routine is called
                SubplanOperator subOp = findSubplanWithExpr(selOp, exp);
                if (subOp == null) { // the exp is not coming from a subplan
                    List<ILogicalExpression> selExprs;
                    selExprs = storeSubplanSelectsAndMakeThemTrue(selOp); // all these will be marked true and will be resorted later.
                    result = runSamplingQuery(optCtx, selOp);
                    restoreAllSubplanSelects(selOp, selExprs);
                } else { // found the matching subPlan oper. Only keep this predicate and make all others true and then restore them.
                    List<ILogicalExpression> selExprs;
                    selExprs = storeSubplanSelectsAndMakeThemTrue(selOp, subOp); // all these will be marked true and will be resorted later.
                    result = runSamplingQuery(optCtx, selOp);
                    restoreAllSubplanSelectConditions(selOp, selExprs, subOp);
                }
            }
        }

        double predicateCardinality = findPredicateCardinality(result, false);
        predicateCardinality = Math.max(predicateCardinality, 0.0001);

        if (arrayIndex) {
            // In case of array predicates, the sample cardinality should be computed as
            // the number of unnested array elements. Run a second sampling query to compute this.
            // The query should already have the unnest operation, so simply replace the select clause with TRUE
            // to get the unnested cardinality from the sample.
            // Example query: SELECT count(*) as revenue
            //                FROM   orders o, o.o_orderline ol
            //                WHERE  ol.ol_delivery_d  >= '2016-01-01 00:00:00.000000'
            //                  AND  ol.ol_delivery_d < '2017-01-01 00:00:00.000000';
            // ol_delivery_d is part of the array o_orderline
            // To get the unnested cardinality,we run the following query on the sample:
            // SELECT count(*) as revenue
            // FROM   orders o, o.o_orderline ol
            // WHERE  TRUE;
            ILogicalExpression saveExprs = selOp.getCondition().getValue();
            selOp.getCondition().setValue(ConstantExpression.TRUE);
            result = runSamplingQuery(optCtx, selOp);
            selOp.getCondition().setValue(saveExprs);
            sampleCard = findPredicateCardinality(result, false);
        }
        // switch  the scanOp back
        parent.getInputs().get(0).setValue(scanOp);

        double sel = (double) predicateCardinality / sampleCard;
        return sel;
    }

    public double findPredicateCardinality(List<List<IAObject>> result, boolean project) {
        if (project) {
            ARecord record = (ARecord) (((IAObject) ((List<IAObject>) (result.get(0))).get(0)));
            int fields = record.numberOfFields();
            IAObject first = record.getValueByPos(0);
            return ((double) ((AInt64) first).getLongValue());
        } else {
            return (double) ((AInt64) result.get(0).get(0)).getLongValue();
        }
    }

    public int numberOfFields(List<List<IAObject>> result) {
        ARecord record = (ARecord) (((IAObject) ((List<IAObject>) (result.get(0))).get(0)));
        return record.numberOfFields();
    }

    public double findSizeVarsFromDisk(List<List<IAObject>> result, int numDiskVars) {
        ARecord record = (ARecord) (((IAObject) ((List<IAObject>) (result.get(0))).get(0)));
        // Now figure out the projected size
        double projectedSize = 0.0;
        int fields = record.numberOfFields();
        for (int j = 1; j <= numDiskVars; j++) {
            IAObject field = record.getValueByPos(j);
            double size = ((double) ((ADouble) field).getDoubleValue());
            projectedSize += size;
        }
        return projectedSize;
    }

    // Can have null returned, so this routine should only be called if at least tuple is returned by the sample
    public double findSizeVarsAfterScan(List<List<IAObject>> result, int numDiskVars) {
        ARecord record = (ARecord) (((IAObject) ((List<IAObject>) (result.get(0))).get(0)));
        // Now figure out the projected size
        double projectedSize = 0.0;
        int fields = record.numberOfFields();
        for (int j = 1 + numDiskVars; j < fields; j++) { // must skip the disk vars
            IAObject field = record.getValueByPos(j);
            double size = ((double) ((ADouble) field).getDoubleValue());
            projectedSize += size;
        }
        return projectedSize;
    }

    // This one only gets the cardinality
    protected List<List<IAObject>> runSamplingQuery(IOptimizationContext ctx, ILogicalOperator logOp)
            throws AlgebricksException {
        LOGGER.info("***running sample query***");

        IOptimizationContext newCtx = ctx.getOptimizationContextFactory().cloneOptimizationContext(ctx);

        ILogicalOperator newScanOp = OperatorManipulationUtil.bottomUpCopyOperators(logOp);

        List<Mutable<ILogicalExpression>> aggFunArgs = new ArrayList<>(1);
        aggFunArgs.add(new MutableObject<>(ConstantExpression.TRUE));
        BuiltinFunctionInfo countFn = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.COUNT);
        AggregateFunctionCallExpression aggExpr = new AggregateFunctionCallExpression(countFn, false, aggFunArgs);

        List<Mutable<ILogicalExpression>> aggExprList = new ArrayList<>(1);
        aggExprList.add(new MutableObject<>(aggExpr));

        List<LogicalVariable> aggVarList = new ArrayList<>(1);
        LogicalVariable aggVar = newCtx.newVar();
        aggVarList.add(aggVar);

        AggregateOperator newAggOp = new AggregateOperator(aggVarList, aggExprList);
        newAggOp.getInputs().add(new MutableObject<>(newScanOp));

        Mutable<ILogicalOperator> newAggOpRef = new MutableObject<>(newAggOp);

        OperatorPropertiesUtil.typeOpRec(newAggOpRef, newCtx);
        LOGGER.info("***returning from sample query***");

        String viewInPlan = new ALogicalPlanImpl(newAggOpRef).toString(); //useful when debugging
        LOGGER.trace("viewInPlan");
        LOGGER.trace(viewInPlan);
        return AnalysisUtil.runQuery(newAggOpRef, Arrays.asList(aggVar), newCtx, IRuleSetFactory.RuleSetKind.SAMPLING);
    }

    // This one gets the cardinality and also projection sizes
    protected List<List<IAObject>> runSamplingQueryProjection(IOptimizationContext ctx, ILogicalOperator logOp,
            int dataset, LogicalVariable primaryKey) throws AlgebricksException {
        LOGGER.info("***running projection sample query***");

        IOptimizationContext newCtx = ctx.getOptimizationContextFactory().cloneOptimizationContext(ctx);

        ILogicalOperator newScanOp = OperatorManipulationUtil.bottomUpCopyOperators(logOp);
        // Now we have to generate plans like this on top of the scanOp (logOp)
        // project ([$$79])
        // assign [$$79] <- [{"$1": $$73, "$2": $$74, "$3": $$75, "$4": $$76, "$5": $$77, "$6": $$78}]
        // aggregate [$$73, $$74, $$75, $$76, $$77, $$78] <- [agg-count(true), sql-avg($$68), sql-avg($$69), sql-avg($$70), sql-avg($$71), sql-avg($$72)]
        // assign [$$68, $$69, $$70, $$71, $$72] <- [serialized-size($$60), serialized-size($$str), serialized-size($$61), serialized-size($$65), serialized-size($$67)]

        // add the assign [$$56, ..., ] <- [encoded-size($$67), ..., ] on top of newAggOp
        List<LogicalVariable> vars1 = new ArrayList<>();
        VariableUtilities.getLiveVariables(logOp, vars1); // all the variables in the leafInput
        // Depending on the order here. Assuming the first three variables are from the data scan operator.
        if (!joinEnum.resultAndJoinVars.contains(primaryKey)) { // if the entire row is not being projected, we must remove $$p
            vars1.remove(primaryKey);
        }

        List<LogicalVariable> vars3 = // these variables can be thrown away as they are not present joins and in the final project
                new ArrayList<>(CollectionUtils.subtract(vars1, joinEnum.resultAndJoinVars /* vars2 */));
        List<LogicalVariable> vars4 = new ArrayList<>(CollectionUtils.subtract(vars1, vars3)); // variables that will flow up the tree

        List<LogicalVariable> vars = new ArrayList<>();
        vars.addAll(vars1);
        vars.addAll(vars4); // doing a union all; duplicates must not be removed
        //vars1 is what comes out of the disk
        joinEnum.jnArray[dataset].setNumVarsFromDisk(vars1.size());
        joinEnum.jnArray[dataset].setNumVarsAfterScan(vars4.size()); // Is this used? check.

        LogicalVariable newVar;
        // array to keep track of the assigns
        List<LogicalVariable> newVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<>();

        // create this assignOperator
        // assign [$$68, $$69, $$70, $$71, $$72] <- [serialized-size($$60), serialized-size($$str), serialized-size($$61), serialized-size($$65), serialized-size($$67)]
        int count = 0;
        for (LogicalVariable lv : vars) {
            count++;
            VariableReferenceExpression varRefExpr = new VariableReferenceExpression(lv);
            List<Mutable<ILogicalExpression>> vars2 = new ArrayList<>();
            vars2.add(new MutableObject<>(varRefExpr));
            ScalarFunctionCallExpression func = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.SERIALIZED_SIZE), vars2);
            exprs.add(new MutableObject<>(func));
            newVar = newCtx.newVar();
            newVars.add(newVar);
        }

        AssignOperator assignOp = new AssignOperator(newVars, exprs);
        assignOp.getInputs().add(new MutableObject<>(newScanOp));
        Mutable<ILogicalOperator> tmpRef = new MutableObject<>(assignOp);

        // aggregate [$$73, $$74, $$75, $$76, $$77, $$78] <- [agg-count(true), sql-avg($$68), sql-avg($$69), sql-avg($$70), sql-avg($$71), sql-avg($$72)]
        // add the count-agg (true) first
        List<LogicalVariable> newVars2 = new ArrayList<>();
        List<Mutable<ILogicalExpression>> aggExprList = new ArrayList<>();
        List<Mutable<ILogicalExpression>> aggFunArgs = new ArrayList<>(1);
        aggFunArgs.add(new MutableObject<>(ConstantExpression.TRUE));
        BuiltinFunctionInfo countFn = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.COUNT);
        AggregateFunctionCallExpression aggExpr = new AggregateFunctionCallExpression(countFn, false, aggFunArgs);
        aggExprList.add(new MutableObject<>(aggExpr));
        LogicalVariable aggVar = newCtx.newVar();
        newVars2.add(aggVar);

        // Now add the other aggs
        for (int i = 0; i < count; i++) {
            VariableReferenceExpression varRefExpr;
            varRefExpr = new VariableReferenceExpression(newVars.get(i));
            List<Mutable<ILogicalExpression>> vars2 = new ArrayList<>();
            vars2.add(new MutableObject<>(varRefExpr));
            BuiltinFunctionInfo avgFn = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SQL_AVG);
            aggExpr = new AggregateFunctionCallExpression(avgFn, false, vars2);
            newVar = newCtx.newVar();

            newVars2.add(newVar);
            aggExprList.add(new MutableObject<>(aggExpr));
        }

        // add assign [$$79] <- [{"$1": $$73, "$2": $$74, "$3": $$75, "$4": $$76, "$5": $$77, "$6": $$78}]
        AggregateOperator newAggOp = new AggregateOperator(newVars2, aggExprList);
        newAggOp.getInputs().add(new MutableObject<>(assignOp));
        Mutable<ILogicalOperator> newAggOpRef = new MutableObject<>(newAggOp);
        OperatorPropertiesUtil.typeOpRec(newAggOpRef, newCtx); // is this really needed??
        List<MutableObject> arr = createMutableObjectArray(newAggOp.getVariables());
        AbstractFunctionCallExpression f = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR));
        for (int i = 0; i < arr.size(); i++) {
            f.getArguments().add(arr.get(i));
        }

        newVar = newCtx.newVar();
        assignOp = new AssignOperator(newVar, new MutableObject<>(f));
        assignOp.getInputs().add(new MutableObject<>(newAggOp));
        ProjectOperator pOp = new ProjectOperator(newVar);
        pOp.getInputs().add(new MutableObject<>(assignOp));

        Mutable<ILogicalOperator> Ref = new MutableObject<>(pOp);

        OperatorPropertiesUtil.typeOpRec(Ref, newCtx);
        if (LOGGER.isTraceEnabled()) {
            String viewInPlan = new ALogicalPlanImpl(Ref).toString(); //useful when debugging
            LOGGER.trace("sampling query before calling runQuery");
            LOGGER.trace(viewInPlan);
        }

        LOGGER.info("***returning from projection sample query***");
        return AnalysisUtil.runQuery(Ref, Arrays.asList(newVar), newCtx, IRuleSetFactory.RuleSetKind.SAMPLING);
    }

    private List<MutableObject> createMutableObjectArray(List<LogicalVariable> vars) {
        List<MutableObject> arr = new ArrayList<>();
        for (int i = 0; i < vars.size(); i++) {
            LiteralExpr le = new LiteralExpr();
            StringLiteral value = new StringLiteral("$" + Integer.toString(i + 1)); // these start from 1
            le.setValue(value);
            AsterixConstantValue cValue = new AsterixConstantValue(ConstantHelper.objectFromLiteral(le.getValue()));
            ConstantExpression cExpr = new ConstantExpression(cValue);
            arr.add(new MutableObject<>(cExpr));
            arr.add(new MutableObject<>(new VariableReferenceExpression(vars.get(i))));
        }
        return arr;
    }

    public long findDistinctCardinality(ILogicalOperator grpByDistinctOp) throws AlgebricksException {
        long distinctCard = 0L;
        LogicalOperatorTag tag = grpByDistinctOp.getOperatorTag();

        // distinct cardinality supported only for GroupByOp and DistinctOp
        if (tag == LogicalOperatorTag.DISTINCT || tag == LogicalOperatorTag.GROUP) {
            ILogicalOperator parent = joinEnum.findDataSourceScanOperatorParent(grpByDistinctOp);
            DataSourceScanOperator scanOp = (DataSourceScanOperator) parent.getInputs().get(0).getValue();
            if (scanOp == null) {
                return distinctCard; // this may happen in case of in lists
            }

            Index index = findSampleIndex(scanOp, optCtx);
            if (index == null) {
                return distinctCard;
            }

            Index.SampleIndexDetails idxDetails = (Index.SampleIndexDetails) index.getIndexDetails();
            double origDatasetCard = idxDetails.getSourceCardinality();

            byte dsType = ((DataSource) scanOp.getDataSource()).getDatasourceType();
            if (!(dsType == DataSource.Type.INTERNAL_DATASET || dsType == DataSource.Type.EXTERNAL_DATASET)) {
                return distinctCard; // Datasource must be of a dataset, not supported for other datasource types
            }
            SampleDataSource sampleDataSource = joinEnum.getSampleDataSource(scanOp);

            ILogicalOperator parentOfSelectOp = findParentOfSelectOp(grpByDistinctOp);
            SelectOperator selOp = (parentOfSelectOp == null) ? null
                    : ((SelectOperator) parentOfSelectOp.getInputs().get(0).getValue());

            setTotalCardFromSample(idxDetails.getSampleCardinalityTarget()); // sample size without predicates (i.e., n)
            if (selOp != null) {
                long sampleWithPredicates = findSampleSizeWithPredicates(selOp, sampleDataSource);
                // set totalSamples to the sample size with predicates (i.e., n_f)
                setTotalCardFromSample(sampleWithPredicates);
            }
            // get the estimated distinct cardinality for the dataset (i.e., D_est or D_est_f)
            distinctCard = findEstDistinctWithPredicates(grpByDistinctOp, origDatasetCard, sampleDataSource);
        }
        return distinctCard;
    }

    private long findSampleSizeWithPredicates(SelectOperator selOp, SampleDataSource sampleDataSource)
            throws AlgebricksException {
        long sampleSize = Long.MAX_VALUE;
        ILogicalOperator copyOfSelOp = OperatorManipulationUtil.bottomUpCopyOperators(selOp);
        if (setSampleDataSource(copyOfSelOp, sampleDataSource)) {
            List<List<IAObject>> result = runSamplingQuery(optCtx, copyOfSelOp);
            sampleSize = (long) findPredicateCardinality(result, false);
        }
        return sampleSize;
    }

    private long findEstDistinctWithPredicates(ILogicalOperator grpByDistinctOp, double origDatasetCardinality,
            SampleDataSource sampleDataSource) throws AlgebricksException {
        double estDistCardinalityFromSample = -1.0;
        double estDistCardinality = -1.0;

        LogicalOperatorTag tag = grpByDistinctOp.getOperatorTag();
        if (tag == LogicalOperatorTag.GROUP || tag == LogicalOperatorTag.DISTINCT) {
            ILogicalOperator copyOfGrpByDistinctOp = OperatorManipulationUtil.bottomUpCopyOperators(grpByDistinctOp);
            if (setSampleDataSource(copyOfGrpByDistinctOp, sampleDataSource)) {
                // get distinct cardinality from the sampling source
                List<List<IAObject>> result = runSamplingQuery(optCtx, copyOfGrpByDistinctOp);
                estDistCardinalityFromSample = findPredicateCardinality(result, false);
            }
        }
        if (estDistCardinalityFromSample != -1.0) { // estimate distinct cardinality for the dataset from the sampled cardinality
            estDistCardinality = distinctEstimator(estDistCardinalityFromSample, origDatasetCardinality);
        }
        estDistCardinality = Math.max(0.0, estDistCardinality);
        return Math.round(estDistCardinality);
    }

    // Use the Newton-Raphson method for distinct cardinality estimation.
    private double distinctEstimator(double estDistinctCardinalityFromSample, double origDatasetCardinality) {
        // initialize the estimate to be the number of distinct values from the sample.
        double estDistinctCardinality = initNR(estDistinctCardinalityFromSample);
        setDistinctCardFromSample(estDistinctCardinality);

        int itr_counter = 0, max_counter = 1000; // allow a maximum number of iterations
        double denominator = derivativeFunctionForMMO(estDistinctCardinality);
        if (denominator == 0.0) { // Newton-Raphson method requires it to be non-zero
            return estDistinctCardinality;
        }
        double fraction = functionForMMO(estDistinctCardinality) / denominator;
        while (Math.abs(fraction) >= 0.001 && itr_counter < max_counter) {
            denominator = derivativeFunctionForMMO(estDistinctCardinality);
            if (denominator == 0.0) {
                break;
            }
            fraction = functionForMMO(estDistinctCardinality) / denominator;
            estDistinctCardinality = estDistinctCardinality - fraction;
            itr_counter++;
            if (estDistinctCardinality > origDatasetCardinality) {
                estDistinctCardinality = origDatasetCardinality; // for preventing infinite growth beyond N
                break;
            }
        }

        // estimated cardinality cannot be less the initial one from samples
        estDistinctCardinality = Math.max(estDistinctCardinality, estDistinctCardinalityFromSample);

        return estDistinctCardinality;
    }

    double initNR(double estDistinctCardinalityFromSample) {
        double estDistinctCardinality = estDistinctCardinalityFromSample;

        // Boundary condition checks for Newton-Raphson method.
        if (totalCardFromSample <= MIN_TOTAL_SAMPLES) {
            setTotalCardFromSample(totalCardFromSample + 2);
            estDistinctCardinality = totalCardFromSample - 1;
        } else if (estDistinctCardinality == totalCardFromSample) {
            estDistinctCardinality--;
        }
        return estDistinctCardinality;
    }

    private double functionForMMO(double x) {
        return (x * (1.0 - Math.exp(-1.0 * (double) totalCardFromSample / x)) - distinctCardFromSample);
    }

    private double derivativeFunctionForMMO(double x) {
        double arg = ((double) totalCardFromSample / x);
        return (1.0 - (arg + 1.0) * Math.exp(-1.0 * arg));
    }

    private boolean setSampleDataSource(ILogicalOperator op, SampleDataSource sampleDataSource) {
        ILogicalOperator parent = joinEnum.findDataSourceScanOperatorParent(op);
        DataSourceScanOperator scanOp = (DataSourceScanOperator) parent.getInputs().get(0).getValue();
        if (scanOp == null) {
            return false;
        }
        // replace the DataSourceScanOp with the sampling source
        scanOp.setDataSource(sampleDataSource);
        return true;
    }

    private ILogicalOperator findParentOfSelectOp(ILogicalOperator op) {
        ILogicalOperator parent = null;
        ILogicalOperator currentOp = op;
        LogicalOperatorTag tag = currentOp.getOperatorTag();

        while (tag != LogicalOperatorTag.DATASOURCESCAN) {
            if (tag == LogicalOperatorTag.SELECT) {
                return parent;
            }
            parent = currentOp;
            currentOp = currentOp.getInputs().get(0).getValue();
            tag = currentOp.getOperatorTag();
        }
        return null; // no SelectOp in the query tree
    }
}