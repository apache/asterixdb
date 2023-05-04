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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.AnalysisUtil;
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
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
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

    public Stats(IOptimizationContext context, JoinEnum joinE) {
        optCtx = context;
        joinEnum = joinE;
    }

    protected DataverseName findDataverseName(DataSourceScanOperator scanOp) {
        if (scanOp == null) {
            // this should rarely happen (IN lists may cause this)
            return null;
        }
        DataSourceId dsid = (DataSourceId) scanOp.getDataSource().getId();
        return dsid.getDataverseName();
    }

    protected Index findSampleIndex(DataSourceScanOperator scanOp, IOptimizationContext context)
            throws AlgebricksException {
        DataverseName dataverseName = findDataverseName(scanOp);
        DataSource ds = (DataSource) scanOp.getDataSource();
        DataSourceId dsid = ds.getId();
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();
        return mdp.findSampleIndex(dataverseName, dsid.getDatasourceName());
    }

    private double findJoinSelectivity(JoinProductivityAnnotation anno, AbstractFunctionCallExpression joinExpr)
            throws AlgebricksException {
        List<LogicalVariable> exprUsedVars = new ArrayList<>();
        joinExpr.getUsedVariables(exprUsedVars);
        if (exprUsedVars.size() != 2) {
            // Since there is a left and right dataset here, expecting only two variables.
            return 1.0;
        }
        int idx1 = joinEnum.findJoinNodeIndex(exprUsedVars.get(0)) + 1;
        int idx2 = joinEnum.findJoinNodeIndex(exprUsedVars.get(1)) + 1;
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
    private double getSelectivityFromAnnotation(AbstractFunctionCallExpression afcExpr, boolean join)
            throws AlgebricksException {
        double sel = 1.0;

        if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.OR)) {
            double orSel = getSelectivityFromAnnotation(
                    (AbstractFunctionCallExpression) afcExpr.getArguments().get(0).getValue(), join);
            for (int i = 1; i < afcExpr.getArguments().size(); i++) {
                ILogicalExpression lexpr = afcExpr.getArguments().get(i).getValue();
                if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    sel = getSelectivityFromAnnotation(
                            (AbstractFunctionCallExpression) afcExpr.getArguments().get(i).getValue(), join);
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
                            (AbstractFunctionCallExpression) afcExpr.getArguments().get(i).getValue(), join);
                    andSel *= sel;
                }
            }
            return andSel;
        } else if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
            ILogicalExpression lexpr = afcExpr.getArguments().get(0).getValue();
            if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                sel = getSelectivityFromAnnotation(
                        (AbstractFunctionCallExpression) afcExpr.getArguments().get(0).getValue(), join);
                return 1.0 - sel;
            }
        }

        double s;
        PredicateCardinalityAnnotation pca = afcExpr.getAnnotation(PredicateCardinalityAnnotation.class);
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
        } else {
            JoinProductivityAnnotation jpa = afcExpr.getAnnotation(JoinProductivityAnnotation.class);
            s = findJoinSelectivity(jpa, afcExpr);
            sel *= s;
        }
        if (join && s == 1.0) {
            // assume no selectivity was assigned
            joinEnum.singleDatasetPreds.add(afcExpr);
        }
        return sel;
    }

    protected double getSelectivityFromAnnotationMain(ILogicalExpression leExpr, boolean join)
            throws AlgebricksException {
        double sel = 1.0;

        if (leExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) leExpr;
            sel = getSelectivityFromAnnotation(afcExpr, join);
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
                sel *= getSelectivityFromAnnotationMain(selOper.getCondition().getValue(), join);
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
                sel *= getSelectivityFromAnnotationMain(selOper.getCondition().getValue(), false);
            }
            if (op.getInputs().size() > 0) {
                op = op.getInputs().get(0).getValue();
            } else {
                break;
            }
        }
        return sel;
    }

    private int countOps(ILogicalOperator op, LogicalOperatorTag tag) {
        int count = 0;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag().equals(tag)) {
                count++;
            }
            op = op.getInputs().get(0).getValue();
        }
        return count;
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
            double datasetCard) throws AlgebricksException {
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
        // origDatasetCard must be equal to datasetCard. So we do not need datasetCard passed in here. VIJAY check if
        // this parameter can be removed.
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

        int numSubplans = countOps(selOp, LogicalOperatorTag.SUBPLAN);

        List<List<IAObject>> result;

        // insert this in place of the scandatasourceOp.
        parent.getInputs().get(0).setValue(deepCopyofScan);
        if (numSubplans == 0) { // just switch the predicates; the simplest case. There should be no other case if subplans were canonical
            ILogicalExpression saveExprs = selOp.getCondition().getValue();
            selOp.getCondition().setValue(exp);
            result = runSamplingQuery(optCtx, selOp);
            selOp.getCondition().setValue(saveExprs);
        } else {
            int numSelects = countOps(selOp, LogicalOperatorTag.SELECT);
            int nonSubplanSelects = numSelects - numSubplans;

            if (numSubplans == 1 && nonSubplanSelects == 0) {
                result = runSamplingQuery(optCtx, selOp); // no need to switch anything
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
        // switch  the scanOp back
        parent.getInputs().get(0).setValue(scanOp);

        double predicateCardinality = (double) ((AInt64) result.get(0).get(0)).getLongValue();
        if (predicateCardinality == 0.0) {
            predicateCardinality = 0.0001 * idxDetails.getSampleCardinalityTarget();
        }
        double sel = (double) predicateCardinality / sampleCard;
        return sel;
    }

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

        return AnalysisUtil.runQuery(newAggOpRef, Arrays.asList(aggVar), newCtx, IRuleSetFactory.RuleSetKind.SAMPLING);
    }
}
