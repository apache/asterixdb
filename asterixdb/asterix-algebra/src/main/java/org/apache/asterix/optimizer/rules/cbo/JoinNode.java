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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.cost.Cost;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.asterix.optimizer.rules.am.AccessMethodAnalysisContext;
import org.apache.asterix.optimizer.rules.am.IAccessMethod;
import org.apache.asterix.optimizer.rules.am.IOptimizableFuncExpr;
import org.apache.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import org.apache.asterix.optimizer.rules.am.IntroduceSelectAccessMethodRule;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JoinNode {
    private static final Logger LOGGER = LogManager.getLogger();
    protected JoinEnum joinEnum;
    protected int jnArrayIndex;
    protected int datasetBits; // this is bitmap of all the keyspaceBits present in this joinNode
    protected List<Integer> datasetIndexes;
    protected List<String> datasetNames;
    protected List<String> aliases;
    protected int cheapestPlanIndex;
    protected PlanNode cheapestPlanNode;
    private ICost cheapestPlanCost;
    protected double origCardinality; // without any selections
    protected double cardinality;
    protected double size; // avg size of whole document; available from the sample
    protected double diskProjectionSize; // what is coming out of the disk; in case of row format, it is the entire document
                                         // in case of columnar we need to add sizes of individual fields.
    protected double projectionSizeAfterScan; // excludes fields only used for selections
    protected double distinctCardinality; // estimated distinct cardinality for this joinNode
    protected List<Integer> planIndexesArray; // indexes into the PlanNode array in enumerateJoins
    protected int jnIndex;
    protected int level;
    protected int highestDatasetId;
    private JoinNode rightJn;
    private JoinNode leftJn;
    private int limitVal = -1; // only for single dataset joinNodes.
    private List<Integer> applicableJoinConditions;
    protected ILogicalOperator leafInput;
    protected Index.SampleIndexDetails idxDetails;
    private List<Triple<Index, Double, AbstractFunctionCallExpression>> IndexCostInfo;
    // The triple above is : Index, selectivity, and the index expression
    protected static int NO_JN = -1;
    private static int NO_CARDS = -2;
    private int numVarsFromDisk = -1; // number of variables projected from disk
    private int numVarsAfterScan = -1; // number of variables after all selection fields have been removed and are needed for joins and final projects
    private double sizeVarsFromDisk = -1.0;
    private double sizeVarsAfterScan = -1.0;
    private boolean columnar = true; // default

    private JoinNode(int i) {
        this.jnArrayIndex = i;
        planIndexesArray = new ArrayList<>();
        cheapestPlanIndex = PlanNode.NO_PLAN;
        cheapestPlanNode = null;
        size = 1; // for now, will be the size of the doc for this joinNode
    }

    protected JoinNode(int i, JoinEnum joinE) {
        this(i);
        joinEnum = joinE;
        cheapestPlanCost = joinEnum.getCostHandle().maxCost();
    }

    protected boolean IsBaseLevelJoinNode() {
        return this.jnArrayIndex <= joinEnum.numberOfTerms;
    }

    protected boolean IsHigherLevelJoinNode() {
        return !IsBaseLevelJoinNode();
    }

    public double getCardinality() {
        return cardinality;
    }

    protected void setCardinality(double card, boolean setMinCard) {
        // Minimum cardinality for operators is MIN_CARD to prevent bad plans due to cardinality under estimation errors.
        cardinality = setMinCard ? Math.max(card, Cost.MIN_CARD) : card;
    }

    public double getOrigCardinality() {
        return origCardinality;
    }

    public PlanNode getCheapestPlanNode() {
        return cheapestPlanNode;
    }

    protected void setOrigCardinality(double card, boolean setMinCard) {
        // Minimum cardinality for operators is MIN_CARD to prevent bad plans due to cardinality under estimation errors.
        origCardinality = setMinCard ? Math.max(card, Cost.MIN_CARD) : card;
    }

    public void setAvgDocSize(double avgDocSize) {
        size = avgDocSize;
    }

    public double getAvgDocSize() {
        return size;
    }

    public void setLimitVal(int val) {
        limitVal = val;
    }

    public int getLimitVal() {
        return limitVal;
    }

    public double getInputSize() {
        return sizeVarsFromDisk;
    }

    public double getOutputSize() {
        return sizeVarsAfterScan;
    }

    public JoinNode getLeftJn() {
        return leftJn;
    }

    public JoinNode getRightJn() {
        return rightJn;
    }

    private List<String> getAliases() {
        return aliases;
    }

    protected List<String> getDatasetNames() {
        return datasetNames;
    }

    protected Index.SampleIndexDetails getIdxDetails() {
        return idxDetails;
    }

    public void setNumVarsFromDisk(int num) {
        numVarsFromDisk = num;
    }

    public void setNumVarsAfterScan(int num) {
        numVarsAfterScan = num;
    }

    public void setSizeVarsFromDisk(double size) {
        sizeVarsFromDisk = size;
    }

    public void setSizeVarsAfterScan(double size) {
        sizeVarsAfterScan = size;
    }

    public int getNumVarsFromDisk() {
        return numVarsFromDisk;
    }

    public int getNumVarsAfterScan() {
        return numVarsAfterScan;
    }

    public double getSizeVarsFromDisk() {
        return sizeVarsFromDisk;
    }

    public double getSizeVarsAfterScan() {
        return sizeVarsAfterScan;
    }

    public void setColumnar(boolean format) {
        columnar = format;
    }

    public boolean getColumnar() {
        return columnar;
    }

    public void setCardsAndSizes(Index.SampleIndexDetails idxDetails, ILogicalOperator leafInput)
            throws AlgebricksException {

        double origDatasetCard, finalDatasetCard;
        finalDatasetCard = origDatasetCard = idxDetails.getSourceCardinality();

        DataSourceScanOperator scanOp = joinEnum.findDataSourceScanOperator(leafInput);
        if (scanOp == null) {
            return; // what happens to the cards and sizes then? this may happen in case of in lists
        }

        double sampleCard = Math.min(idxDetails.getSampleCardinalityTarget(), origDatasetCard);
        if (sampleCard == 0) { // should not happen unless the original dataset is empty
            sampleCard = 1; // we may have to make some adjustments to costs when the sample returns very rows.

            IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(scanOp.getSourceLocation(),
                        org.apache.asterix.common.exceptions.ErrorCode.SAMPLE_HAS_ZERO_ROWS));
            }
        }

        List<List<IAObject>> result;
        SelectOperator selop = (SelectOperator) joinEnum.findASelectOp(leafInput);
        if (selop == null) { // add a SelectOperator with TRUE condition. The code below becomes simpler with a select operator.
            selop = new SelectOperator(new MutableObject<>(ConstantExpression.TRUE));
            ILogicalOperator op = selop;
            op.getInputs().add(new MutableObject<>(leafInput));
            leafInput = op;
        }
        ILogicalOperator parent = joinEnum.findDataSourceScanOperatorParent(leafInput);
        Mutable<ILogicalOperator> ref = new MutableObject<>(leafInput);

        OperatorPropertiesUtil.typeOpRec(ref, joinEnum.optCtx);
        if (LOGGER.isTraceEnabled()) {
            String viewPlan = new ALogicalPlanImpl(ref).toString(); //useful when debugging
            LOGGER.trace("viewPlan");
            LOGGER.trace(viewPlan);
        }

        // find if row or columnar format
        DatasetDataSource dds = (DatasetDataSource) scanOp.getDataSource();
        if (dds.getDataset().getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.ROW) {
            setColumnar(false);
        }

        SampleDataSource sampledatasource = joinEnum.getSampleDataSource(scanOp);
        DataSourceScanOperator deepCopyofScan =
                (DataSourceScanOperator) OperatorManipulationUtil.bottomUpCopyOperators(scanOp);
        deepCopyofScan.setDataSource(sampledatasource);
        LogicalVariable primaryKey;
        if (deepCopyofScan.getVariables().size() > 1) {
            primaryKey = deepCopyofScan.getVariables().get(1);
        } else {
            primaryKey = deepCopyofScan.getVariables().get(0);
        }
        // if there is only one conjunct, I do not have to call the sampling query during index selection!
        // insert this in place of the scandatasourceOp.
        parent.getInputs().get(0).setValue(deepCopyofScan);
        // There are predicates here. So skip the predicates and get the original dataset card.
        // Now apply all the predicates and get the card after all predicates are applied.
        result = joinEnum.getStatsHandle().runSamplingQueryProjection(joinEnum.optCtx, leafInput, jnArrayIndex,
                primaryKey);
        double predicateCardinalityFromSample = joinEnum.getStatsHandle().findPredicateCardinality(result, true);

        double sizeVarsFromDisk;
        double sizeVarsAfterScan;

        if (predicateCardinalityFromSample > 0.0) { // otherwise, we get nulls for the averages
            sizeVarsFromDisk = joinEnum.getStatsHandle().findSizeVarsFromDisk(result, getNumVarsFromDisk());
            sizeVarsAfterScan = joinEnum.getStatsHandle().findSizeVarsAfterScan(result, getNumVarsFromDisk());
        } else { // in case we did not get any tuples from the sample, get the size by setting the predicate to true.
            ILogicalExpression saveExpr = selop.getCondition().getValue();
            selop.getCondition().setValue(ConstantExpression.TRUE);
            result = joinEnum.getStatsHandle().runSamplingQueryProjection(joinEnum.optCtx, leafInput, jnArrayIndex,
                    primaryKey);
            double x = joinEnum.getStatsHandle().findPredicateCardinality(result, true);
            // better to check if x is 0
            if (x == 0.0) {
                sizeVarsFromDisk = getNumVarsFromDisk() * 100;
                sizeVarsAfterScan = getNumVarsAfterScan() * 100; // cant think of anything better... cards are more important anyway
            } else {
                sizeVarsFromDisk = joinEnum.getStatsHandle().findSizeVarsFromDisk(result, getNumVarsFromDisk());
                sizeVarsAfterScan = joinEnum.getStatsHandle().findSizeVarsAfterScan(result, getNumVarsFromDisk());
            }
            selop.getCondition().setValue(saveExpr); // restore the expression
        }

        // Adjust for zero predicate cardinality from the sample.
        predicateCardinalityFromSample = Math.max(predicateCardinalityFromSample, 0.0001);

        // Do the scale up for the final cardinality after the predicates.
        boolean scaleUp = sampleCard != origDatasetCard;
        if (scaleUp) {
            finalDatasetCard *= predicateCardinalityFromSample / sampleCard;
        } else {
            finalDatasetCard = predicateCardinalityFromSample;
        }
        // now switch the input back.
        parent.getInputs().get(0).setValue(scanOp);

        if (getCardinality() == getOrigCardinality()) { // this means there was no selectivity hint provided
            // If the sample size is the same as the original dataset (happens when the dataset
            // is small), no need to assign any artificial min. cardinality as the sample is accurate.
            setCardinality(finalDatasetCard, scaleUp);
        }

        setSizeVarsFromDisk(sizeVarsFromDisk);
        setSizeVarsAfterScan(sizeVarsAfterScan);
        setAvgDocSize(idxDetails.getSourceAvgItemSize());
    }

    /** one is a subset of two */
    private boolean subset(int one, int two) {
        return (one & two) == one;
    }

    private void findApplicableJoinConditions() {
        List<JoinCondition> joinConditions = joinEnum.getJoinConditions();

        int i = 0;
        for (JoinCondition jc : joinConditions) {
            if (subset(jc.datasetBits, this.datasetBits)) {
                this.applicableJoinConditions.add(i);
            }
            i++;
        }
    }

    private List<Integer> getNewJoinConditionsOnly() {
        List<Integer> newJoinConditions = new ArrayList<>();
        JoinNode leftJn = this.leftJn;
        JoinNode rightJn = this.rightJn;
        // find the new table being added. This assume only zig zag trees for now.
        int newTableBits = 0;
        if (leftJn.jnArrayIndex <= joinEnum.numberOfTerms) {
            newTableBits = leftJn.datasetBits;
        } else if (rightJn.jnArrayIndex <= joinEnum.numberOfTerms) {
            newTableBits = rightJn.datasetBits;
        }

        if (LOGGER.isTraceEnabled() && newTableBits == 0) {
            LOGGER.trace("newTable Bits == 0");
        }

        // All the new join predicates will have these bits turned on
        for (int idx : this.applicableJoinConditions) {
            if ((joinEnum.joinConditions.get(idx).datasetBits & newTableBits) > 0) {
                newJoinConditions.add(idx);
            }
        }

        return newJoinConditions; // this can be of size 0 because this may be a cartesian join
    }

    public double computeJoinCardinality() {
        JoinNode[] jnArray = joinEnum.getJnArray();
        List<JoinCondition> joinConditions = joinEnum.getJoinConditions();
        double joinCard;

        this.applicableJoinConditions = new ArrayList<>();
        findApplicableJoinConditions();

        if (LOGGER.isTraceEnabled() && this.applicableJoinConditions.size() == 0) {
            LOGGER.trace("applicable Join Conditions size is 0 in join Node " + this.jnArrayIndex);
        }

        // Wonder if this computation will result in an overflow exception. Better to multiply them with selectivities also.
        double productJoinCardinality = 1.0;
        for (int idx : this.datasetIndexes) {
            productJoinCardinality *= jnArray[idx].cardinality;
        }

        double productJoinSels = 1.0;
        for (int idx : this.applicableJoinConditions) {
            productJoinSels *= joinConditions.get(idx).selectivity;
        }
        joinCard = productJoinCardinality * productJoinSels;

        double redundantSel = 1.0;
        // Now see if any redundant edges are present; R.a = S.a and S.a = T.a ==> R.a = T.a.
        // One of them must be removed to estimate cardinality correctly.
        if (this.applicableJoinConditions.size() >= 3) {
            redundantSel = removeRedundantPred(this.applicableJoinConditions);
        }

        // By dividing by redundantSel, we are undoing the earlier multiplication of all the selectivities.
        return joinCard / redundantSel;
    }

    private static double adjustSelectivities(JoinCondition jc1, JoinCondition jc2, JoinCondition jc3) {
        double sel;
        if (jc1.comparisonType == JoinCondition.comparisonOp.OP_EQ
                && jc2.comparisonType == JoinCondition.comparisonOp.OP_EQ
                && jc3.comparisonType == JoinCondition.comparisonOp.OP_EQ) {
            sel = findRedundantSel(jc1.selectivity, jc2.selectivity, jc3.selectivity);
        } else {
            // at least one of the predicates in not an equality predicate
            //this can get messy here, as 1, or 2 or all 3 can be non equality
            // we will just drop the first one we find now
            if (jc1.comparisonType != JoinCondition.comparisonOp.OP_EQ) {
                sel = jc1.selectivity;
            } else if (jc2.comparisonType != JoinCondition.comparisonOp.OP_EQ) {
                sel = jc2.selectivity;
            } else {
                sel = jc3.selectivity;
            }
        }
        return sel;
    }

    // if a redundant edge is found, we need to eliminate one of the edges.
    // If two triangles share an edge, removing the common edge will suffice
    // Each edge has two vertices. So we can only handle predicate with exactly two tables such as R.a = S.a
    // We will not handle cases such as R.a + S.a = T.a
    // It should be easy to identify two vertex edges as only two bits will be set for such conditions.
    private double removeRedundantPred(List<Integer> applicablePredicatesInCurrentJn) {
        double redundantSel = 1.0;
        List<JoinCondition> joinConditions = joinEnum.getJoinConditions();
        JoinCondition jc1, jc2, jc3;
        int[] vertices = new int[6];
        int[] verticesCopy = new int[6];
        for (int i = 0; i <= applicablePredicatesInCurrentJn.size() - 3; i++) {
            jc1 = joinConditions.get(applicablePredicatesInCurrentJn.get(i));
            if (jc1.partOfComposite) {
                continue; // must ignore these or the same triangles will be found more than once.
            }
            vertices[0] = jc1.leftSideBits;
            vertices[1] = jc1.rightSideBits;
            for (int j = i + 1; j <= applicablePredicatesInCurrentJn.size() - 2; j++) {
                jc2 = joinConditions.get(applicablePredicatesInCurrentJn.get(j));
                if (jc2.partOfComposite) {
                    continue;
                }
                vertices[2] = jc2.leftSideBits;
                vertices[3] = jc2.rightSideBits;
                for (int k = j + 1; k <= applicablePredicatesInCurrentJn.size() - 1; k++) {
                    jc3 = joinConditions.get(applicablePredicatesInCurrentJn.get(k));
                    if (jc3.partOfComposite) {
                        continue;
                    }
                    vertices[4] = jc3.leftSideBits;
                    vertices[5] = jc3.rightSideBits;

                    System.arraycopy(vertices, 0, verticesCopy, 0, 6);
                    Arrays.sort(verticesCopy);
                    if (verticesCopy[0] == verticesCopy[1] && verticesCopy[2] == verticesCopy[3]
                            && verticesCopy[4] == verticesCopy[5]) {
                        // redundant edge found
                        redundantSel *= adjustSelectivities(jc1, jc2, jc3);
                    }
                }
            }
        }
        return redundantSel;
    }

    private static double findRedundantSel(double sel1, double sel2, double sel3) {
        double[] sels = new double[3];
        sels[0] = sel1;
        sels[1] = sel2;
        sels[2] = sel3;

        Arrays.sort(sels); // we are sorting to make this deterministic
        return sels[1]; // the middle one is closest to one of the extremes
    }

    protected int addSingleDatasetPlans() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost opCost;
        PlanNode pn;
        opCost = joinEnum.getCostMethodsHandle().costFullScan(this);
        boolean forceEnum = level <= joinEnum.cboFullEnumLevel;
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || opCost.costLT(this.cheapestPlanCost) || forceEnum) {
            // for now just add one plan
            pn = new PlanNode(allPlans.size(), joinEnum, this, datasetNames.get(0), leafInput);
            pn.setScanMethod(PlanNode.ScanMethod.TABLE_SCAN);
            pn.setScanCosts(opCost);
            planIndexesArray.add(pn.allPlansIndex);
            allPlans.add(pn);
            setCheapestPlan(pn, forceEnum);
            return pn.allPlansIndex;
        }
        return PlanNode.NO_PLAN;
    }

    private AbstractFunctionCallExpression buildExpr(List<IOptimizableFuncExpr> exprs,
            List<Pair<Integer, Integer>> pairs) {
        int i;
        if (pairs.size() == 1) {
            i = pairs.get(0).getFirst();
            return exprs.get(i).getFuncExpr();
        }

        ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));

        for (i = 0; i < pairs.size(); i++) {
            IOptimizableFuncExpr expr = exprs.get(pairs.get(i).getFirst());
            andExpr.getArguments().add(new MutableObject<>(expr.getFuncExpr()));
        }
        return andExpr;
    }

    private void setSkipIndexAnnotationsForUnusedIndexes() {
        for (int i = 0; i < IndexCostInfo.size(); i++) {
            if (IndexCostInfo.get(i).second == -1.0) {
                AbstractFunctionCallExpression afce = IndexCostInfo.get(i).third;
                SkipSecondaryIndexSearchExpressionAnnotation skipAnno = joinEnum.findSkipIndexHint(afce);
                Collection<String> indexNames = new HashSet<>();
                if (skipAnno != null && skipAnno.getIndexNames() != null) {
                    indexNames.addAll(skipAnno.getIndexNames());
                }
                if (indexNames.isEmpty()) {
                    // this index has to be skipped, so find the corresponding expression
                    EnumerateJoinsRule.setAnnotation(afce, SkipSecondaryIndexSearchExpressionAnnotation
                            .newInstance(Collections.singleton(IndexCostInfo.get(i).first.getIndexName())));
                } else {
                    indexNames.add(IndexCostInfo.get(i).first.getIndexName());
                    EnumerateJoinsRule.setAnnotation(afce,
                            SkipSecondaryIndexSearchExpressionAnnotation.newInstance(indexNames));
                }
            }
        }
    }

    private void costAndChooseIndexPlans(ILogicalOperator leafInput,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) throws AlgebricksException {
        SelectOperator selOp;
        double sel;

        List<Triple<Index, Double, AbstractFunctionCallExpression>> IndexCostInfo = new ArrayList<>();
        for (Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry : analyzedAMs.entrySet()) {
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                    analysisCtx.getIteratorForIndexExprsAndVars();
            List<IOptimizableFuncExpr> exprs = analysisCtx.getMatchedFuncExprs();
            while (indexIt.hasNext()) {
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
                Index chosenIndex = indexEntry.getKey();
                if (chosenIndex.getIndexType().equals(DatasetConfig.IndexType.LENGTH_PARTITIONED_WORD_INVIX)
                        || chosenIndex.getIndexType().equals(DatasetConfig.IndexType.SINGLE_PARTITION_WORD_INVIX)
                        || chosenIndex.getIndexType().equals(DatasetConfig.IndexType.LENGTH_PARTITIONED_NGRAM_INVIX)
                        || chosenIndex.getIndexType().equals(DatasetConfig.IndexType.SINGLE_PARTITION_NGRAM_INVIX)) {
                    continue;
                }
                AbstractFunctionCallExpression afce = buildExpr(exprs, indexEntry.getValue());
                PredicateCardinalityAnnotation selectivityAnnotation =
                        afce.getAnnotation(PredicateCardinalityAnnotation.class);
                if (selectivityAnnotation != null) {
                    sel = selectivityAnnotation.getSelectivity();
                } else {
                    if (leafInput.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                        selOp = (SelectOperator) joinEnum.getStatsHandle().findSelectOpWithExpr(leafInput, afce);
                        if (selOp == null) {
                            selOp = (SelectOperator) leafInput;
                        }
                    } else {
                        selOp = new SelectOperator(new MutableObject<>(afce));
                        selOp.getInputs().add(new MutableObject<>(leafInput));
                    }
                    sel = joinEnum.getStatsHandle().findSelectivityForThisPredicate(selOp, afce,
                            chosenIndex.getIndexType().equals(DatasetConfig.IndexType.ARRAY)
                                    || joinEnum.findUnnestOp(selOp));
                }
                IndexCostInfo.add(new Triple<>(chosenIndex, sel, afce));
            }
        }
        this.IndexCostInfo = IndexCostInfo;
        if (IndexCostInfo.size() > 0) {
            buildIndexPlans();
        }
        setSkipIndexAnnotationsForUnusedIndexes();
    }

    private void buildIndexPlans() {
        List<PlanNode> allPlans = joinEnum.getAllPlans();
        PlanNode pn;
        ICost opCost, totalCost;
        List<Triple<Index, Double, AbstractFunctionCallExpression>> mandatoryIndexesInfo = new ArrayList<>();
        List<Triple<Index, Double, AbstractFunctionCallExpression>> optionalIndexesInfo = new ArrayList<>();
        double sel = 1.0;
        opCost = this.joinEnum.getCostHandle().zeroCost();
        for (int i = 0; i < IndexCostInfo.size(); i++) {
            if (joinEnum.findUseIndexHint(IndexCostInfo.get(i).third)) {
                mandatoryIndexesInfo.add(IndexCostInfo.get(i));
            } else {
                optionalIndexesInfo.add(IndexCostInfo.get(i));
            }
        }

        List<ICost> indexCosts = new ArrayList<>(); // these are the costs associated with the index only
        // First cost all the mandatory indexes. These will be in the plan regardless of the cost
        if (mandatoryIndexesInfo.size() > 0) {
            for (int i = 0; i < mandatoryIndexesInfo.size(); i++) {
                indexCosts.add(joinEnum.getCostMethodsHandle().costIndexScan(this, mandatoryIndexesInfo.get(i).second));
            }

            opCost = this.joinEnum.getCostHandle().zeroCost();

            for (int i = 0; i < mandatoryIndexesInfo.size(); i++) {
                opCost = opCost.costAdd(indexCosts.get(i)); // opCost will have all the index scan costs
                sel *= mandatoryIndexesInfo.get(i).second; // assuming selectivities are independent for now
            }

            // Now add the data Scan cost.
            ICost dataScanCost = joinEnum.getCostMethodsHandle().costIndexDataScan(this, sel);
            opCost = opCost.costAdd(dataScanCost); // opCost now has the total cost of all the mandatory indexes + data costs.

        }

        ICost mandatoryIndexesCost = opCost; // This will be added at the end to the total cost irrespective of optimality.

        // Now lets deal with the optional indexes. These are the ones without any hints on them.
        List<ICost> dataCosts = new ArrayList<>(); // these are the costs associated with accessing the data records
        indexCosts.clear();
        if (optionalIndexesInfo.size() > 0) {
            optionalIndexesInfo.sort(Comparator.comparingDouble(o -> o.second)); // sort on selectivity.

            // find the costs using one index at a time first.

            // sel is now the selectivity of all the previous mandatory indexes.
            for (int i = 0; i < optionalIndexesInfo.size(); i++) {
                indexCosts.add(joinEnum.getCostMethodsHandle().costIndexScan(this, optionalIndexesInfo.get(i).second)); // I0; I1; I2; ...
                // Now get the cost of the datascans involved with the multiplied selectivity
                // dataCost (0) will contain the dataScan cost with the first index
                //dataCost (1) will contain the dataScan cost with the first index and the 2nd index and so on.
                sel *= optionalIndexesInfo.get(i).second; // assuming selectivities are independent for now
                if (optionalIndexesInfo.get(i).first.isPrimaryIndex()) {
                    dataCosts.add(joinEnum.getCostHandle().zeroCost());
                } else {
                    dataCosts.add(joinEnum.getCostMethodsHandle().costIndexDataScan(this, sel)); // D0; D01; D012; ...
                }
            }

            // At the of of the above loop, I0, I1, I2 ... have been computed
            // Also D0, D01, D012 ... have been computed.

            opCost = indexCosts.get(0).costAdd(dataCosts.get(0));
            //opCost is now the cost of the first (and cheapest) optional index plus the corresponding data scan

            //Intersect the first two and then the first three and so on.
            //If the cost does not decrease, then stop

            ICost newIdxCost = indexCosts.get(0); // I0
            ICost currentCost;
            for (int i = 1; i < optionalIndexesInfo.size(); i++) {
                newIdxCost = newIdxCost.costAdd(indexCosts.get(i)); // I0 + I1; I0 + I1 + I2
                currentCost = newIdxCost.costAdd(dataCosts.get(i)); // I0 + I1 + D01; I0 + I1 + I2 + D012
                if (currentCost.costLT(opCost) || level <= joinEnum.cboFullEnumLevel) { // save this cost and try adding one more index
                    opCost = currentCost;
                } else {
                    // set the selectivites of the indexes not picked to be -1.0, so we can set
                    // the skp index annotations correctly
                    for (int j = i; j < optionalIndexesInfo.size(); j++) {
                        optionalIndexesInfo.get(j).second = -1.0;
                    }
                    break; // can't get any cheaper.
                }
            }
        }

        // opCost is now the total cost of the indexes chosen along with the associated data scan cost.
        if (opCost.costGT(this.cheapestPlanCost) && level > joinEnum.cboFullEnumLevel) { // cheapest plan cost is the data scan cost.
            for (int j = 0; j < optionalIndexesInfo.size(); j++) {
                optionalIndexesInfo.get(j).second = -1.0; // remove all indexes from consideration.
            }
        }

        totalCost = opCost.costAdd(mandatoryIndexesCost); // cost of all the indexes chosen
        boolean forceEnum = mandatoryIndexesInfo.size() > 0 || level <= joinEnum.cboFullEnumLevel;
        if (opCost.costLT(this.cheapestPlanCost) || forceEnum) {
            pn = new PlanNode(allPlans.size(), joinEnum, this, datasetNames.get(0), leafInput);
            pn.setScanAndHintInfo(PlanNode.ScanMethod.INDEX_SCAN, mandatoryIndexesInfo, optionalIndexesInfo);
            pn.setScanCosts(totalCost);
            planIndexesArray.add(pn.allPlansIndex);
            allPlans.add(pn);
            setCheapestPlan(pn, forceEnum);
        }
    }

    private SelectOperator copySelExprsAndSetTrue(List<ILogicalExpression> selExprs, List<SelectOperator> selOpers,
            ILogicalOperator leafInput) {
        ILogicalOperator op = leafInput;
        SelectOperator firstSelOp = null;
        boolean firstSel = true;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOp = (SelectOperator) op;
                if (firstSel) {
                    firstSelOp = selOp;
                    firstSel = false;
                }
                selOpers.add(selOp);
                selExprs.add(selOp.getCondition().getValue());
                selOp.getCondition().setValue(ConstantExpression.TRUE); // we will switch these back later
            }
            op = op.getInputs().get(0).getValue();
        }
        return firstSelOp;
    }

    private void restoreSelExprs(List<ILogicalExpression> selExprs, List<SelectOperator> selOpers) {
        for (int i = 0; i < selExprs.size(); i++) {
            selOpers.get(i).getCondition().setValue(selExprs.get(i));
        }
    }

    private ILogicalExpression andAlltheExprs(List<ILogicalExpression> selExprs) {
        if (selExprs.size() == 1) {
            return selExprs.get(0);
        }

        ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));

        for (ILogicalExpression se : selExprs) {
            andExpr.getArguments().add(new MutableObject<>(se));
        }
        return andExpr;
    }

    // Look for the pattern select, select, subplan and collapse to select, subplan
    // This code does not belong in the CBO!!
    private boolean combineDoubleSelectsBeforeSubPlans(ILogicalOperator op) {
        boolean changes = false;
        while (op != null && op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOp1 = (SelectOperator) op;
                if (selOp1.getInputs().get(0).getValue().getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                    SelectOperator selOp2 = (SelectOperator) (op.getInputs().get(0).getValue());
                    ILogicalOperator op2 = selOp2.getInputs().get(0).getValue();
                    if (op2.getOperatorTag() == LogicalOperatorTag.SUBPLAN) { // found the pattern we are looking for
                        selOp1.getInputs().get(0).setValue(op2);
                        ILogicalExpression exp1 = selOp1.getCondition().getValue();
                        ILogicalExpression exp2 = selOp2.getCondition().getValue();
                        ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));
                        andExpr.getArguments().add(new MutableObject<>(exp1));
                        andExpr.getArguments().add(new MutableObject<>(exp2));
                        selOp1.getCondition().setValue(andExpr);
                        op = op2.getInputs().get(0).getValue();
                        changes = true;
                    }
                }
            }
            op = op.getInputs().get(0).getValue();
        }
        return changes;
    }

    protected void addIndexAccessPlans(ILogicalOperator leafInput) throws AlgebricksException {
        IntroduceSelectAccessMethodRule tmp = new IntroduceSelectAccessMethodRule();
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();

        while (combineDoubleSelectsBeforeSubPlans(leafInput));
        List<ILogicalExpression> selExprs = new ArrayList<>();
        List<SelectOperator> selOpers = new ArrayList<>();
        SelectOperator firstSelop = copySelExprsAndSetTrue(selExprs, selOpers, leafInput);
        if (firstSelop != null) { // if there are no selects, then there is no question of index selections either.
            firstSelop.getCondition().setValue(andAlltheExprs(selExprs));
            boolean index_access_possible =
                    tmp.checkApplicable(new MutableObject<>(leafInput), joinEnum.optCtx, chosenIndexes, analyzedAMs);
            restoreSelExprs(selExprs, selOpers);
            if (index_access_possible) {
                costAndChooseIndexPlans(leafInput, analyzedAMs);
            }
        } else {
            restoreSelExprs(selExprs, selOpers);
        }
    }

    // check if the left side or the right side is the preserving side.
    // The preserving side has to be the probe side (which is the left side since our engine builds from the right side)
    // R LOJ S -- R is the preserving side; S is the null extending side.
    // In the dependency list, S will the second entry in the quadruple.
    // So R must be on the left side and S must be on the right side.

    private boolean nullExtendingSide(int bits, boolean outerJoin) {
        if (outerJoin) {
            for (Quadruple<Integer, Integer, JoinOperator, Integer> qu : joinEnum.outerJoinsDependencyList) {
                if (qu.getThird().getOuterJoin()) {
                    if (qu.getSecond() == bits) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean hashJoinApplicable(JoinNode leftJn, boolean outerJoin, ILogicalExpression hashJoinExpr) {
        if (nullExtendingSide(leftJn.datasetBits, outerJoin)) {
            return false;
        }

        if (hashJoinExpr == null || hashJoinExpr == ConstantExpression.TRUE) {
            return false;
        }

        if (joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_LEFTDEEP) && !leftJn.IsBaseLevelJoinNode()
                && level > joinEnum.cboFullEnumLevel) {
            return false;
        }

        if (joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_RIGHTDEEP)
                && !rightJn.IsBaseLevelJoinNode() && level > joinEnum.cboFullEnumLevel) {
            return false;
        }

        return true;
    }

    private boolean nestedLoopsApplicable(ILogicalExpression joinExpr, boolean outerJoin,
            List<Pair<IAccessMethod, Index>> chosenIndexes, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs)
            throws AlgebricksException {

        if (joinExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        List<LogicalVariable> usedVarList = new ArrayList<>();
        joinExpr.getUsedVariables(usedVarList);
        if (usedVarList.size() != 2 && outerJoin) {
            return false;
        }
        ILogicalOperator joinLeafInput0 = null;
        ILogicalOperator joinLeafInput1 = null;
        // Find which joinLeafInput these vars belong to.
        // go through the leaf inputs and see where these variables came from
        for (LogicalVariable usedVar : usedVarList) {
            ILogicalOperator joinLeafInput = joinEnum.findLeafInput(Collections.singletonList(usedVar));
            if (joinLeafInput0 == null) {
                joinLeafInput0 = joinLeafInput;
            } else if (joinLeafInput1 == null && joinLeafInput != joinLeafInput0) {
                joinLeafInput1 = joinLeafInput;
            }
            // This check ensures that the used variables in the join expression
            // refer to not more than two leaf inputs.
            if (joinLeafInput != joinLeafInput0 && joinLeafInput != joinLeafInput1) {
                return false;
            }
        }

        // This check ensures that the used variables in the join expression
        // refer to not less than two leaf inputs.
        if (joinLeafInput0 == null || joinLeafInput1 == null) {
            return false;
        }

        ILogicalOperator innerLeafInput = this.leafInput;

        // This must equal one of the two joinLeafInputsHashMap found above. check for sanity!!
        if (innerLeafInput != joinLeafInput0 && innerLeafInput != joinLeafInput1) {
            return false; // This should not happen. So debug to find out why this happened.
        }

        if (innerLeafInput == joinLeafInput0) {
            joinEnum.localJoinOp.getInputs().get(0).setValue(joinLeafInput1);
        } else {
            joinEnum.localJoinOp.getInputs().get(0).setValue(joinLeafInput0);
        }

        joinEnum.localJoinOp.getInputs().get(1).setValue(innerLeafInput);

        // We will always use the first join Op to provide the joinOp input for invoking rewritePre
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinEnum.localJoinOp;
        joinOp.getCondition().setValue(joinExpr);

        // Now call the rewritePre code
        IntroduceJoinAccessMethodRule tmp = new IntroduceJoinAccessMethodRule();
        boolean retVal = tmp.checkApplicable(new MutableObject<>(joinEnum.localJoinOp), joinEnum.optCtx, chosenIndexes,
                analyzedAMs);

        return retVal;
    }

    private boolean NLJoinApplicable(JoinNode leftJn, JoinNode rightJn, boolean outerJoin,
            ILogicalExpression nestedLoopJoinExpr, List<Pair<IAccessMethod, Index>> chosenIndexes,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) throws AlgebricksException {
        if (nullExtendingSide(leftJn.datasetBits, outerJoin)) {
            return false;
        }

        if (rightJn.jnArrayIndex > joinEnum.numberOfTerms) {
            // right side consists of more than one table
            return false; // nested loop plan not possible.
        }

        if (nestedLoopJoinExpr == null
                || !rightJn.nestedLoopsApplicable(nestedLoopJoinExpr, outerJoin, chosenIndexes, analyzedAMs)) {
            return false;
        }

        return true;
    }

    private boolean CPJoinApplicable(JoinNode leftJn, boolean outerJoin) {
        if (!joinEnum.cboCPEnumMode) {
            return false;
        }

        if (nullExtendingSide(leftJn.datasetBits, outerJoin)) {
            return false;
        }

        return true;
    }

    protected int buildHashJoinPlan(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression hashJoinExpr,
            HashJoinExpressionAnnotation hintHashJoin, boolean outerJoin) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();
        PlanNode pn;
        ICost hjCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;
        this.leftJn = leftJn;
        this.rightJn = rightJn;

        if (!hashJoinApplicable(leftJn, outerJoin, hashJoinExpr)) {
            return PlanNode.NO_PLAN;
        }

        boolean forceEnum = hintHashJoin != null || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG) || outerJoin
                || level <= joinEnum.cboFullEnumLevel;
        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || forceEnum) {
            // We want to build with the smaller side.
            hjCost = joinEnum.getCostMethodsHandle().costHashJoin(this);
            leftExchangeCost = joinEnum.getCostMethodsHandle().computeHJProbeExchangeCost(this);
            rightExchangeCost = joinEnum.getCostMethodsHandle().computeHJBuildExchangeCost(this);
            childCosts = allPlans.get(leftPlan.allPlansIndex).totalCost
                    .costAdd(allPlans.get(rightPlan.allPlansIndex).totalCost);
            totalCost = hjCost.costAdd(leftExchangeCost).costAdd(rightExchangeCost).costAdd(childCosts);
            if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost) || forceEnum) {
                pn = new PlanNode(allPlans.size(), joinEnum, this, leftPlan, rightPlan, outerJoin);
                pn.setJoinAndHintInfo(PlanNode.JoinMethod.HYBRID_HASH_JOIN, hashJoinExpr, null,
                        HashJoinExpressionAnnotation.BuildSide.RIGHT, hintHashJoin);
                pn.setJoinCosts(hjCost, totalCost, leftExchangeCost, rightExchangeCost);
                planIndexesArray.add(pn.allPlansIndex);
                allPlans.add(pn);
                setCheapestPlan(pn, forceEnum);
                return pn.allPlansIndex;
            }
        }
        return PlanNode.NO_PLAN;
    }

    private int buildBroadcastHashJoinPlan(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression hashJoinExpr,
            BroadcastExpressionAnnotation hintBroadcastHashJoin, boolean outerJoin) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();
        PlanNode pn;
        ICost bcastHjCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        if (!hashJoinApplicable(leftJn, outerJoin, hashJoinExpr)) {
            return PlanNode.NO_PLAN;
        }

        boolean forceEnum = hintBroadcastHashJoin != null || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG) || outerJoin
                || level <= joinEnum.cboFullEnumLevel;
        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || forceEnum) {
            // We want to broadcast and build with the smaller side.
            bcastHjCost = joinEnum.getCostMethodsHandle().costBroadcastHashJoin(this);
            leftExchangeCost = joinEnum.getCostHandle().zeroCost();
            rightExchangeCost = joinEnum.getCostMethodsHandle().computeBHJBuildExchangeCost(this);
            childCosts = allPlans.get(leftPlan.allPlansIndex).totalCost
                    .costAdd(allPlans.get(rightPlan.allPlansIndex).totalCost);
            totalCost = bcastHjCost.costAdd(rightExchangeCost).costAdd(childCosts);
            if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost) || forceEnum) {
                pn = new PlanNode(allPlans.size(), joinEnum, this, leftPlan, rightPlan, outerJoin);
                pn.setJoinAndHintInfo(PlanNode.JoinMethod.BROADCAST_HASH_JOIN, hashJoinExpr, null,
                        HashJoinExpressionAnnotation.BuildSide.RIGHT, hintBroadcastHashJoin);
                pn.setJoinCosts(bcastHjCost, totalCost, leftExchangeCost, rightExchangeCost);
                planIndexesArray.add(pn.allPlansIndex);
                allPlans.add(pn);
                setCheapestPlan(pn, forceEnum);
                return pn.allPlansIndex;
            }
        }
        return PlanNode.NO_PLAN;
    }

    private int buildNLJoinPlan(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression nestedLoopJoinExpr,
            IndexedNLJoinExpressionAnnotation hintNLJoin, boolean outerJoin) throws AlgebricksException {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        // Build a nested loops plan, first check if it is possible
        // left right order must be preserved and right side should be a single data set
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost nljCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        if (!NLJoinApplicable(leftJn, rightJn, outerJoin, nestedLoopJoinExpr, chosenIndexes, analyzedAMs)) {
            return PlanNode.NO_PLAN;
        }
        if (chosenIndexes.isEmpty()) {
            return PlanNode.NO_PLAN;
        }

        Pair<AbstractFunctionCallExpression, IndexedNLJoinExpressionAnnotation> exprAndHint = new Pair<>(null, null);
        nljCost = joinEnum.getCostHandle().maxCost();
        ICost curNljCost;
        for (Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry : analyzedAMs.entrySet()) {
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                    analysisCtx.getIteratorForIndexExprsAndVars();
            List<IOptimizableFuncExpr> exprs = analysisCtx.getMatchedFuncExprs();
            while (indexIt.hasNext()) {
                Collection<String> indexNames = new ArrayList<>();
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
                Index index = indexEntry.getKey();
                AbstractFunctionCallExpression afce = buildExpr(exprs, indexEntry.getValue());
                curNljCost = joinEnum.getCostMethodsHandle().costIndexNLJoin(this, index);
                if (curNljCost.costLE(nljCost)) {
                    nljCost = curNljCost;
                    indexNames.add(index.getIndexName());
                    exprAndHint = new Pair<>(afce, IndexedNLJoinExpressionAnnotation.newInstance(indexNames));
                }
            }
        }
        if (exprAndHint.first == null) {
            return PlanNode.NO_PLAN;
        }
        leftExchangeCost = joinEnum.getCostMethodsHandle().computeNLJOuterExchangeCost(this);
        rightExchangeCost = joinEnum.getCostHandle().zeroCost();
        childCosts = allPlans.get(leftPlan.allPlansIndex).totalCost;
        totalCost = nljCost.costAdd(leftExchangeCost).costAdd(childCosts);
        boolean forceEnum =
                hintNLJoin != null || joinEnum.forceJoinOrderMode || outerJoin || level <= joinEnum.cboFullEnumLevel;
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost) || forceEnum) {
            pn = new PlanNode(allPlans.size(), joinEnum, this, leftPlan, rightPlan, outerJoin);
            pn.setJoinAndHintInfo(PlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN, nestedLoopJoinExpr, exprAndHint, null,
                    hintNLJoin);
            pn.setJoinCosts(nljCost, totalCost, leftExchangeCost, rightExchangeCost);
            planIndexesArray.add(pn.allPlansIndex);
            allPlans.add(pn);
            setCheapestPlan(pn, forceEnum);
            return pn.allPlansIndex;
        }
        return PlanNode.NO_PLAN;
    }

    private int buildCPJoinPlan(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression hashJoinExpr,
            ILogicalExpression nestedLoopJoinExpr, boolean outerJoin) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        // Now build a cartesian product nested loops plan
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost cpCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        if (!CPJoinApplicable(leftJn, outerJoin)) {
            return PlanNode.NO_PLAN;
        }

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        ILogicalExpression cpJoinExpr;
        List<Integer> newJoinConditions = this.getNewJoinConditionsOnly();
        if (hashJoinExpr == null && nestedLoopJoinExpr == null) {
            cpJoinExpr = joinEnum.combineAllConditions(newJoinConditions);
        } else if (hashJoinExpr != null && nestedLoopJoinExpr == null) {
            cpJoinExpr = hashJoinExpr;
        } else if (hashJoinExpr == null) {
            cpJoinExpr = nestedLoopJoinExpr;
        } else if (Objects.equals(hashJoinExpr, nestedLoopJoinExpr)) {
            cpJoinExpr = hashJoinExpr;
        } else {
            ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));
            andExpr.getArguments().add(new MutableObject<>(hashJoinExpr));
            andExpr.getArguments().add(new MutableObject<>(nestedLoopJoinExpr));
            cpJoinExpr = andExpr;
        }

        cpCost = joinEnum.getCostMethodsHandle().costCartesianProductJoin(this);
        leftExchangeCost = joinEnum.getCostHandle().zeroCost();
        rightExchangeCost = joinEnum.getCostMethodsHandle().computeCPRightExchangeCost(this);
        childCosts =
                allPlans.get(leftPlan.allPlansIndex).totalCost.costAdd(allPlans.get(rightPlan.allPlansIndex).totalCost);
        totalCost = cpCost.costAdd(rightExchangeCost).costAdd(childCosts);
        boolean forceEnum = joinEnum.forceJoinOrderMode || outerJoin || level <= joinEnum.cboFullEnumLevel;
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost) || forceEnum) {
            pn = new PlanNode(allPlans.size(), joinEnum, this, leftPlan, rightPlan, outerJoin);
            pn.setJoinAndHintInfo(PlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN,
                    Objects.requireNonNullElse(cpJoinExpr, ConstantExpression.TRUE), null, null, null);
            pn.setJoinCosts(cpCost, totalCost, leftExchangeCost, rightExchangeCost);
            planIndexesArray.add(pn.allPlansIndex);
            allPlans.add(pn);
            setCheapestPlan(pn, forceEnum);
            return pn.allPlansIndex;
        }
        return PlanNode.NO_PLAN;
    }

    protected void addMultiDatasetPlans(JoinNode leftJn, JoinNode rightJn) throws AlgebricksException {
        PlanNode leftPlan, rightPlan;

        if (level > joinEnum.cboFullEnumLevel) {
            // FOR JOIN NODE LEVELS GREATER THAN THE LEVEL SPECIFIED FOR FULL ENUMERATION,
            // DO NOT DO FULL ENUMERATION => PRUNE
            if (leftJn.cheapestPlanIndex == PlanNode.NO_PLAN || rightJn.cheapestPlanIndex == PlanNode.NO_PLAN) {
                return;
            }
            leftPlan = joinEnum.allPlans.get(leftJn.cheapestPlanIndex);
            rightPlan = joinEnum.allPlans.get(rightJn.cheapestPlanIndex);
            addMultiDatasetPlans(leftPlan, rightPlan);
        } else {
            // FOR JOIN NODE LEVELS LESS THAN OR EQUAL TO THE LEVEL SPECIFIED FOR FULL ENUMERATION,
            // DO FULL ENUMERATION => DO NOT PRUNE
            for (int leftPlanIndex : leftJn.planIndexesArray) {
                leftPlan = joinEnum.allPlans.get(leftPlanIndex);
                for (int rightPlanIndex : rightJn.planIndexesArray) {
                    rightPlan = joinEnum.allPlans.get(rightPlanIndex);
                    addMultiDatasetPlans(leftPlan, rightPlan);
                }
            }
        }
    }

    protected void addMultiDatasetPlans(PlanNode leftPlan, PlanNode rightPlan) throws AlgebricksException {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        if (leftJn.planIndexesArray.size() == 0 || rightJn.planIndexesArray.size() == 0) {
            return;
        }

        if (this.cardinality >= Cost.MAX_CARD) {
            return; // no card available, so do not add this plan
        }

        if (leftJn.cheapestPlanIndex == PlanNode.NO_PLAN || rightJn.cheapestPlanIndex == PlanNode.NO_PLAN) {
            return;
        }

        List<Integer> newJoinConditions = this.getNewJoinConditionsOnly(); // these will be a subset of applicable join conditions.
        if ((newJoinConditions.size() == 0) && joinEnum.connectedJoinGraph) {
            // at least one plan must be there at each level as the graph is fully connected.
            if (leftJn.cardinality * rightJn.cardinality > 10000.0 && level > joinEnum.cboFullEnumLevel) {
                return;
            }
        }
        ILogicalExpression hashJoinExpr = joinEnum.getHashJoinExpr(newJoinConditions);
        ILogicalExpression nestedLoopJoinExpr = joinEnum.getNestedLoopJoinExpr(newJoinConditions);
        boolean outerJoin = joinEnum.lookForOuterJoins(newJoinConditions);

        double current_card = this.cardinality;
        if (current_card >= Cost.MAX_CARD) {
            return; // no card available, so do not add this plan
        }

        if (leftJn.distinctCardinality == 0.0) { // no group-by/distinct attribute(s) from the left node
            this.distinctCardinality = rightJn.distinctCardinality;
        } else if (rightJn.distinctCardinality == 0.0) { // no group-by/distinct attributes(s) from the right node
            this.distinctCardinality = leftJn.distinctCardinality;
        } else {
            // Heuristic used to propagate distinct cardinalities to join nodes.
            // D_est_jn = (D_est_l > D_est_r) ? (D_est_l * join productivity of leftJn) : (D_est_r * join productivity of rightJn)
            double leftJnCard, rightJnCard;
            leftJnCard = (leftJn.IsBaseLevelJoinNode()) ? leftJn.getOrigCardinality() : leftJn.getCardinality();
            rightJnCard = (rightJn.IsBaseLevelJoinNode()) ? rightJn.getOrigCardinality() : rightJn.getCardinality();
            if (leftJn.distinctCardinality > rightJn.distinctCardinality) {
                this.distinctCardinality = leftJn.distinctCardinality * this.cardinality / leftJnCard;
            } else {
                this.distinctCardinality = rightJn.distinctCardinality * this.cardinality / rightJnCard;
            }
            this.distinctCardinality = (double) Math.round(this.distinctCardinality * 100) / 100;
        }

        boolean validPlan = false;
        HashJoinExpressionAnnotation hintHashJoin = joinEnum.findHashJoinHint(newJoinConditions);
        BroadcastExpressionAnnotation hintBroadcastHashJoin = joinEnum.findBroadcastHashJoinHint(newJoinConditions);
        IndexedNLJoinExpressionAnnotation hintNLJoin = joinEnum.findNLJoinHint(newJoinConditions);

        if (hintHashJoin != null) {
            validPlan =
                    buildHintedHJPlans(leftPlan, rightPlan, hashJoinExpr, hintHashJoin, outerJoin, newJoinConditions);
        } else if (hintBroadcastHashJoin != null) {
            validPlan = buildHintedBcastHJPlans(leftPlan, rightPlan, hashJoinExpr, hintBroadcastHashJoin, outerJoin,
                    newJoinConditions);
        } else if (hintNLJoin != null) {
            validPlan = buildHintedNLJPlans(leftPlan, rightPlan, nestedLoopJoinExpr, hintNLJoin, outerJoin,
                    newJoinConditions);
        }

        if (!validPlan) {
            // No join hints or inapplicable hinted plans, try all non hinted plans.
            buildAllNonHintedPlans(leftPlan, rightPlan, hashJoinExpr, nestedLoopJoinExpr, outerJoin);
        }

        //Reset as these might have changed when we tried the commutative joins.
        this.leftJn = leftJn;
        this.rightJn = rightJn;
    }

    private boolean buildHintedHJPlans(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression hashJoinExpr,
            HashJoinExpressionAnnotation hintHashJoin, boolean outerJoin, List<Integer> newJoinConditions) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        int hjPlan, commutativeHjPlan;
        hjPlan = commutativeHjPlan = PlanNode.NO_PLAN;
        boolean build = (hintHashJoin.getBuildOrProbe() == HashJoinExpressionAnnotation.BuildOrProbe.BUILD);
        boolean probe = (hintHashJoin.getBuildOrProbe() == HashJoinExpressionAnnotation.BuildOrProbe.PROBE);
        boolean validBuildOrProbeObject = false;
        String buildOrProbeObject = hintHashJoin.getName();
        if (buildOrProbeObject != null && (rightJn.datasetNames.contains(buildOrProbeObject)
                || rightJn.aliases.contains(buildOrProbeObject) || leftJn.datasetNames.contains(buildOrProbeObject)
                || leftJn.aliases.contains(buildOrProbeObject))) {
            validBuildOrProbeObject = true;
        }
        if (validBuildOrProbeObject) {
            if ((build && (rightJn.datasetNames.contains(buildOrProbeObject)
                    || rightJn.aliases.contains(buildOrProbeObject)))
                    || (probe && (leftJn.datasetNames.contains(buildOrProbeObject)
                            || leftJn.aliases.contains(buildOrProbeObject)))) {
                hjPlan = buildHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, hintHashJoin, outerJoin);
            } else if ((build && (leftJn.datasetNames.contains(buildOrProbeObject)
                    || leftJn.aliases.contains(buildOrProbeObject)))
                    || (probe && (rightJn.datasetNames.contains(buildOrProbeObject)
                            || rightJn.aliases.contains(buildOrProbeObject)))) {
                commutativeHjPlan = buildHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, hintHashJoin, outerJoin);
            }
        }

        return handleHints(hjPlan, commutativeHjPlan, hintHashJoin, newJoinConditions);
    }

    private boolean buildHintedBcastHJPlans(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression hashJoinExpr,
            BroadcastExpressionAnnotation hintBroadcastHashJoin, boolean outerJoin, List<Integer> newJoinConditions) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();
        int bcastHjPlan, commutativeBcastHjPlan;
        bcastHjPlan = commutativeBcastHjPlan = PlanNode.NO_PLAN;
        boolean validBroadcastObject = false;
        String broadcastObject = hintBroadcastHashJoin.getName();
        if (broadcastObject != null
                && (rightJn.datasetNames.contains(broadcastObject) || rightJn.aliases.contains(broadcastObject)
                        || leftJn.datasetNames.contains(broadcastObject) || leftJn.aliases.contains(broadcastObject))) {
            validBroadcastObject = true;
        }
        if (validBroadcastObject) {
            if (rightJn.datasetNames.contains(broadcastObject) || rightJn.aliases.contains(broadcastObject)) {
                bcastHjPlan =
                        buildBroadcastHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, hintBroadcastHashJoin, outerJoin);
            } else if (leftJn.datasetNames.contains(broadcastObject) || leftJn.aliases.contains(broadcastObject)) {
                commutativeBcastHjPlan =
                        buildBroadcastHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, hintBroadcastHashJoin, outerJoin);
            }
        } else if (broadcastObject == null) {
            bcastHjPlan =
                    buildBroadcastHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, hintBroadcastHashJoin, outerJoin);
            if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
                commutativeBcastHjPlan =
                        buildBroadcastHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, hintBroadcastHashJoin, outerJoin);
            }
        }

        return handleHints(bcastHjPlan, commutativeBcastHjPlan, hintBroadcastHashJoin, newJoinConditions);
    }

    private boolean buildHintedNLJPlans(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression nestedLoopJoinExpr,
            IndexedNLJoinExpressionAnnotation hintNLJoin, boolean outerJoin, List<Integer> newJoinConditions)
            throws AlgebricksException {
        int nljPlan, commutativeNljPlan;
        nljPlan = commutativeNljPlan = PlanNode.NO_PLAN;
        nljPlan = buildNLJoinPlan(leftPlan, rightPlan, nestedLoopJoinExpr, hintNLJoin, outerJoin);

        // The indexnl hint may have been removed during applicability checking
        // and is no longer available for a hintedNL plan.
        if (joinEnum.findNLJoinHint(newJoinConditions) == null) {
            return false;
        }
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            commutativeNljPlan = buildNLJoinPlan(rightPlan, leftPlan, nestedLoopJoinExpr, hintNLJoin, outerJoin);
            // The indexnl hint may have been removed during applicability checking
            // and is no longer available for a hintedNL plan.
            if (joinEnum.findNLJoinHint(newJoinConditions) == null) {
                return false;
            }
        }

        return handleHints(nljPlan, commutativeNljPlan, hintNLJoin, newJoinConditions);
    }

    private boolean handleHints(int plan, int commutativePlan, IExpressionAnnotation hint,
            List<Integer> newJoinConditions) {
        if (plan != PlanNode.NO_PLAN || commutativePlan != PlanNode.NO_PLAN) {
            // The hint has been applied to produce a valid plan.
            joinEnum.joinHints.put(hint, null);
            return true;
        }

        // No hinted plan, issue an inapplicable hint warning if the hint
        // has not been applied previously.
        if (!(joinEnum.joinHints.containsKey(hint) && joinEnum.joinHints.get(hint) == null)) {
            inapplicableHintWarning(hint, newJoinConditions);
            return false; // This will trigger enumeration of all non-hinted plans.
        }

        // The hint has been applied previously, do not enumerate all non-hinted plans.
        return true;
    }

    private void buildAllNonHintedPlans(PlanNode leftPlan, PlanNode rightPlan, ILogicalExpression hashJoinExpr,
            ILogicalExpression nestedLoopJoinExpr, boolean outerJoin) throws AlgebricksException {

        buildHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, null, outerJoin);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, null, outerJoin);
        }
        buildBroadcastHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, null, outerJoin);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildBroadcastHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, null, outerJoin);
        }
        buildNLJoinPlan(leftPlan, rightPlan, nestedLoopJoinExpr, null, outerJoin);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildNLJoinPlan(rightPlan, leftPlan, nestedLoopJoinExpr, null, outerJoin);
        }
        buildCPJoinPlan(leftPlan, rightPlan, hashJoinExpr, nestedLoopJoinExpr, outerJoin);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildCPJoinPlan(rightPlan, leftPlan, hashJoinExpr, nestedLoopJoinExpr, outerJoin);
        }
    }

    private void inapplicableHintWarning(IExpressionAnnotation hint, List<Integer> newJoinConditions) {
        HashJoinExpressionAnnotation hintHashJoin;
        BroadcastExpressionAnnotation hintBroadcastHashJoin;

        String param1 = "";
        String param2 = "";

        if (hint instanceof HashJoinExpressionAnnotation) {
            hintHashJoin = (HashJoinExpressionAnnotation) hint;
            boolean build = (hintHashJoin.getBuildOrProbe() == HashJoinExpressionAnnotation.BuildOrProbe.BUILD);
            String buildOrProbeObject = hintHashJoin.getName();
            param1 = "hash join";
            param2 = (build ? "build " : "probe ") + "with " + buildOrProbeObject;
        } else if (hint instanceof BroadcastExpressionAnnotation) {
            hintBroadcastHashJoin = (BroadcastExpressionAnnotation) hint;
            String broadcastObject = hintBroadcastHashJoin.getName();
            param1 = "broadcast hash join";
            param2 = "broadcast " + broadcastObject == null ? "" : broadcastObject;
        } else if (hint instanceof IndexedNLJoinExpressionAnnotation) {
            param1 = "index nested loop join";
            param2 = "ignored";
        }

        if (!(joinEnum.joinHints.containsKey(hint) && joinEnum.joinHints.get(hint) == null)) {
            // No valid hinted plans were built, issue a warning.
            // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
            if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                joinEnum.joinHints.put(hint, Warning.of(
                        joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition.getSourceLocation(),
                        ErrorCode.INAPPLICABLE_HINT, param1, param2));
            }
        }
    }

    private PlanNode findCheapestPlan() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost cheapestCost = joinEnum.getCostHandle().maxCost();
        PlanNode cheapestPlanNode = null;
        int numHintsUsedInCheapestPlan = 0;

        for (int planIndex : this.planIndexesArray) {
            PlanNode plan = allPlans.get(planIndex);
            if (plan.numHintsUsed > numHintsUsedInCheapestPlan) {
                // "plan" has used more hints than "cheapestPlan", "plan" wins!
                cheapestPlanNode = plan;
                cheapestCost = plan.totalCost;
                numHintsUsedInCheapestPlan = plan.numHintsUsed;
            } else if (plan.numHintsUsed == numHintsUsedInCheapestPlan) {
                // Either both "plan" and "cheapestPlan" are hinted, or both are non-hinted.
                // Cost is the decider.
                if (plan.totalCost.costLT(cheapestCost)) {
                    cheapestPlanNode = plan;
                    cheapestCost = plan.totalCost;
                }
            } else {
                // This is the case where "plan" has used fewer hints than "cheapestPlan".
                // We have already captured the cheapest plan, nothing to do.
            }
        }
        return cheapestPlanNode;
    }

    private void setCheapestPlan(PlanNode pn, boolean forceEnum) {
        PlanNode cheapestPlan = forceEnum ? findCheapestPlan() : pn;
        cheapestPlanCost = cheapestPlan.totalCost;
        cheapestPlanIndex = cheapestPlan.allPlansIndex;
        cheapestPlanNode = cheapestPlan;
    }

    @Override
    public String toString() {
        List<PlanNode> allPlans = joinEnum.getAllPlans();
        StringBuilder sb = new StringBuilder(128);
        if (IsBaseLevelJoinNode()) {
            sb.append("Printing Scan Node ");
        } else {
            sb.append("Printing Join Node ");
        }
        sb.append(jnArrayIndex).append('\n');
        sb.append("datasetNames (aliases) ");
        for (int j = 0; j < datasetNames.size(); j++) {
            sb.append(datasetNames.get(j)).append('(').append(aliases.get(j)).append(')').append(' ');
        }
        sb.append('\n');
        sb.append("datasetIndexes ");
        for (int j = 0; j < datasetIndexes.size(); j++) {
            sb.append(j).append(datasetIndexes.get(j)).append(' ');
        }
        sb.append('\n');
        sb.append("datasetBits ").append(datasetBits).append('\n');
        sb.append("jnIndex ").append(jnIndex).append('\n');
        sb.append("level ").append(level).append('\n');
        sb.append("highestDatasetId ").append(highestDatasetId).append('\n');
        if (IsBaseLevelJoinNode()) {
            sb.append("orig cardinality ").append(dumpDouble(origCardinality));
        }
        sb.append("cardinality ").append(dumpDouble(cardinality));
        sb.append("size ").append(dumpDouble(size));
        sb.append("outputSize(sizeVarsAfterScan) ").append(dumpDouble(sizeVarsAfterScan));
        if (planIndexesArray.size() == 0) {
            sb.append("No plans considered for this join node").append('\n');
        } else {
            for (int j = 0; j < planIndexesArray.size(); j++) {
                int k = planIndexesArray.get(j);
                PlanNode pn = allPlans.get(k);
                sb.append("\nPrinting Plan ").append(k).append('\n');
                sb.append("planIndexesArray [").append(j).append("] ").append(k).append('\n');
                if (IsBaseLevelJoinNode()) {
                    if (pn.IsScanNode()) {
                        if (pn.getScanOp() == PlanNode.ScanMethod.TABLE_SCAN) {
                            sb.append("DATA_SOURCE_SCAN").append('\n');
                        } else {
                            sb.append("INDEX_SCAN").append('\n');
                        }
                    }
                } else {
                    sb.append(pn.joinMethod().getFirst()).append('\n');
                    sb.append("Join expr ");
                    if (pn.joinExpr != null) {
                        sb.append(pn.joinExpr).append('\n');
                        sb.append("outer join " + pn.outerJoin).append('\n');
                    } else {
                        sb.append("null").append('\n');
                    }
                }
                sb.append("operator cost ").append(dumpCost(pn.getOpCost()));
                sb.append("total cost ").append(dumpCost(pn.getTotalCost()));
                sb.append("jnIndexes ").append(pn.jnIndexes[0]).append(" ").append(pn.jnIndexes[1]).append('\n');
                if (IsHigherLevelJoinNode()) {
                    PlanNode leftPlan = pn.getLeftPlanNode();
                    PlanNode rightPlan = pn.getRightPlanNode();
                    sb.append("planIndexes ").append(leftPlan.getIndex()).append(" ").append(rightPlan.getIndex())
                            .append('\n');
                    sb.append(dumpLeftRightPlanCosts(pn));
                }
            }
            if (cheapestPlanIndex != PlanNode.NO_PLAN) {
                sb.append("\nCheapest plan for JoinNode ").append(jnArrayIndex).append(" is ").append(cheapestPlanIndex)
                        .append(", cost is ")
                        .append(dumpDouble(allPlans.get(cheapestPlanIndex).getTotalCost().computeTotalCost()));
            }
        }
        sb.append("-----------------------------------------------------------------").append('\n');
        return sb.toString();
    }

    protected String dumpCost(ICost cost) {
        StringBuilder sb = new StringBuilder(128);
        sb.append((double) Math.round(cost.computeTotalCost() * 100) / 100).append('\n');
        return sb.toString();
    }

    protected String dumpDouble(double val) {
        StringBuilder sb = new StringBuilder(128);
        sb.append((double) Math.round(val * 100) / 100).append('\n');
        return sb.toString();
    }

    protected String dumpDoubleNoNewline(double val) {
        StringBuilder sb = new StringBuilder(128);
        sb.append((double) Math.round(val * 100) / 100);
        return sb.toString();
    }

    protected String dumpLeftRightPlanCosts(PlanNode pn) {
        StringBuilder sb = new StringBuilder(128);
        PlanNode leftPlan = pn.getLeftPlanNode();
        PlanNode rightPlan = pn.getRightPlanNode();
        ICost leftPlanTotalCost = leftPlan.getTotalCost();
        ICost rightPlanTotalCost = rightPlan.getTotalCost();

        sb.append("  left plan cost ").append(dumpDoubleNoNewline(leftPlanTotalCost.computeTotalCost()))
                .append(", right plan cost ").append(dumpDouble(rightPlanTotalCost.computeTotalCost()));
        return sb.toString();
    }

    protected void printCostOfAllPlans(StringBuilder sb) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost minCost = joinEnum.getCostHandle().maxCost();
        int lowestCostPlanIndex = 0;
        for (int planIndex : planIndexesArray) {
            ICost planCost = allPlans.get(planIndex).totalCost;
            sb.append("plan ").append(planIndex).append(" cost is ").append(dumpDouble(planCost.computeTotalCost()));
            if (planCost.costLT(minCost)) {
                minCost = planCost;
                lowestCostPlanIndex = planIndex;
            }
        }
        sb.append("Cheapest Plan is ").append(lowestCostPlanIndex).append(", Cost is ")
                .append(dumpDouble(minCost.computeTotalCost()));
    }
}
