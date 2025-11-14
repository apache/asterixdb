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

import static org.apache.asterix.optimizer.rules.cbo.DatasetRegistry.isSubset;

import java.text.DecimalFormat;
import java.text.NumberFormat;
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
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.declared.SampleDataSource;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.cost.Cost;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.asterix.optimizer.rules.am.AbstractIntroduceAccessMethodRule;
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
    public DatasetRegistry.DatasetSubset datasetSubset; // this is bitmap of all the keyspaceBits present in this joinNode
    protected List<Integer> datasetIndexes;
    protected List<String> datasetNames;
    protected List<String> aliases;
    protected int cheapestPlanIndex;
    protected AbstractPlanNode cheapestPlanNode;
    private ICost cheapestPlanCost;
    protected double origCardinality; // without any selections
    protected double cardinality;
    protected double size; // avg size of whole document; available from the sample
    protected double unnestFactor;
    protected double diskProjectionSize; // what is coming out of the disk; in case of row format, it is the entire document
                                         // in case of columnar we need to add sizes of individual fields.
    protected double projectionSizeAfterScan; // excludes fields only used for selections
    protected double distinctCardinality; // estimated distinct cardinality for this joinNode
    protected PlanRegistry planRegistry;
    protected int jnIndex;
    protected int level;
    protected int highestDatasetId;
    private JoinNode rightJn;
    private JoinNode leftJn;
    private int limitVal = -1; // only for single dataset joinNodes.
    private List<Integer> applicableJoinConditions;
    protected AbstractLeafInput leafInput;
    protected Index.SampleIndexDetails idxDetails;
    private List<IndexCostInfo> indexCostInfoList;
    // The triple above is : Index, selectivity, and the index expression
    protected static int NO_JN = -1;
    protected static JoinNode DUMMY_JN = new JoinNode(NO_JN);
    private static int NO_CARDS = -2;
    private int numVarsFromDisk = -1; // number of variables projected from disk
    private int numVarsAfterScan = -1; // number of variables after all selection fields have been removed and are needed for joins and final projects
    private double sizeVarsFromDisk = -1.0;
    private double sizeVarsAfterScan = -1.0;
    private boolean columnar = true; // default
    private boolean fake = false; // default; Fake will be set to true when we introduce fake leafInputs for unnesting arrays.
    private int leafInputNumber = -1; // this field and the next are used for Array Unnest ops.
    private int arrayRef = -1;

    private JoinNode(int i) {
        this.jnArrayIndex = i;
        planRegistry = new PlanRegistry();
        cheapestPlanNode = DummyPlanNode.INSTANCE;
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

    public PlanRegistry getPlanRegistry() {
        return planRegistry;
    }

    protected void setCardinality(double card, boolean setMinCard) {
        // Minimum cardinality for operators is MIN_CARD to prevent bad plans due to cardinality under estimation errors.
        cardinality = setMinCard ? Math.max(card, Cost.MIN_CARD) : card;
    }

    public double getOrigCardinality() {
        return origCardinality;
    }

    public AbstractPlanNode getCheapestPlanNode() {
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

    public double getUnnestFactor() {
        return unnestFactor;
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

    public void setFake() {
        fake = true;
    }

    public boolean getFake() {
        return fake;
    }

    public void setLeafInputNumber(int inputNumber) {
        leafInputNumber = inputNumber;
    }

    public int getLeafInputNumber() {
        return leafInputNumber;
    }

    public void setArrayRef(int arrayRef) {
        this.arrayRef = arrayRef;
    }

    public int getArrayRef() {
        return arrayRef;
    }

    protected void setCardsAndSizesForFakeJn(int leafInputNumber, int arrayRef, double unnestFactor) {
        joinEnum.jnArray[leafInputNumber + arrayRef].setOrigCardinality(unnestFactor, false);
        joinEnum.jnArray[leafInputNumber + arrayRef].setCardinality(unnestFactor, false);
        joinEnum.jnArray[leafInputNumber + arrayRef].setSizeVarsFromDisk(4);
        joinEnum.jnArray[leafInputNumber + arrayRef].setSizeVarsAfterScan(4);
        joinEnum.jnArray[leafInputNumber + arrayRef].setAvgDocSize(4);
        joinEnum.jnArray[leafInputNumber + arrayRef].setLeafInputNumber(leafInputNumber);
        joinEnum.jnArray[leafInputNumber + arrayRef].setArrayRef(arrayRef);
    }

    public void setCardsAndSizes(Index.SampleIndexDetails idxDetails, ILogicalOperator leafInput, int leafInputNumber)
            throws AlgebricksException {

        //double origDatasetCard, finalDatasetCard, sampleCard1, sampleCard2;
        double origDatasetCard, finalDatasetCard, sampleCard;
        unnestFactor = 1.0;

        int numArrayRefs = 0;
        if (joinEnum.unnestOpsInfo.size() > 0) {
            numArrayRefs = joinEnum.unnestOpsInfo.get(leafInputNumber - 1).size();
        }

        DataSourceScanOperator scanOp = EnumerateJoinsRule.findDataSourceScanOperator(leafInput);
        if (scanOp == null) {
            return; // what happens to the cards and sizes then? this may happen in case of in lists
        }

        List<List<IAObject>> result;
        SelectOperator selOp = (SelectOperator) joinEnum.findASelectOp(leafInput);

        if (selOp == null) { // this should not happen. So check later why this happening.
            // add a SelectOperator with TRUE condition. The code below becomes simpler with a select operator.
            selOp = new SelectOperator(new MutableObject<>(ConstantExpression.TRUE));
            ILogicalOperator op = selOp;
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
        finalDatasetCard = origDatasetCard = idxDetails.getSourceCardinality();
        sampleCard = Math.min(idxDetails.getSampleCardinalityTarget(), origDatasetCard);
        for (int i = 1; i <= numArrayRefs; i++) {
            sampleCard = Math.min(idxDetails.getSampleCardinalityTarget(), origDatasetCard);
            ILogicalExpression saveExpr = selOp.getCondition().getValue();
            double unnestSampleCard =
                    joinEnum.stats.computeUnnestedOriginalCardinality(leafInput, leafInputNumber, numArrayRefs, i);
            selOp.getCondition().setValue(saveExpr); // restore the expression
            unnestFactor = unnestSampleCard / sampleCard;
            setCardsAndSizesForFakeJn(leafInputNumber, i, unnestFactor);
            finalDatasetCard = origDatasetCard;
            removeUnnestOp(leafInput); // remove the unnest op that was added in computeUnnestedOriginalCardinality
        }
        if (sampleCard == 0) { // should not happen unless the original dataset is empty
            sampleCard = 1; // we may have to make some adjustments to costs when the sample returns very rows.

            IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(scanOp.getSourceLocation(),
                        org.apache.asterix.common.exceptions.ErrorCode.SAMPLE_HAS_ZERO_ROWS));
            }
        }

        // There are predicates here. So skip the predicates and get the original dataset card.
        // Now apply all the predicates and get the card after all predicates are applied.
        // We call the sampling query even if a selectivity hint was provided because we have to get the lengths of the variables.
        result = joinEnum.getStatsHandle().runSamplingQueryProjection(joinEnum.optCtx, leafInput, jnArrayIndex,
                primaryKey);
        double predicateCardinalityFromSample = joinEnum.getStatsHandle().findPredicateCardinality(result, true);

        double sizeVarsFromDisk;
        double sizeVarsAfterScan;

        if (predicateCardinalityFromSample > 0.0) { // otherwise, we get nulls for the averages
            sizeVarsFromDisk = joinEnum.getStatsHandle().findSizeVarsFromDisk(result, getNumVarsFromDisk());
            sizeVarsAfterScan = joinEnum.getStatsHandle().findSizeVarsAfterScan(result, getNumVarsFromDisk());
        } else { // in case we did not get any tuples from the sample, get the size by setting the predicate to true.
            ILogicalExpression saveExpr = selOp.getCondition().getValue();
            selOp.getCondition().setValue(ConstantExpression.TRUE);
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
            selOp.getCondition().setValue(saveExpr); // restore the expression
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
        setOrigCardinality(origDatasetCard, false);

        setSizeVarsFromDisk(sizeVarsFromDisk);
        setSizeVarsAfterScan(sizeVarsAfterScan);
        setAvgDocSize(idxDetails.getSourceAvgItemSize());
    }

    private void removeUnnestOp(ILogicalOperator op) { // There will be only one UnnestOp for now at the top, so a while is strictly not necessary
        ILogicalOperator parent = op;
        op = op.getInputs().get(0).getValue(); // skip the select on the top
        while (op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
            op = op.getInputs().get(0).getValue();
        }
        parent.getInputs().get(0).setValue(op);
    }

    private void findApplicableJoinConditions() {
        List<JoinCondition> joinConditions = joinEnum.getJoinConditions();

        int i = 0;
        for (JoinCondition jc : joinConditions) {
            if (isSubset(jc.getDatasetSubset(), this.datasetSubset)) {
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
        DatasetRegistry.DatasetSubset newTableBits = joinEnum.datasetRegistry.new DatasetSubset();
        if (leftJn.jnArrayIndex <= joinEnum.numberOfTerms) {
            newTableBits = leftJn.datasetSubset;
        } else if (rightJn.jnArrayIndex <= joinEnum.numberOfTerms) {
            newTableBits = rightJn.datasetSubset;
        }

        if (LOGGER.isTraceEnabled() && newTableBits.size() == 0) {
            LOGGER.trace("newTable Bits == 0");
        }

        // All the new join predicates will have these bits turned on
        for (int idx : this.applicableJoinConditions) {
            if (joinEnum.datasetRegistry.intersection(joinEnum.joinConditions.get(idx).getDatasetSubset(), newTableBits)
                    .size() > 0) {
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
        // mark all conditions back to non deleted state
        for (JoinCondition jc : joinConditions) {
            jc.deleted = false;
        }
        // By dividing by redundantSel, we are undoing the earlier multiplication of all the selectivities.
        return joinCard / redundantSel;
    }

    private static double adjustSelectivities(JoinCondition jc1, JoinCondition jc2, JoinCondition jc3) {
        double sel;

        if (jc1.comparisonType == JoinCondition.comparisonOp.OP_EQ
                && jc2.comparisonType == JoinCondition.comparisonOp.OP_EQ
                && jc3.comparisonType == JoinCondition.comparisonOp.OP_EQ) {
            if (!jc1.partOfComposite)
                return jc1.selectivity;
            if (!jc2.partOfComposite)
                return jc2.selectivity;
            if (!jc3.partOfComposite)
                return jc3.selectivity;
            // one of the above must be true.
            sel = findRedundantSel(jc1, jc2, jc3);
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

    private static double findRedundantSel(JoinCondition jc1, JoinCondition jc2, JoinCondition jc3) {
        // find middle selectivity
        if (jc2.selectivity <= jc1.selectivity && jc1.selectivity <= jc3.selectivity) {
            jc1.deleted = true;
            return jc1.selectivity;
        }
        if (jc3.selectivity <= jc1.selectivity && jc1.selectivity <= jc2.selectivity) {
            jc1.deleted = true;
            return jc1.selectivity;
        }
        if (jc1.selectivity <= jc2.selectivity && jc2.selectivity <= jc3.selectivity) {
            jc2.deleted = true;
            return jc2.selectivity;
        }
        if (jc3.selectivity <= jc2.selectivity && jc2.selectivity <= jc1.selectivity) {
            jc2.deleted = true;
            return jc2.selectivity;
        }
        if (jc1.selectivity <= jc3.selectivity && jc3.selectivity <= jc2.selectivity) {
            jc3.deleted = true;
            return jc3.selectivity;
        }
        if (jc2.selectivity <= jc3.selectivity && jc3.selectivity <= jc1.selectivity) {
            jc3.deleted = true;
            return jc3.selectivity;
        }
        return 1.0; // keep compiler happy
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
        String[] vars = new String[6];
        String[] varsCopy = new String[6];
        for (int i = 0; i <= applicablePredicatesInCurrentJn.size() - 3; i++) {
            jc1 = joinConditions.get(applicablePredicatesInCurrentJn.get(i));
            if (jc1.deleted || jc1.usedVars == null) {
                continue; // must ignore these or the same triangles will be found more than once.
            }
            vars[0] = jc1.usedVars.get(0).toString();
            vars[1] = jc1.usedVars.get(1).toString();
            for (int j = i + 1; j <= applicablePredicatesInCurrentJn.size() - 2; j++) {
                jc2 = joinConditions.get(applicablePredicatesInCurrentJn.get(j));
                if (jc2.deleted || jc2.usedVars == null) {
                    continue;
                }
                vars[2] = jc2.usedVars.get(0).toString();
                vars[3] = jc2.usedVars.get(1).toString();
                for (int k = j + 1; k <= applicablePredicatesInCurrentJn.size() - 1; k++) {
                    jc3 = joinConditions.get(applicablePredicatesInCurrentJn.get(k));
                    if (jc3.deleted || jc3.usedVars == null) {
                        continue;
                    }
                    vars[4] = jc3.usedVars.get(0).toString();
                    vars[5] = jc3.usedVars.get(1).toString();

                    System.arraycopy(vars, 0, varsCopy, 0, 6);
                    Arrays.sort(varsCopy);
                    if (varsCopy[0] == varsCopy[1] && varsCopy[2] == varsCopy[3] && varsCopy[4] == varsCopy[5]) {
                        // redundant edge found
                        if (!(jc1.deleted || jc2.deleted)) {
                            redundantSel *= adjustSelectivities(jc1, jc2, jc3);
                        }
                    }
                }
            }
        }
        return redundantSel;
    }

    protected AbstractPlanNode addSingleDatasetPlans() {
        ICost opCost;
        ScanPlanNode pn;
        opCost = joinEnum.getCostMethodsHandle().costFullScan(this);
        boolean forceEnum = level <= joinEnum.cboFullEnumLevel;
        if (this.cheapestPlanNode == DummyPlanNode.INSTANCE || opCost.costLT(this.cheapestPlanCost) || forceEnum) {
            // for now just add one plan
            pn = new ScanPlanNode(datasetNames.get(0), leafInput.getOp(), this);
            pn.setScanMethod(ScanPlanNode.ScanMethod.TABLE_SCAN);
            pn.setScanCosts(opCost);
            planRegistry.addPlan(pn);
            setCheapestPlan(pn, forceEnum);
            return pn;
        }
        return DummyPlanNode.INSTANCE;
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

    static class IndexCostInfo {
        private final Index index;
        private double selectivity;
        private final AbstractFunctionCallExpression afce;
        private final boolean isIndexOnly;

        public IndexCostInfo(Index index, double selectivity, AbstractFunctionCallExpression afce,
                boolean isIndexOnly) {
            this.index = index;
            this.selectivity = selectivity;
            this.afce = afce;
            this.isIndexOnly = isIndexOnly;
        }

        public AbstractFunctionCallExpression getAfce() {
            return afce;
        }

        public Index getIndex() {
            return index;
        }

        public double getSelectivity() {
            return selectivity;
        }

        public void setSelectivity(double selectivity) {
            this.selectivity = selectivity;
        }

        public boolean isIndexOnly() {
            return isIndexOnly;
        }
    }

    private void setSkipIndexAnnotationsForUnusedIndexes() {

        for (IndexCostInfo indexCostInfo : indexCostInfoList) {
            if (indexCostInfo.getSelectivity() == -1.0) {
                AbstractFunctionCallExpression afce = indexCostInfo.getAfce();
                SkipSecondaryIndexSearchExpressionAnnotation skipAnno = joinEnum.findSkipIndexHint(afce);
                Collection<String> indexNames = new HashSet<>();
                if (skipAnno != null && skipAnno.getIndexNames() != null) {
                    indexNames.addAll(skipAnno.getIndexNames());
                }
                if (indexNames.isEmpty()) {
                    // this index has to be skipped, so find the corresponding expression
                    EnumerateJoinsRule.setAnnotation(afce, SkipSecondaryIndexSearchExpressionAnnotation
                            .newInstance(Collections.singleton(indexCostInfo.getIndex().getIndexName())));
                } else {
                    indexNames.add(indexCostInfo.getIndex().getIndexName());
                    EnumerateJoinsRule.setAnnotation(afce,
                            SkipSecondaryIndexSearchExpressionAnnotation.newInstance(indexNames));
                }
            }
        }
    }

    private void costAndChooseIndexPlans(ILogicalOperator leafInput,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            List<IntroduceSelectAccessMethodRule.IndexAccessInfo> chosenIndexes) throws AlgebricksException {
        SelectOperator selOp;
        double sel;

        List<IndexCostInfo> indexCostInfoList = new ArrayList<>();
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
                    selOp = (SelectOperator) joinEnum.getStatsHandle().findSelectOpWithExpr(leafInput, afce);
                    if (selOp == null) {
                        if (leafInput.getOperatorTag().equals(LogicalOperatorTag.SELECT)) {
                            selOp = (SelectOperator) leafInput;
                        } else {
                            selOp = new SelectOperator(new MutableObject<>(afce));
                            selOp.getInputs().add(new MutableObject<>(leafInput));
                        }
                    }
                    sel = joinEnum.getStatsHandle().findSelectivityForThisPredicate(selOp, afce,
                            chosenIndex.getIndexType().equals(DatasetConfig.IndexType.ARRAY)
                                    || joinEnum.findUnnestOp(selOp));
                }

                boolean isIndexOnlyApplicable = chosenIndexes.stream()
                        .filter(indexAccessInfo -> indexAccessInfo.getIndex().equals(chosenIndex)).findFirst()
                        .map(AbstractIntroduceAccessMethodRule.IndexAccessInfo::isIndexOnlyPlan).orElse(false);
                indexCostInfoList.add(new IndexCostInfo(chosenIndex, sel, afce, isIndexOnlyApplicable));
            }
        }
        this.indexCostInfoList = indexCostInfoList;
        if (!indexCostInfoList.isEmpty()) {
            buildIndexPlans();
        }
        setSkipIndexAnnotationsForUnusedIndexes();
    }

    private void buildIndexPlans() {
        ScanPlanNode pn;
        double ic, dc, tc, oc; // for debugging

        List<IndexCostInfo> mandatoryIndexesInfo = new ArrayList<>();
        List<IndexCostInfo> optionalIndexesInfo = new ArrayList<>();
        double sel = 1.0;

        for (IndexCostInfo indexCostInfo : indexCostInfoList) {

            if (joinEnum.findUseIndexHint(indexCostInfo.getAfce())) {
                mandatoryIndexesInfo.add(indexCostInfo);
            } else {
                optionalIndexesInfo.add(indexCostInfo);
            }

        }

        ICost indexCosts = this.joinEnum.getCostHandle().zeroCost();
        ICost totalCost = this.joinEnum.getCostHandle().zeroCost();
        ICost dataScanCost;

        boolean isIndexOnlyApplicable = true;
        int numSecondaryIndexesUsed = 0;

        if (mandatoryIndexesInfo.size() > 0) {
            for (int i = 0; i < mandatoryIndexesInfo.size(); i++) {
                ICost cost = joinEnum.getCostMethodsHandle().costIndexScan(this,
                        mandatoryIndexesInfo.get(i).getSelectivity());
                if ((mandatoryIndexesInfo.get(i).getIndex().getIndexType() == DatasetConfig.IndexType.ARRAY)) {
                    cost = cost.costAdd(cost); // double the cost for arrays.
                }

                isIndexOnlyApplicable = isIndexOnlyApplicable && mandatoryIndexesInfo.get(i).isIndexOnly();
                numSecondaryIndexesUsed += mandatoryIndexesInfo.get(i).getIndex().isPrimaryIndex() ? 0 : 1;

                indexCosts = indexCosts.costAdd(cost); // a running tally
                sel *= mandatoryIndexesInfo.get(i).getSelectivity();
            }
            dataScanCost = joinEnum.getCostMethodsHandle().costIndexDataScan(this, sel);
            totalCost = indexCosts.costAdd(dataScanCost); // this is the total cost of using the mandatory costs. This cost cannot be skipped.
        }

        ICost opCost = totalCost;
        if (optionalIndexesInfo.size() > 0) {
            optionalIndexesInfo.sort(Comparator.comparingDouble(o -> o.getSelectivity()));

            for (int i = 0; i < optionalIndexesInfo.size(); i++) {
                ICost cost = joinEnum.getCostMethodsHandle().costIndexScan(this,
                        optionalIndexesInfo.get(i).getSelectivity());
                if ((optionalIndexesInfo.get(i).getIndex().getIndexType() == DatasetConfig.IndexType.ARRAY)) {
                    cost = cost.costAdd(cost); // double the cost for arrays.
                }
                indexCosts = indexCosts.costAdd(cost);
                sel *= optionalIndexesInfo.get(i).getSelectivity();
                dataScanCost = joinEnum.getCostMethodsHandle().costIndexDataScan(this, sel);
                opCost = indexCosts.costAdd(dataScanCost);
                tc = totalCost.computeTotalCost();

                isIndexOnlyApplicable = isIndexOnlyApplicable && optionalIndexesInfo.get(i).isIndexOnly();
                numSecondaryIndexesUsed += optionalIndexesInfo.get(i).getIndex().isPrimaryIndex() ? 0 : 1;

                if (tc > 0.0) {
                    if (opCost.costGT(totalCost)) { // we can stop here since additional indexes are not useful
                        for (int j = i; j < optionalIndexesInfo.size(); j++) {
                            optionalIndexesInfo.get(j).setSelectivity(-1.0);
                        }
                        opCost = totalCost;
                        break;
                    }
                } else {
                    if (!isIndexOnlyApplicable || numSecondaryIndexesUsed > 1) {
                        totalCost = indexCosts.costAdd(dataScanCost);
                    }
                }
            }
        }

        // opCost is now the total cost of the indexes chosen along with the associated data scan cost.
        if (opCost.costGT(this.cheapestPlanCost) && level > joinEnum.cboFullEnumLevel) { // cheapest plan cost is the data scan cost.
            for (int j = 0; j < optionalIndexesInfo.size(); j++) {
                optionalIndexesInfo.get(j).setSelectivity(-1.0); // remove all indexes from consideration.
            }
        }

        boolean forceEnum = mandatoryIndexesInfo.size() > 0 || level <= joinEnum.cboFullEnumLevel;
        if (opCost.costLT(this.cheapestPlanCost) || forceEnum) {
            pn = new ScanPlanNode(datasetNames.get(0), leafInput.getOp(), this);
            pn.setScanAndHintInfo(ScanPlanNode.ScanMethod.INDEX_SCAN, mandatoryIndexesInfo, optionalIndexesInfo);
            pn.setScanCosts(totalCost);
            planRegistry.addPlan(pn);
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

    protected void addIndexAccessPlans(ILogicalOperator leafInput, IIndexProvider indexProvider)
            throws AlgebricksException {
        IntroduceSelectAccessMethodRule tmp = new IntroduceSelectAccessMethodRule();
        List<IntroduceSelectAccessMethodRule.IndexAccessInfo> chosenIndexes = new ArrayList<>();
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();

        while (combineDoubleSelectsBeforeSubPlans(leafInput));
        List<ILogicalExpression> selExprs = new ArrayList<>();
        List<SelectOperator> selOpers = new ArrayList<>();
        SelectOperator firstSelop = copySelExprsAndSetTrue(selExprs, selOpers, leafInput);
        if (firstSelop != null) { // if there are no selects, then there is no question of index selections either.
            firstSelop.getCondition().setValue(andAlltheExprs(selExprs));
            boolean index_access_possible = tmp.checkApplicable(new MutableObject<>(leafInput), joinEnum.optCtx,
                    chosenIndexes, analyzedAMs, indexProvider);
            restoreSelExprs(selExprs, selOpers);
            if (index_access_possible) {
                costAndChooseIndexPlans(leafInput, analyzedAMs, chosenIndexes);
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

    private boolean nullExtendingSide(DatasetRegistry.DatasetSubset bits, boolean outerJoin) {
        if (outerJoin) {
            for (Quadruple<DatasetRegistry.DatasetSubset, DatasetRegistry.DatasetSubset, JoinOperator, Integer> qu : joinEnum.outerJoinsDependencyList) {
                if (qu.getThird().getOuterJoin()) {
                    if (DatasetRegistry.equals(qu.getSecond(), bits)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean hashJoinApplicable(JoinNode leftJn, boolean outerJoin, ILogicalExpression hashJoinExpr) {
        if (nullExtendingSide(leftJn.datasetSubset, outerJoin)) {
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
            List<AbstractIntroduceAccessMethodRule.IndexAccessInfo> chosenIndexes,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IIndexProvider indexProvider)
            throws AlgebricksException {

        if (joinExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        List<LogicalVariable> usedVarList = new ArrayList<>();
        joinExpr.getUsedVariables(usedVarList);
        if (usedVarList.size() != 2 && outerJoin) {
            return false;
        }
        RealLeafInput joinLeafInput0 = null;
        RealLeafInput joinLeafInput1 = null;
        // Find which joinLeafInput these vars belong to.
        // go through the leaf inputs and see where these variables came from
        for (LogicalVariable usedVar : usedVarList) {
            RealLeafInput joinLeafInput = joinEnum.findLeafInput(Collections.singletonList(usedVar));
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

        RealLeafInput innerLeafInput = (RealLeafInput) this.leafInput;

        // This must equal one of the two joinLeafInputsHashMap found above. check for sanity!!
        if (innerLeafInput != joinLeafInput0 && innerLeafInput != joinLeafInput1) {
            return false; // This should not happen. So debug to find out why this happened.
        }

        if (innerLeafInput == joinLeafInput0) { // skip the Select Operator with condition(TRUE) on top
            joinEnum.localJoinOp.getInputs().getFirst().setValue(joinLeafInput1.getOp().getInputs().get(0).getValue());
        } else {
            joinEnum.localJoinOp.getInputs().getFirst().setValue(joinLeafInput0.getOp().getInputs().get(0).getValue());
        }

        joinEnum.localJoinOp.getInputs().get(1).setValue(innerLeafInput.getOp().getInputs().get(0).getValue());

        // We will always use the first join Op to provide the joinOp input for invoking rewritePre
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinEnum.localJoinOp;
        joinOp.getCondition().setValue(joinExpr);

        // Now call the rewritePre code
        IntroduceJoinAccessMethodRule tmp = new IntroduceJoinAccessMethodRule();
        boolean retVal = tmp.checkApplicable(new MutableObject<>(joinEnum.localJoinOp), joinEnum.optCtx, chosenIndexes,
                analyzedAMs, indexProvider);

        return retVal;
    }

    private boolean NLJoinApplicable(JoinNode leftJn, JoinNode rightJn, boolean outerJoin,
            ILogicalExpression nestedLoopJoinExpr,
            List<AbstractIntroduceAccessMethodRule.IndexAccessInfo> chosenIndexes,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IIndexProvider indexProvider)
            throws AlgebricksException {

        if (leftJn.fake || rightJn.fake) {
            return false;
        }
        if (nullExtendingSide(leftJn.datasetSubset, outerJoin)) {
            return false;
        }

        if (rightJn.jnArrayIndex > joinEnum.numberOfTerms) {
            // right side consists of more than one table
            return false; // nested loop plan not possible.
        }

        if (nestedLoopJoinExpr == null || !rightJn.nestedLoopsApplicable(nestedLoopJoinExpr, outerJoin, chosenIndexes,
                analyzedAMs, indexProvider)) {
            return false;
        }

        return true;
    }

    private boolean CPJoinApplicable(JoinNode leftJn, boolean outerJoin) {

        if (leftJn.fake || rightJn.fake) {
            return false;
        }

        if (!joinEnum.cboCPEnumMode) {
            return false;
        }

        if (nullExtendingSide(leftJn.datasetSubset, outerJoin)) {
            return false;
        }

        return true;
    }

    protected AbstractPlanNode buildHashJoinPlan(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression hashJoinExpr, HashJoinExpressionAnnotation hintHashJoin, boolean outerJoin) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();
        JoinPlanNode pn;
        ICost hjCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;
        this.leftJn = leftJn;
        this.rightJn = rightJn;

        if (!hashJoinApplicable(leftJn, outerJoin, hashJoinExpr)) {
            return DummyPlanNode.INSTANCE;
        }

        boolean forceEnum = hintHashJoin != null || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG) || outerJoin
                || level <= joinEnum.cboFullEnumLevel;
        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || forceEnum) {
            // We want to build with the smaller side.
            hjCost = joinEnum.getCostMethodsHandle().costHashJoin(this);
            leftExchangeCost = joinEnum.getCostMethodsHandle().computeHJProbeExchangeCost(this);
            rightExchangeCost = joinEnum.getCostMethodsHandle().computeHJBuildExchangeCost(this);
            childCosts = leftPlan.getTotalCost().costAdd(rightPlan.getTotalCost());
            totalCost = hjCost.costAdd(leftExchangeCost).costAdd(rightExchangeCost).costAdd(childCosts);
            if (this.cheapestPlanNode == DummyPlanNode.INSTANCE || totalCost.costLT(this.cheapestPlanCost)
                    || forceEnum) {
                pn = new JoinPlanNode(this, leftPlan, rightPlan, outerJoin);
                pn.setJoinAndHintInfo(JoinPlanNode.JoinMethod.HYBRID_HASH_JOIN, hashJoinExpr, null,
                        HashJoinExpressionAnnotation.BuildSide.RIGHT, hintHashJoin);
                pn.setJoinCosts(hjCost, totalCost, leftExchangeCost, rightExchangeCost);
                planRegistry.addPlan(pn);
                setCheapestPlan(pn, forceEnum);
                return pn;
            }
        }
        return DummyPlanNode.INSTANCE;
    }

    private AbstractPlanNode buildBroadcastHashJoinPlan(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression hashJoinExpr, BroadcastExpressionAnnotation hintBroadcastHashJoin, boolean outerJoin) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();
        JoinPlanNode pn;
        ICost bcastHjCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        //if (leftJn.fake || rightJn.fake) { // uncomment if broadcast hash joins are not applicable
        //return PlanNode.NO_PLAN;
        //}

        if (!hashJoinApplicable(leftJn, outerJoin, hashJoinExpr)) {
            return DummyPlanNode.INSTANCE;
        }

        boolean forceEnum = hintBroadcastHashJoin != null || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG) || outerJoin
                || level <= joinEnum.cboFullEnumLevel;
        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || forceEnum) {
            // We want to broadcast and build with the smaller side.
            bcastHjCost = joinEnum.getCostMethodsHandle().costBroadcastHashJoin(this);
            leftExchangeCost = joinEnum.getCostHandle().zeroCost();
            rightExchangeCost = joinEnum.getCostMethodsHandle().computeBHJBuildExchangeCost(this);
            childCosts = leftPlan.getTotalCost().costAdd(rightPlan.getTotalCost());
            totalCost = bcastHjCost.costAdd(rightExchangeCost).costAdd(childCosts);
            if (this.cheapestPlanNode == DummyPlanNode.INSTANCE || totalCost.costLT(this.cheapestPlanCost)
                    || forceEnum) {
                pn = new JoinPlanNode(this, leftPlan, rightPlan, outerJoin);
                pn.setJoinAndHintInfo(JoinPlanNode.JoinMethod.BROADCAST_HASH_JOIN, hashJoinExpr, null,
                        HashJoinExpressionAnnotation.BuildSide.RIGHT, hintBroadcastHashJoin);
                pn.setJoinCosts(bcastHjCost, totalCost, leftExchangeCost, rightExchangeCost);
                planRegistry.addPlan(pn);
                setCheapestPlan(pn, forceEnum);
                return pn;
            }
        }
        return DummyPlanNode.INSTANCE;
    }

    private AbstractPlanNode buildNLJoinPlan(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression nestedLoopJoinExpr, IndexedNLJoinExpressionAnnotation hintNLJoin, boolean outerJoin,
            IIndexProvider indexProvider) throws AlgebricksException {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        // Build a nested loops plan, first check if it is possible
        // left right order must be preserved and right side should be a single data set
        JoinPlanNode pn;
        ICost nljCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        if (leftJn.fake || rightJn.fake) {
            return DummyPlanNode.INSTANCE;
        }

        List<AbstractIntroduceAccessMethodRule.IndexAccessInfo> chosenIndexes = new ArrayList<>();
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        if (!NLJoinApplicable(leftJn, rightJn, outerJoin, nestedLoopJoinExpr, chosenIndexes, analyzedAMs,
                indexProvider)) {
            return DummyPlanNode.INSTANCE;
        }
        if (chosenIndexes.isEmpty()) {
            return DummyPlanNode.INSTANCE;
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
            return DummyPlanNode.INSTANCE;
        }
        leftExchangeCost = joinEnum.getCostMethodsHandle().computeNLJOuterExchangeCost(this);
        rightExchangeCost = joinEnum.getCostHandle().zeroCost();
        childCosts = leftPlan.getTotalCost();
        totalCost = nljCost.costAdd(leftExchangeCost).costAdd(childCosts);
        boolean forceEnum =
                hintNLJoin != null || joinEnum.forceJoinOrderMode || outerJoin || level <= joinEnum.cboFullEnumLevel;
        if (this.cheapestPlanNode == DummyPlanNode.INSTANCE || totalCost.costLT(this.cheapestPlanCost) || forceEnum) {
            pn = new JoinPlanNode(this, leftPlan, rightPlan, outerJoin);
            pn.setJoinAndHintInfo(JoinPlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN, nestedLoopJoinExpr, exprAndHint, null,
                    hintNLJoin);
            pn.setJoinCosts(nljCost, totalCost, leftExchangeCost, rightExchangeCost);
            planRegistry.addPlan(pn);
            setCheapestPlan(pn, forceEnum);
            return pn;
        }
        return DummyPlanNode.INSTANCE;
    }

    private AbstractPlanNode buildCPJoinPlan(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression hashJoinExpr, ILogicalExpression nestedLoopJoinExpr, boolean outerJoin) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        if (leftJn.fake || rightJn.fake) {
            return DummyPlanNode.INSTANCE;
        }

        // Now build a cartesian product nested loops plan
        JoinPlanNode pn;
        ICost cpCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        if (!CPJoinApplicable(leftJn, outerJoin)) {
            return DummyPlanNode.INSTANCE;
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
        childCosts = leftPlan.getTotalCost().costAdd(rightPlan.getTotalCost());
        totalCost = cpCost.costAdd(rightExchangeCost).costAdd(childCosts);
        boolean forceEnum = joinEnum.forceJoinOrderMode || outerJoin || level <= joinEnum.cboFullEnumLevel;
        if (this.cheapestPlanNode == DummyPlanNode.INSTANCE || totalCost.costLT(this.cheapestPlanCost) || forceEnum) {
            pn = new JoinPlanNode(this, leftPlan, rightPlan, outerJoin);
            pn.setJoinAndHintInfo(JoinPlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN,
                    Objects.requireNonNullElse(cpJoinExpr, ConstantExpression.TRUE), null, null, null);
            pn.setJoinCosts(cpCost, totalCost, leftExchangeCost, rightExchangeCost);
            planRegistry.addPlan(pn);
            setCheapestPlan(pn, forceEnum);
            return pn;
        }
        return DummyPlanNode.INSTANCE;
    }

    protected void addMultiDatasetPlans(JoinNode leftJn, JoinNode rightJn, IIndexProvider indexProvider)
            throws AlgebricksException {

        if (level > joinEnum.cboFullEnumLevel) {
            // FOR JOIN NODE LEVELS GREATER THAN THE LEVEL SPECIFIED FOR FULL ENUMERATION,
            // DO NOT DO FULL ENUMERATION => PRUNE
            if (leftJn.cheapestPlanNode == DummyPlanNode.INSTANCE
                    || rightJn.cheapestPlanNode == DummyPlanNode.INSTANCE) {
                return;
            }

            AbstractPlanNode leftPlan = leftJn.cheapestPlanNode, rightPlan = rightJn.cheapestPlanNode;

            addMultiDatasetPlans(leftPlan, rightPlan, indexProvider);
        } else {
            // FOR JOIN NODE LEVELS LESS THAN OR EQUAL TO THE LEVEL SPECIFIED FOR FULL ENUMERATION,
            // DO FULL ENUMERATION => DO NOT PRUNE
            for (AbstractPlanNode leftPlan : leftJn.getPlanRegistry().getAllPlans()) {
                for (AbstractPlanNode rightPlan : rightJn.getPlanRegistry().getAllPlans()) {
                    addMultiDatasetPlans(leftPlan, rightPlan, indexProvider);
                }
            }
        }
    }

    protected void addMultiDatasetPlans(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            IIndexProvider indexProvider) throws AlgebricksException {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        this.leftJn = leftJn;
        this.rightJn = rightJn;

        if (leftJn.getPlanRegistry().size() == 0 || rightJn.getPlanRegistry().size() == 0) {
            return;
        }

        if (this.cardinality >= Cost.MAX_CARD) {
            return; // no card available, so do not add this plan
        }

        if (leftJn.cheapestPlanNode == DummyPlanNode.INSTANCE || rightJn.cheapestPlanNode == DummyPlanNode.INSTANCE) {
            return;
        }

        List<Integer> newJoinConditions = this.getNewJoinConditionsOnly(); // these will be a subset of applicable join conditions.
        if ((newJoinConditions.size() == 0) && joinEnum.connectedJoinGraph) {
            // at least one plan must be there at each level as the graph is fully connected.
            if (leftJn.cardinality * rightJn.cardinality > 10000.0 && level > joinEnum.cboFullEnumLevel
                    && !joinEnum.outerJoin) { // when outer joins are present, moving joins around is restricted
                return;
            }
        }
        boolean outerJoin = joinEnum.lookForOuterJoins(newJoinConditions);
        ILogicalExpression hashJoinExpr = joinEnum.getHashJoinExpr(newJoinConditions, outerJoin);
        ILogicalExpression nestedLoopJoinExpr = joinEnum.getNestedLoopJoinExpr(newJoinConditions);

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
                    newJoinConditions, indexProvider);
        }

        if (!validPlan) {
            // No join hints or inapplicable hinted plans, try all non hinted plans.
            buildAllNonHintedPlans(leftPlan, rightPlan, hashJoinExpr, nestedLoopJoinExpr, outerJoin, indexProvider);
        }

        //Reset as these might have changed when we tried the commutative joins.
        this.leftJn = leftJn;
        this.rightJn = rightJn;
    }

    private boolean buildHintedHJPlans(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression hashJoinExpr, HashJoinExpressionAnnotation hintHashJoin, boolean outerJoin,
            List<Integer> newJoinConditions) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();

        AbstractPlanNode hjPlan, commutativeHjPlan;
        hjPlan = commutativeHjPlan = DummyPlanNode.INSTANCE;
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

    private boolean buildHintedBcastHJPlans(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression hashJoinExpr, BroadcastExpressionAnnotation hintBroadcastHashJoin, boolean outerJoin,
            List<Integer> newJoinConditions) {
        JoinNode leftJn = leftPlan.getJoinNode();
        JoinNode rightJn = rightPlan.getJoinNode();
        AbstractPlanNode bcastHjPlan, commutativeBcastHjPlan;
        bcastHjPlan = commutativeBcastHjPlan = DummyPlanNode.INSTANCE;
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

    private boolean buildHintedNLJPlans(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression nestedLoopJoinExpr, IndexedNLJoinExpressionAnnotation hintNLJoin, boolean outerJoin,
            List<Integer> newJoinConditions, IIndexProvider indexProvider) throws AlgebricksException {
        AbstractPlanNode nljPlan, commutativeNljPlan;
        nljPlan = commutativeNljPlan = DummyPlanNode.INSTANCE;
        nljPlan = buildNLJoinPlan(leftPlan, rightPlan, nestedLoopJoinExpr, hintNLJoin, outerJoin, indexProvider);

        // The indexnl hint may have been removed during applicability checking
        // and is no longer available for a hintedNL plan.
        if (joinEnum.findNLJoinHint(newJoinConditions) == null) {
            return false;
        }
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            commutativeNljPlan =
                    buildNLJoinPlan(rightPlan, leftPlan, nestedLoopJoinExpr, hintNLJoin, outerJoin, indexProvider);
            // The indexnl hint may have been removed during applicability checking
            // and is no longer available for a hintedNL plan.
            if (joinEnum.findNLJoinHint(newJoinConditions) == null) {
                return false;
            }
        }

        return handleHints(nljPlan, commutativeNljPlan, hintNLJoin, newJoinConditions);
    }

    private boolean handleHints(AbstractPlanNode plan, AbstractPlanNode commutativePlan, IExpressionAnnotation hint,
            List<Integer> newJoinConditions) {
        if (plan != DummyPlanNode.INSTANCE || commutativePlan != DummyPlanNode.INSTANCE) {
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

    private void buildAllNonHintedPlans(AbstractPlanNode leftPlan, AbstractPlanNode rightPlan,
            ILogicalExpression hashJoinExpr, ILogicalExpression nestedLoopJoinExpr, boolean outerJoin,
            IIndexProvider indexProvider) throws AlgebricksException {

        buildHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, null, outerJoin);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, null, outerJoin);
        }
        buildBroadcastHashJoinPlan(leftPlan, rightPlan, hashJoinExpr, null, outerJoin);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildBroadcastHashJoinPlan(rightPlan, leftPlan, hashJoinExpr, null, outerJoin);
        }
        buildNLJoinPlan(leftPlan, rightPlan, nestedLoopJoinExpr, null, outerJoin, indexProvider);
        if (!joinEnum.forceJoinOrderMode || level <= joinEnum.cboFullEnumLevel) {
            buildNLJoinPlan(rightPlan, leftPlan, nestedLoopJoinExpr, null, outerJoin, indexProvider);
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

    private AbstractPlanNode findCheapestPlan() {
        ICost cheapestCost = joinEnum.getCostHandle().maxCost();
        AbstractPlanNode cheapestPlanNode = DummyPlanNode.INSTANCE;
        int numHintsUsedInCheapestPlan = 0;

        for (AbstractPlanNode plan : planRegistry.getAllPlans()) {
            if (plan.numHintsUsed > numHintsUsedInCheapestPlan) {
                // "plan" has used more hints than "cheapestPlan", "plan" wins!
                cheapestPlanNode = plan;
                cheapestCost = plan.getTotalCost();
                numHintsUsedInCheapestPlan = plan.numHintsUsed;
            } else if (plan.numHintsUsed == numHintsUsedInCheapestPlan) {
                // Either both "plan" and "cheapestPlan" are hinted, or both are non-hinted.
                // Cost is the decider.
                if (plan.getTotalCost().costLT(cheapestCost)) {
                    cheapestPlanNode = plan;
                    cheapestCost = plan.getTotalCost();
                }
            } else {
                // This is the case where "plan" has used fewer hints than "cheapestPlan".
                // We have already captured the cheapest plan, nothing to do.
            }
        }
        return cheapestPlanNode;
    }

    private void setCheapestPlan(AbstractPlanNode pn, boolean forceEnum) {
        AbstractPlanNode cheapestPlanNode = forceEnum ? findCheapestPlan() : pn;
        this.cheapestPlanNode = cheapestPlanNode;
        this.cheapestPlanCost = cheapestPlanNode.getTotalCost();
    }

    @Override
    public String toString() {
        NumberFormat scientificFormat = new DecimalFormat("0.###E0");
        DecimalFormat formatter = new DecimalFormat("#,###.00");
        StringBuilder sb = new StringBuilder(128);
        if (IsBaseLevelJoinNode()) {
            sb.append("Printing Scan Node ");
            sb.append("Fake " + getFake() + " ");
        } else {
            sb.append("Printing Join Node ");
        }
        sb.append(jnArrayIndex).append('\n');
        sb.append("datasetNames (aliases) ");
        if(datasetNames!=null) {
            for (int j = 0; j < datasetNames.size(); j++) {
                sb.append(datasetNames.get(j)).append('(').append(aliases.get(j)).append(')').append(' ');
            }
        }
        sb.append('\n');
        sb.append("datasetIndexes ");
        for (int j = 0; j < datasetIndexes.size(); j++) {
            sb.append(j).append(datasetIndexes.get(j)).append(' ');
        }
        sb.append('\n');
        sb.append("datasetBits ").append(datasetSubset).append('\n');
        sb.append("jnIndex ").append(jnIndex).append('\n');
        sb.append("level ").append(level).append('\n');
        sb.append("highestDatasetId ").append(highestDatasetId).append('\n');
        if (IsBaseLevelJoinNode()) {
            //sb.append("orig cardinality ").append(dumpDouble(origCardinality));
            //sb.append("orig cardinality ").append(dumpDouble(Double.parseDouble(scientificFormat.format(origCardinality))));
            sb.append("orig cardinality2 ")
                    .append(formatter.format(Double.parseDouble(String.valueOf(origCardinality))));
            sb.append("\n    cardinality \n").append(formatter.format(Double.parseDouble(String.valueOf(cardinality))));
        } else {
            //sb.append("cardinality ").append(dumpDouble(cardinality));
            //sb.append("cardinality ").append(dumpDouble(Double.parseDouble(scientificFormat.format(cardinality))));
            sb.append("cardinality \n").append(formatter.format(Double.parseDouble(String.valueOf(cardinality))));
        }
        sb.append("size ").append(dumpDouble(size));
        sb.append("outputSize(sizeVarsAfterScan) ").append(dumpDouble(sizeVarsAfterScan));
        if (planRegistry.size() == 0) {
            sb.append("No plans considered for this join node").append('\n');
        } else {
            for (int j = 0; j < planRegistry.size(); j++) {
                AbstractPlanNode pn = planRegistry.getAllPlans().get(j);
                sb.append("\nPrinting Plan ").append(pn).append('\n');
                sb.append("Plan Registry [").append(j).append("] ").append(pn).append('\n');
                if (IsBaseLevelJoinNode()) {
                    if (pn instanceof  ScanPlanNode scanPlanNode) {
                        if (scanPlanNode.getScanOp() == ScanPlanNode.ScanMethod.TABLE_SCAN) {
                            sb.append("DATA_SOURCE_SCAN").append('\n');
                        } else {
                            sb.append("INDEX_SCAN").append('\n');
                        }
                    }
                } else {
                    JoinPlanNode joinPlanNode = (JoinPlanNode) pn;
                    sb.append(joinPlanNode.joinMethod().getFirst()).append('\n');
                    sb.append("Join expr ");
                    if (joinPlanNode.getJoinExpr() != null) {
                        sb.append(joinPlanNode.getJoinExpr()).append('\n');
                        sb.append("outer join " + joinPlanNode.outerJoin).append('\n');
                    } else {
                        sb.append("null").append('\n');
                    }
                }
                sb.append("operator cost ").append(dumpCost(pn.getOpCost()));
                sb.append("total cost ").append(dumpCost(pn.getTotalCost()));
//                sb.append("jnIndexes ").append(pn.jnIndexes[0]).append(" ").append(pn.jnIndexes[1]).append('\n');
                if (IsHigherLevelJoinNode()) {
                    JoinPlanNode joinPlanNode  = (JoinPlanNode) pn;
                    AbstractPlanNode     leftPlan = joinPlanNode.getLeftPlan();
                    AbstractPlanNode rightPlan = joinPlanNode.getRightPlan();
//                    sb.append("Left & Right Plans ").append(leftPlan).append(" ").append(rightPlan)
//                            .append('\n');
//                    sb.append(dumpLeftRightPlanCosts(pn));
//                  TODO(preetham) : Revisit printing left and right plan details
                }
            }
            if (cheapestPlanNode != DummyPlanNode.INSTANCE) {
                sb.append("\nCheapest plan for JoinNode ").append(jnArrayIndex).append(" is ").append(cheapestPlanIndex)
                        .append(", cost is ")
                        .append(dumpDouble(cheapestPlanNode.getTotalCost().computeTotalCost()));
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

    protected String dumpLeftRightPlanCosts(JoinPlanNode pn) {
        StringBuilder sb = new StringBuilder(128);
        AbstractPlanNode leftPlan = pn.getLeftPlan();
        AbstractPlanNode rightPlan = pn.getRightPlan();
        ICost leftPlanTotalCost = leftPlan.getTotalCost();
        ICost rightPlanTotalCost = rightPlan.getTotalCost();

        sb.append("  left plan cost ").append(dumpDoubleNoNewline(leftPlanTotalCost.computeTotalCost()))
                .append(", right plan cost ").append(dumpDouble(rightPlanTotalCost.computeTotalCost()));
        return sb.toString();
    }

    protected void printCostOfAllPlans(StringBuilder sb) {
        ICost minCost = joinEnum.getCostHandle().maxCost();
        AbstractPlanNode lowestCostPlan = DummyPlanNode.INSTANCE;
        for (AbstractPlanNode plan : planRegistry.getAllPlans()) {
            ICost planCost = plan.getTotalCost();
            sb.append("plan ").append(plan).append(" cost is ").append(dumpDouble(planCost.computeTotalCost()));
            if (planCost.costLT(minCost)) {
                minCost = planCost;
                lowestCostPlan = plan;
            }
        }
        sb.append("END Cheapest Plan is ").append(lowestCostPlan).append(", Cost is ")
                .append(dumpDouble(minCost.computeTotalCost()));
    }
}
