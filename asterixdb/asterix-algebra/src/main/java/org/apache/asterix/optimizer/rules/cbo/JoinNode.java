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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.cost.Cost;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.asterix.optimizer.rules.am.AccessMethodAnalysisContext;
import org.apache.asterix.optimizer.rules.am.IAccessMethod;
import org.apache.asterix.optimizer.rules.am.IOptimizableFuncExpr;
import org.apache.asterix.optimizer.rules.am.IntroduceJoinAccessMethodRule;
import org.apache.asterix.optimizer.rules.am.IntroduceSelectAccessMethodRule;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
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
    private ICost cheapestPlanCost;
    protected double origCardinality; // without any selections
    protected double cardinality;
    protected double size;
    protected List<Integer> planIndexesArray; // indexes into the PlanNode array in enumerateJoins
    protected int jnIndex;
    protected int level;
    protected int highestDatasetId;
    private JoinNode rightJn;
    private JoinNode leftJn;
    private List<Integer> applicableJoinConditions;
    protected EmptyTupleSourceOperator correspondingEmptyTupleSourceOp; // There is a 1-1 relationship between the LVs and the dataSourceScanOps and the leafInputs.
    private List<Pair<IAccessMethod, Index>> chosenIndexes;
    private Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs;
    protected Index.SampleIndexDetails idxDetails;
    private List<Triple<Index, Double, AbstractFunctionCallExpression>> IndexCostInfo;
    // The triple above is : Index, selectivity, and the index expression
    protected static int NO_JN = -1;
    private static int NO_CARDS = -2;

    private JoinNode(int i) {
        this.jnArrayIndex = i;
        planIndexesArray = new ArrayList<>();
        cheapestPlanIndex = PlanNode.NO_PLAN;
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

    protected void setCardinality(double card) {
        cardinality = card;
    }

    public double getOrigCardinality() {
        return origCardinality;
    }

    protected void setOrigCardinality(double card) {
        origCardinality = card;
    }

    protected void setAvgDocSize(double avgDocSize) {
        size = avgDocSize;
    }

    public double getInputSize() {
        return size;
    }

    public double getOutputSize() {
        return size; // need to change this to account for projections
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

    private boolean nestedLoopsApplicable(ILogicalExpression joinExpr) throws AlgebricksException {

        List<LogicalVariable> usedVarList = new ArrayList<>();
        joinExpr.getUsedVariables(usedVarList);
        if (usedVarList.size() != 2) {
            return false;
        }

        if (joinExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        LogicalVariable var0 = usedVarList.get(0);
        LogicalVariable var1 = usedVarList.get(1);

        // Find which joinLeafInput these vars belong to.
        // go thru the leaf inputs and see where these variables came from
        ILogicalOperator joinLeafInput0 = joinEnum.findLeafInput(Collections.singletonList(var0));
        if (joinLeafInput0 == null) {
            return false; // this should not happen unless an assignment is between two joins.
        }

        ILogicalOperator joinLeafInput1 = joinEnum.findLeafInput(Collections.singletonList(var1));
        if (joinLeafInput1 == null) {
            return false;
        }

        // We need to find out which one of these is the inner joinLeafInput. So for that get the joinLeafInput of this join node.
        ILogicalOperator innerLeafInput = joinEnum.joinLeafInputsHashMap.get(this.correspondingEmptyTupleSourceOp);

        // This must equal one of the two joinLeafInputsHashMap found above. check for sanity!!
        if (innerLeafInput != joinLeafInput1 && innerLeafInput != joinLeafInput0) {
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
        boolean retVal = tmp.checkApplicable(new MutableObject<>(joinEnum.localJoinOp), joinEnum.optCtx);

        return retVal;
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
            if (!joinConditions.get(idx).partOfComposite) {
                productJoinSels *= joinConditions.get(idx).selectivity;
            }
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
        ICost opCost, totalCost;

        opCost = joinEnum.getCostMethodsHandle().costFullScan(this);
        totalCost = opCost;
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || opCost.costLT(this.cheapestPlanCost)) {
            // for now just add one plan
            PlanNode pn = new PlanNode(allPlans.size(), joinEnum);
            pn.setJoinNode(this);
            pn.datasetName = this.datasetNames.get(0);
            pn.correspondingEmptyTupleSourceOp = this.correspondingEmptyTupleSourceOp;
            pn.setLeftJoinIndex(this.jnArrayIndex);
            pn.setRightJoinIndex(JoinNode.NO_JN);
            pn.setLeftPlanIndex(PlanNode.NO_PLAN); // There ane no plans below this plan.
            pn.setRightPlanIndex(PlanNode.NO_PLAN); // There ane no plans below this plan.
            pn.opCost = opCost;
            pn.scanOp = PlanNode.ScanMethod.TABLE_SCAN;
            pn.totalCost = totalCost;

            allPlans.add(pn);
            this.planIndexesArray.add(pn.allPlansIndex);
            this.cheapestPlanCost = totalCost;
            this.cheapestPlanIndex = pn.allPlansIndex;
            return this.cheapestPlanIndex;
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
                // this index has to be skipped, so find the corresponding expression
                afce.putAnnotation(SkipSecondaryIndexSearchExpressionAnnotation
                        .newInstance(Collections.singleton(IndexCostInfo.get(i).first.getIndexName())));
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
                        selOp = (SelectOperator) leafInput;
                    } else {
                        selOp = new SelectOperator(new MutableObject<>(afce));
                        selOp.getInputs().add(new MutableObject<>(leafInput));
                    }
                    sel = joinEnum.getStatsHandle().findSelectivityForThisPredicate(selOp, afce, this.origCardinality);
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
                dataCosts.add(joinEnum.getCostMethodsHandle().costIndexDataScan(this, sel)); // D0; D01; D012; ...
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
                if (currentCost.costLT(opCost)) { // save this cost and try adding one more index
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
        if (opCost.costGT(this.cheapestPlanCost)) { // cheapest plan cost is the data scan cost.
            for (int j = 0; j < optionalIndexesInfo.size(); j++) {
                optionalIndexesInfo.get(j).second = -1.0; // remove all indexes from consideration.
            }
        }

        totalCost = opCost.costAdd(mandatoryIndexesCost); // cost of all the indexes chosen
        if (opCost.costLT(this.cheapestPlanCost) || mandatoryIndexesInfo.size() > 0) {
            PlanNode pn = new PlanNode(allPlans.size(), joinEnum);
            pn.setJoinNode(this);
            pn.setDatasetName(getDatasetNames().get(0));
            pn.setEmptyTupleSourceOp(this.correspondingEmptyTupleSourceOp);
            pn.setLeftJoinIndex(this.jnArrayIndex);
            pn.setRightJoinIndex(JoinNode.NO_JN);
            pn.setLeftPlanIndex(PlanNode.NO_PLAN); // There ane no plans below this plan.
            pn.setRightPlanIndex(PlanNode.NO_PLAN); // There ane no plans below this plan.
            pn.setOpCost(totalCost);
            pn.setScanMethod(PlanNode.ScanMethod.INDEX_SCAN);
            pn.setTotalCost(totalCost);

            allPlans.add(pn);
            this.planIndexesArray.add(pn.allPlansIndex);
            this.cheapestPlanCost = totalCost; // in the presence of mandatory indexes, this may not be the cheapest plan! But we have no choice!
            this.cheapestPlanIndex = pn.allPlansIndex;
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
            this.chosenIndexes = chosenIndexes;
            this.analyzedAMs = analyzedAMs;
            restoreSelExprs(selExprs, selOpers);
            if (index_access_possible) {
                costAndChooseIndexPlans(leafInput, analyzedAMs);
            }
        } else {
            restoreSelExprs(selExprs, selOpers);
        }
    }

    private int buildHashJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression hashJoinExpr,
            HashJoinExpressionAnnotation hintHashJoin) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost hjCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;
        this.leftJn = leftJn;
        this.rightJn = rightJn;
        int leftPlan = leftJn.cheapestPlanIndex;
        int rightPlan = rightJn.cheapestPlanIndex;

        if (hashJoinExpr == null || hashJoinExpr == ConstantExpression.TRUE) {
            return PlanNode.NO_PLAN;
        }

        if (joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_LEFTDEEP)
                && !leftJn.IsBaseLevelJoinNode()) {
            return PlanNode.NO_PLAN;
        }

        if (joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_RIGHTDEEP)
                && !rightJn.IsBaseLevelJoinNode()) {
            return PlanNode.NO_PLAN;
        }

        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || hintHashJoin != null
                || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG)) {
            // We want to build with the smaller side.
            hjCost = joinEnum.getCostMethodsHandle().costHashJoin(this);
            leftExchangeCost = joinEnum.getCostMethodsHandle().computeHJProbeExchangeCost(this);
            rightExchangeCost = joinEnum.getCostMethodsHandle().computeHJBuildExchangeCost(this);
            childCosts = allPlans.get(leftPlan).totalCost.costAdd(allPlans.get(rightPlan).totalCost);
            totalCost = hjCost.costAdd(leftExchangeCost).costAdd(rightExchangeCost).costAdd(childCosts);
            if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)
                    || hintHashJoin != null) {
                pn = new PlanNode(allPlans.size(), joinEnum);
                pn.setJoinNode(this);
                pn.setLeftJoinIndex(leftJn.jnArrayIndex);
                pn.setRightJoinIndex(rightJn.jnArrayIndex);
                pn.setLeftPlanIndex(leftPlan);
                pn.setRightPlanIndex(rightPlan);
                pn.joinOp = PlanNode.JoinMethod.HYBRID_HASH_JOIN; // need to check that all the conditions have equality predicates ONLY.
                pn.joinHint = hintHashJoin;
                pn.side = HashJoinExpressionAnnotation.BuildSide.RIGHT;
                pn.joinExpr = hashJoinExpr;
                pn.opCost = hjCost;
                pn.totalCost = totalCost;
                pn.leftExchangeCost = leftExchangeCost;
                pn.rightExchangeCost = rightExchangeCost;
                allPlans.add(pn);
                this.planIndexesArray.add(pn.allPlansIndex);
                return pn.allPlansIndex;
            }
        }

        return PlanNode.NO_PLAN;
    }

    private int buildBroadcastHashJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression hashJoinExpr,
            BroadcastExpressionAnnotation hintBroadcastHashJoin) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost bcastHjCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;
        int leftPlan = leftJn.cheapestPlanIndex;
        int rightPlan = rightJn.cheapestPlanIndex;

        if (hashJoinExpr == null || hashJoinExpr == ConstantExpression.TRUE) {
            return PlanNode.NO_PLAN;
        }

        if (joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_LEFTDEEP)
                && !leftJn.IsBaseLevelJoinNode()) {
            return PlanNode.NO_PLAN;
        }

        if (joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_RIGHTDEEP)
                && !rightJn.IsBaseLevelJoinNode()) {
            return PlanNode.NO_PLAN;
        }

        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || hintBroadcastHashJoin != null
                || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG)) {
            // We want to broadcast and build with the smaller side.
            bcastHjCost = joinEnum.getCostMethodsHandle().costBroadcastHashJoin(this);
            leftExchangeCost = joinEnum.getCostHandle().zeroCost();
            rightExchangeCost = joinEnum.getCostMethodsHandle().computeBHJBuildExchangeCost(this);
            childCosts = allPlans.get(leftPlan).totalCost.costAdd(allPlans.get(rightPlan).totalCost);
            totalCost = bcastHjCost.costAdd(rightExchangeCost).costAdd(childCosts);
            if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)
                    || hintBroadcastHashJoin != null) {
                pn = new PlanNode(allPlans.size(), joinEnum);
                pn.setJoinNode(this);
                pn.setLeftJoinIndex(leftJn.jnArrayIndex);
                pn.setRightJoinIndex(rightJn.jnArrayIndex);
                pn.setLeftPlanIndex(leftPlan);
                pn.setRightPlanIndex(rightPlan);
                pn.joinOp = PlanNode.JoinMethod.BROADCAST_HASH_JOIN; // need to check that all the conditions have equality predicates ONLY.
                pn.joinHint = hintBroadcastHashJoin;
                pn.side = HashJoinExpressionAnnotation.BuildSide.RIGHT;
                pn.joinExpr = hashJoinExpr;
                pn.opCost = bcastHjCost;
                pn.totalCost = totalCost;
                pn.leftExchangeCost = leftExchangeCost;
                pn.rightExchangeCost = rightExchangeCost;

                allPlans.add(pn);
                this.planIndexesArray.add(pn.allPlansIndex);
                return pn.allPlansIndex;
            }
        }

        return PlanNode.NO_PLAN;
    }

    private int buildNLJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression nestedLoopJoinExpr,
            IndexedNLJoinExpressionAnnotation hintNLJoin) throws AlgebricksException {
        // Build a nested loops plan, first check if it is possible
        // left right order must be preserved and right side should be a single data set
        List<PlanNode> allPlans = joinEnum.allPlans;
        int numberOfTerms = joinEnum.numberOfTerms;
        PlanNode pn;
        ICost nljCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;
        int leftPlan = leftJn.cheapestPlanIndex;
        int rightPlan = rightJn.cheapestPlanIndex;
        if (rightJn.jnArrayIndex > numberOfTerms) {
            // right side consists of more than one table
            return PlanNode.NO_PLAN; // nested loop plan not possible.
        }

        if (nestedLoopJoinExpr == null || !rightJn.nestedLoopsApplicable(nestedLoopJoinExpr)) {
            return PlanNode.NO_PLAN;
        }

        nljCost = joinEnum.getCostMethodsHandle().costIndexNLJoin(this);
        leftExchangeCost = joinEnum.getCostMethodsHandle().computeNLJOuterExchangeCost(this);
        rightExchangeCost = joinEnum.getCostHandle().zeroCost();
        childCosts = allPlans.get(leftPlan).totalCost;
        totalCost = nljCost.costAdd(leftExchangeCost).costAdd(childCosts);
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)
                || hintNLJoin != null) {
            pn = new PlanNode(allPlans.size(), joinEnum);
            pn.setJoinNode(this);
            pn.setLeftJoinIndex(leftJn.jnArrayIndex);
            pn.setRightJoinIndex(rightJn.jnArrayIndex);
            pn.setLeftPlanIndex(leftPlan);
            pn.setRightPlanIndex(rightPlan);
            pn.joinOp = PlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN;
            pn.joinHint = hintNLJoin;
            pn.joinExpr = nestedLoopJoinExpr; // save it so can be used to add the NESTED annotation in getNewTree.
            pn.opCost = nljCost;
            pn.totalCost = totalCost;
            pn.leftExchangeCost = leftExchangeCost;
            pn.rightExchangeCost = rightExchangeCost;
            allPlans.add(pn);
            this.planIndexesArray.add(pn.allPlansIndex);
            return pn.allPlansIndex;
        }
        return PlanNode.NO_PLAN;
    }

    private int buildCPJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression hashJoinExpr,
            ILogicalExpression nestedLoopJoinExpr) {
        // Now build a cartesian product nested loops plan
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost cpCost, leftExchangeCost, rightExchangeCost, childCosts, totalCost;

        this.leftJn = leftJn;
        this.rightJn = rightJn;
        int leftPlan = leftJn.cheapestPlanIndex;
        int rightPlan = rightJn.cheapestPlanIndex;

        ILogicalExpression cpJoinExpr = null;
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
        childCosts = allPlans.get(leftPlan).totalCost.costAdd(allPlans.get(rightPlan).totalCost);
        totalCost = cpCost.costAdd(rightExchangeCost).costAdd(childCosts);
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)) {
            pn = new PlanNode(allPlans.size(), joinEnum);
            pn.setJoinNode(this);
            pn.setLeftJoinIndex(leftJn.jnArrayIndex);
            pn.setRightJoinIndex(rightJn.jnArrayIndex);
            pn.setLeftPlanIndex(leftPlan);
            pn.setRightPlanIndex(rightPlan);
            pn.joinOp = PlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN;
            pn.joinExpr = Objects.requireNonNullElse(cpJoinExpr, ConstantExpression.TRUE);
            pn.opCost = cpCost;
            pn.totalCost = totalCost;
            pn.leftExchangeCost = leftExchangeCost;
            pn.rightExchangeCost = rightExchangeCost;
            allPlans.add(pn);
            this.planIndexesArray.add(pn.allPlansIndex);
            return pn.allPlansIndex;
        }
        return PlanNode.NO_PLAN;
    }

    protected Pair<Integer, ICost> addMultiDatasetPlans(JoinNode leftJn, JoinNode rightJn) throws AlgebricksException {
        this.leftJn = leftJn;
        this.rightJn = rightJn;
        ICost noJoinCost = joinEnum.getCostHandle().maxCost();

        if (leftJn.planIndexesArray.size() == 0 || rightJn.planIndexesArray.size() == 0) {
            return new Pair<>(PlanNode.NO_PLAN, noJoinCost);
        }

        if (this.cardinality >= Cost.MAX_CARD) {
            return new Pair<>(PlanNode.NO_PLAN, noJoinCost); // no card hint available, so do not add this plan
        }

        List<Integer> newJoinConditions = this.getNewJoinConditionsOnly(); // these will be a subset of applicable join conditions.
        ILogicalExpression hashJoinExpr = joinEnum.getHashJoinExpr(newJoinConditions);
        ILogicalExpression nestedLoopJoinExpr = joinEnum.getNestedLoopJoinExpr(newJoinConditions);

        if ((newJoinConditions.size() == 0) && joinEnum.connectedJoinGraph) {
            // at least one plan must be there at each level as the graph is fully connected.
            if (leftJn.cardinality * rightJn.cardinality > 10000.0) {
                return new Pair<>(PlanNode.NO_PLAN, noJoinCost);
            }
        }

        double current_card = this.cardinality;
        if (current_card >= Cost.MAX_CARD) {
            return new Pair<>(PlanNode.NO_PLAN, noJoinCost); // no card hint available, so do not add this plan
        }

        int hjPlan, commutativeHjPlan, bcastHjPlan, commutativeBcastHjPlan, nljPlan, commutativeNljPlan, cpPlan,
                commutativeCpPlan;
        hjPlan = commutativeHjPlan = bcastHjPlan =
                commutativeBcastHjPlan = nljPlan = commutativeNljPlan = cpPlan = commutativeCpPlan = PlanNode.NO_PLAN;

        HashJoinExpressionAnnotation hintHashJoin = joinEnum.findHashJoinHint(newJoinConditions);
        BroadcastExpressionAnnotation hintBroadcastHashJoin = joinEnum.findBroadcastHashJoinHint(newJoinConditions);
        IndexedNLJoinExpressionAnnotation hintNLJoin = joinEnum.findNLJoinHint(newJoinConditions);

        if (leftJn.cheapestPlanIndex == PlanNode.NO_PLAN || rightJn.cheapestPlanIndex == PlanNode.NO_PLAN) {
            return new Pair<>(PlanNode.NO_PLAN, noJoinCost);
        }

        if (hintHashJoin != null) {
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
                joinEnum.joinHints.put(hintHashJoin, null);
                if ((build && (rightJn.datasetNames.contains(buildOrProbeObject)
                        || rightJn.aliases.contains(buildOrProbeObject)))
                        || (probe && (leftJn.datasetNames.contains(buildOrProbeObject)
                                || leftJn.aliases.contains(buildOrProbeObject)))) {
                    hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr, hintHashJoin);
                } else if ((build && (leftJn.datasetNames.contains(buildOrProbeObject)
                        || leftJn.aliases.contains(buildOrProbeObject)))
                        || (probe && (rightJn.datasetNames.contains(buildOrProbeObject)
                                || rightJn.aliases.contains(buildOrProbeObject)))) {
                    commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr, hintHashJoin);
                }
            }
            if (hjPlan == PlanNode.NO_PLAN && commutativeHjPlan == PlanNode.NO_PLAN) {
                // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
                if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (!joinEnum.joinHints.containsKey(hintHashJoin)) {
                        joinEnum.joinHints.put(hintHashJoin,
                                Warning.of(
                                        joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition
                                                .getSourceLocation(),
                                        ErrorCode.INAPPLICABLE_HINT, "hash join",
                                        (build ? "build " : "probe ") + "with " + buildOrProbeObject));
                    }
                }
                hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
                }
                bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
                }
                nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr, null);
                }
                cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
                }
            }
        } else if (hintBroadcastHashJoin != null) {
            boolean validBroadcastObject = false;
            String broadcastObject = hintBroadcastHashJoin.getName();
            if (broadcastObject != null && (rightJn.datasetNames.contains(broadcastObject)
                    || rightJn.aliases.contains(broadcastObject) || leftJn.datasetNames.contains(broadcastObject)
                    || leftJn.aliases.contains(broadcastObject))) {
                validBroadcastObject = true;
            }
            if (validBroadcastObject) {
                joinEnum.joinHints.put(hintBroadcastHashJoin, null);
                if (rightJn.datasetNames.contains(broadcastObject) || rightJn.aliases.contains(broadcastObject)) {
                    bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr, hintBroadcastHashJoin);
                } else if (leftJn.datasetNames.contains(broadcastObject) || leftJn.aliases.contains(broadcastObject)) {
                    commutativeBcastHjPlan =
                            buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr, hintBroadcastHashJoin);
                }
            } else if (broadcastObject == null) {
                joinEnum.joinHints.put(hintBroadcastHashJoin, null);
                bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr, hintBroadcastHashJoin);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeBcastHjPlan =
                            buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr, hintBroadcastHashJoin);
                }
            }
            if (bcastHjPlan == PlanNode.NO_PLAN && commutativeBcastHjPlan == PlanNode.NO_PLAN) {
                // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
                if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (!joinEnum.joinHints.containsKey(hintBroadcastHashJoin)) {
                        joinEnum.joinHints.put(hintBroadcastHashJoin,
                                Warning.of(
                                        joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition
                                                .getSourceLocation(),
                                        ErrorCode.INAPPLICABLE_HINT, "broadcast hash join",
                                        "broadcast " + broadcastObject));
                    }
                }

                hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
                }
                bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
                }
                nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr, null);
                }
                cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
                }
            }
        } else if (hintNLJoin != null) {
            joinEnum.joinHints.put(hintNLJoin, null);
            nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr, hintNLJoin);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr, hintNLJoin);
            }
            if (nljPlan == PlanNode.NO_PLAN && commutativeNljPlan == PlanNode.NO_PLAN) {
                // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
                if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (!joinEnum.joinHints.containsKey(hintNLJoin)) {
                        joinEnum.joinHints.put(hintNLJoin,
                                Warning.of(
                                        joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition
                                                .getSourceLocation(),
                                        ErrorCode.INAPPLICABLE_HINT, "index nested loop join", "ignored"));
                    }
                }
                hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
                }
                bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
                }
                cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
                }
            }
        } else {
            hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
            }
            bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr, null);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr, null);
            }
            nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr, null);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr, null);
            }
            cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
            }
        }

        if (hjPlan == PlanNode.NO_PLAN && commutativeHjPlan == PlanNode.NO_PLAN && bcastHjPlan == PlanNode.NO_PLAN
                && commutativeBcastHjPlan == PlanNode.NO_PLAN && nljPlan == PlanNode.NO_PLAN
                && commutativeNljPlan == PlanNode.NO_PLAN && cpPlan == PlanNode.NO_PLAN
                && commutativeCpPlan == PlanNode.NO_PLAN) {
            return new Pair<>(PlanNode.NO_PLAN, noJoinCost);
        }

        //Reset as these might have changed when we tried the commutative joins.
        this.leftJn = leftJn;
        this.rightJn = rightJn;

        PlanNode cheapestPlan = findCheapestPlan();
        this.cheapestPlanCost = cheapestPlan.totalCost;
        this.cheapestPlanIndex = cheapestPlan.allPlansIndex;

        return new Pair<>(this.cheapestPlanIndex, this.cheapestPlanCost);
    }

    private PlanNode findCheapestPlan() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost cheapestCost = joinEnum.getCostHandle().maxCost();
        PlanNode cheapestPlanNode = null;
        IExpressionAnnotation cheapestPlanJoinHint = null;

        for (int planIndex : this.planIndexesArray) {
            PlanNode plan = allPlans.get(planIndex);
            if (plan.joinHint != null && cheapestPlanJoinHint == null) {
                // The hinted plan wins!
                cheapestPlanNode = plan;
                cheapestCost = plan.totalCost;
                cheapestPlanJoinHint = plan.joinHint;
            } else if (plan.joinHint != null || cheapestPlanJoinHint == null) {
                // Either both plans are hinted, or both are non-hinted.
                // Cost is the decider.
                if (plan.totalCost.costLT(cheapestCost)) {
                    cheapestPlanNode = plan;
                    cheapestCost = plan.totalCost;
                    cheapestPlanJoinHint = plan.joinHint;
                }
            }
        }
        return cheapestPlanNode;
    }

    @Override
    public String toString() {
        if (planIndexesArray.isEmpty()) {
            return "";
        }
        List<PlanNode> allPlans = joinEnum.allPlans;
        StringBuilder sb = new StringBuilder(128);
        // This will avoid printing JoinNodes that have no plans
        sb.append("Printing Join Node ").append(jnArrayIndex).append('\n');
        sb.append("datasetNames ").append('\n');
        for (String datasetName : datasetNames) {
            // Need to not print newline
            sb.append(datasetName).append(' ');
        }
        sb.append("datasetIndex ").append('\n');
        for (int j = 0; j < datasetIndexes.size(); j++) {
            sb.append(j).append(datasetIndexes.get(j)).append('\n');
        }
        sb.append("datasetBits is ").append(datasetBits).append('\n');
        if (IsBaseLevelJoinNode()) {
            sb.append("orig cardinality is ").append((double) Math.round(origCardinality * 100) / 100).append('\n');
        }
        sb.append("cardinality is ").append((double) Math.round(cardinality * 100) / 100).append('\n');
        if (planIndexesArray.size() == 0) {
            sb.append("No plans considered for this join node").append('\n');
        }
        for (int j = 0; j < planIndexesArray.size(); j++) {
            int k = planIndexesArray.get(j);
            PlanNode pn = allPlans.get(k);
            sb.append("planIndexesArray  [").append(j).append("] is ").append(k).append('\n');
            sb.append("Printing PlanNode ").append(k).append('\n');
            if (IsBaseLevelJoinNode()) {
                sb.append("DATA_SOURCE_SCAN").append('\n');
            } else {
                sb.append("\n");
                sb.append(pn.joinMethod().getFirst()).append('\n');
                sb.append("Printing Join expr ").append('\n');
                if (pn.joinExpr != null) {
                    sb.append(pn.joinExpr).append('\n');
                } else {
                    sb.append("null").append('\n');
                }
            }
            sb.append("card ").append((double) Math.round(cardinality * 100) / 100).append('\n');
            sb.append("------------------").append('\n');
            sb.append("operator cost ").append(pn.opCost.computeTotalCost()).append('\n');
            sb.append("total cost ").append(pn.totalCost.computeTotalCost()).append('\n');
            sb.append("jnIndexes ").append(pn.jnIndexes[0]).append(" ").append(pn.jnIndexes[1]).append('\n');
            if (IsHigherLevelJoinNode()) {
                PlanNode leftPlan = pn.getLeftPlanNode();
                PlanNode rightPlan = pn.getRightPlanNode();
                int l = leftPlan.allPlansIndex;
                int r = rightPlan.allPlansIndex;
                sb.append("planIndexes ").append(l).append(" ").append(r).append('\n');
                sb.append("(lcost = ").append(leftPlan.totalCost.computeTotalCost()).append(") (rcost = ")
                        .append(rightPlan.totalCost.computeTotalCost()).append(")").append('\n');
            }
            sb.append("\n");
        }
        sb.append("jnIndex ").append(jnIndex).append('\n');
        sb.append("datasetBits ").append(datasetBits).append('\n');
        sb.append("cardinality ").append((double) Math.round(cardinality * 100) / 100).append('\n');
        sb.append("size ").append((double) Math.round(size * 100) / 100).append('\n');
        sb.append("level ").append(level).append('\n');
        sb.append("highestDatasetId ").append(highestDatasetId).append('\n');
        sb.append("--------------------------------------").append('\n');
        return sb.toString();
    }

    protected void printCostOfAllPlans(StringBuilder sb) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost minCost = joinEnum.getCostHandle().maxCost();
        for (int planIndex : planIndexesArray) {
            ICost planCost = allPlans.get(planIndex).totalCost;
            sb.append("plan ").append(planIndex).append(" cost is ").append(planCost.computeTotalCost()).append('\n');
            if (planCost.costLT(minCost)) {
                minCost = planCost;
            }
        }
        sb.append("LOWEST COST ").append(minCost.computeTotalCost()).append('\n');
    }
}
