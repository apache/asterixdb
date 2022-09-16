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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
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
    protected String alias;
    protected int cheapestPlanIndex;
    protected ICost cheapestPlanCost;
    protected double origCardinality; // without any selections
    protected double cardinality;
    protected double size;
    protected List<Integer> planIndexesArray; // indexes into the PlanNode array in enumerateJoins
    protected int jnIndex, level, highestDatasetId;
    protected JoinNode rightJn, leftJn;
    protected List<Integer> applicableJoinConditions;
    protected EmptyTupleSourceOperator correspondingEmptyTupleSourceOp; // There is a 1-1 relationship between the LVs and the dataSourceScanOps and the leafInputs.
    protected List<Pair<IAccessMethod, Index>> chosenIndexes;
    protected Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs;
    protected Index.SampleIndexDetails idxDetails;
    protected static int NO_JN = -1;
    protected static int NO_CARDS = -2;

    public JoinNode(int i) {
        this.jnArrayIndex = i;
        planIndexesArray = new ArrayList<>();
        cheapestPlanIndex = PlanNode.NO_PLAN;
        size = 1; // for now, will be the size of the doc for this joinNode
    }

    public JoinNode(int i, JoinEnum joinE) {
        this(i);
        joinEnum = joinE;
        cheapestPlanCost = joinEnum.getCostHandle().maxCost();
    }

    public boolean IsBaseLevelJoinNode() {
        if (this.jnArrayIndex <= joinEnum.numberOfTerms) {
            return true;
        }
        return false;
    }

    public boolean IsHigherLevelJoinNode() {
        return !IsBaseLevelJoinNode();
    }

    public double computeJoinCardinality() {
        JoinNode[] jnArray = joinEnum.getJnArray();
        List<JoinCondition> joinConditions = joinEnum.getJoinConditions();

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
        return productJoinCardinality * productJoinSels;
    }

    public double getCardinality() {
        return cardinality;
    }

    public void setCardinality(double card) {
        cardinality = card;
    }

    public double getOrigCardinality() {
        return origCardinality;
    }

    public void setOrigCardinality(double card) {
        origCardinality = card;
    }

    public void setAvgDocSize(double avgDocSize) {
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

    public String getAlias() {
        return alias;
    }

    public List<String> getDatasetNames() {
        return datasetNames;
    }

    public Index.SampleIndexDetails getIdxDetails() {
        return idxDetails;
    }

    protected boolean nestedLoopsApplicable(ILogicalExpression joinExpr) throws AlgebricksException {

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
        boolean temp = tmp.checkApplicable(new MutableObject<>(joinEnum.localJoinOp), joinEnum.optCtx);

        return temp;
    }

    /** one is a subset of two */
    private boolean subset(int one, int two) {
        return (one & two) == one;
    }

    protected void findApplicableJoinConditions() {
        List<JoinCondition> joinConditions = joinEnum.getJoinConditions();

        int i = 0;
        for (JoinCondition jc : joinConditions) {
            if (subset(jc.datasetBits, this.datasetBits)) {
                this.applicableJoinConditions.add(i);
            }
            i++;
        }
    }

    protected List<Integer> getNewJoinConditionsOnly() {
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

    public int addSingleDatasetPlans() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost opCost, totalCost;

        opCost = joinEnum.getCostMethodsHandle().costFullScan(this);
        totalCost = opCost;
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || opCost.costLT(this.cheapestPlanCost)) {
            // for now just add one plan
            PlanNode pn = new PlanNode(allPlans.size(), joinEnum);
            pn.jn = this;
            pn.datasetName = this.datasetNames.get(0);
            pn.correspondingEmptyTupleSourceOp = this.correspondingEmptyTupleSourceOp;
            pn.jnIndexes[0] = this.jnArrayIndex;
            pn.jnIndexes[1] = JoinNode.NO_JN;
            pn.planIndexes[0] = PlanNode.NO_PLAN; // There ane no plans below this plan.
            pn.planIndexes[1] = PlanNode.NO_PLAN; // There ane no plans below this plan.
            pn.opCost = opCost;
            pn.scanOp = PlanNode.ScanMethod.TABLE_SCAN;
            pn.totalCost = totalCost;

            allPlans.add(pn);
            this.planIndexesArray.add(allPlans.size() - 1);
            this.cheapestPlanCost = totalCost;
            this.cheapestPlanIndex = allPlans.size() - 1;
            return this.cheapestPlanIndex;
        }
        return PlanNode.NO_PLAN;
    }

    protected void buildIndexPlan() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost opCost, totalCost;

        opCost = joinEnum.getCostMethodsHandle().costIndexScan(this);
        totalCost = opCost;
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || opCost.costLT(this.cheapestPlanCost)) {
            // for now just add one plan
            PlanNode pn = new PlanNode(allPlans.size(), joinEnum);
            pn.jn = this;
            pn.datasetName = this.datasetNames.get(0);
            pn.correspondingEmptyTupleSourceOp = this.correspondingEmptyTupleSourceOp;
            pn.jnIndexes[0] = this.jnArrayIndex;
            pn.jnIndexes[1] = JoinNode.NO_JN;
            pn.planIndexes[0] = PlanNode.NO_PLAN; // There ane no plans below this plan.
            pn.planIndexes[1] = PlanNode.NO_PLAN; // There ane no plans below this plan.
            pn.opCost = opCost;
            pn.scanOp = PlanNode.ScanMethod.INDEX_SCAN;
            pn.totalCost = totalCost;

            allPlans.add(pn);
            this.planIndexesArray.add(allPlans.size() - 1);
            this.cheapestPlanCost = totalCost;
            this.cheapestPlanIndex = allPlans.size() - 1;
        }
    }

    protected void costAndChooseIndexPlans(ILogicalOperator leafInput,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) throws AlgebricksException {
        // Skip indexes with selectivity greater than 0.1, add the SKIP_SECONDARY_INDEX annotation to its expression.
        double sel;
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry = amIt.next();
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                    analysisCtx.getIteratorForIndexExprsAndVars();
            List<IOptimizableFuncExpr> exprs = analysisCtx.getMatchedFuncExprs();
            int exprIndex = 0;
            while (indexIt.hasNext()) {
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
                Index chosenIndex = indexEntry.getKey();
                IOptimizableFuncExpr expr = exprs.get(exprIndex++);
                AbstractFunctionCallExpression afce = expr.getFuncExpr();
                PredicateCardinalityAnnotation selectivityAnnotation =
                        afce.getAnnotation(PredicateCardinalityAnnotation.class);
                if (selectivityAnnotation != null) {
                    sel = selectivityAnnotation.getSelectivity();
                    if (sel >= joinEnum.stats.SELECTIVITY_FOR_SECONDARY_INDEX_SELECTION) {
                        afce.putAnnotation(SkipSecondaryIndexSearchExpressionAnnotation
                                .newInstance(Collections.singleton(chosenIndex.getIndexName())));
                    } else {
                        buildIndexPlan();
                    }
                }
            }
        }
    }

    public void addIndexAccessPlans(ILogicalOperator leafInput) throws AlgebricksException {
        IntroduceSelectAccessMethodRule tmp = new IntroduceSelectAccessMethodRule();
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        boolean index_access_possible =
                tmp.checkApplicable(new MutableObject<>(leafInput), joinEnum.optCtx, chosenIndexes, analyzedAMs);
        this.chosenIndexes = chosenIndexes;
        this.analyzedAMs = analyzedAMs;
        if (index_access_possible) {
            costAndChooseIndexPlans(leafInput, analyzedAMs);
        }
    }

    protected int buildHashJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression hashJoinExpr) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost hjCost, childCosts, totalCost;

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

        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG)) {
            // We want to build with the smaller side.
            hjCost = joinEnum.getCostMethodsHandle().costHashJoin(this);
            childCosts = allPlans.get(leftPlan).totalCost.costAdd(allPlans.get(rightPlan).totalCost);
            totalCost = hjCost.costAdd(childCosts);
            if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)) {
                pn = new PlanNode(allPlans.size(), joinEnum);
                pn.jn = this;
                pn.jnIndexes[0] = leftJn.jnArrayIndex;
                pn.jnIndexes[1] = rightJn.jnArrayIndex;
                pn.planIndexes[0] = leftPlan;
                pn.planIndexes[1] = rightPlan;
                pn.joinOp = PlanNode.JoinMethod.HYBRID_HASH_JOIN; // need to check that all the conditions have equality predicates ONLY.
                pn.side = HashJoinExpressionAnnotation.BuildSide.RIGHT;
                pn.joinExpr = hashJoinExpr;
                pn.opCost = hjCost;
                pn.totalCost = totalCost;
                pn.leftExchangeCost = joinEnum.getCostMethodsHandle().computeHJProbeExchangeCost(this);
                pn.rightExchangeCost = joinEnum.getCostMethodsHandle().computeHJBuildExchangeCost(this);

                allPlans.add(pn);
                this.planIndexesArray.add(allPlans.size() - 1);
                this.cheapestPlanCost = totalCost;
                this.cheapestPlanIndex = allPlans.size() - 1;
                return this.cheapestPlanIndex;
            }
        }

        return PlanNode.NO_PLAN;
    }

    protected int buildBroadcastHashJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression hashJoinExpr) {
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost bcastHjCost, childCosts, totalCost;

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

        if (rightJn.cardinality * rightJn.size <= leftJn.cardinality * leftJn.size || joinEnum.forceJoinOrderMode
                || !joinEnum.queryPlanShape.equals(AlgebricksConfig.QUERY_PLAN_SHAPE_ZIGZAG)) {
            // We want to broadcast and build with the smaller side.
            bcastHjCost = joinEnum.getCostMethodsHandle().costBroadcastHashJoin(this);
            childCosts = allPlans.get(leftPlan).totalCost.costAdd(allPlans.get(rightPlan).totalCost);
            totalCost = bcastHjCost.costAdd(childCosts);
            if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)) {
                pn = new PlanNode(allPlans.size(), joinEnum);
                pn.jn = this;
                pn.jnIndexes[0] = leftJn.jnArrayIndex;
                pn.jnIndexes[1] = rightJn.jnArrayIndex;
                pn.planIndexes[0] = leftPlan;
                pn.planIndexes[1] = rightPlan;
                pn.joinOp = PlanNode.JoinMethod.BROADCAST_HASH_JOIN; // need to check that all the conditions have equality predicates ONLY.
                pn.side = HashJoinExpressionAnnotation.BuildSide.RIGHT;
                pn.joinExpr = hashJoinExpr;
                pn.opCost = bcastHjCost;
                pn.totalCost = totalCost;
                pn.leftExchangeCost = joinEnum.getCostHandle().zeroCost();
                pn.rightExchangeCost = joinEnum.getCostMethodsHandle().computeBHJBuildExchangeCost(this);

                allPlans.add(pn);
                this.planIndexesArray.add(allPlans.size() - 1);
                this.cheapestPlanCost = totalCost;
                this.cheapestPlanIndex = allPlans.size() - 1;
                return this.cheapestPlanIndex;
            }
        }

        return PlanNode.NO_PLAN;
    }

    protected int buildNLJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression nestedLoopJoinExpr)
            throws AlgebricksException {
        // Build a nested loops plan, first check if it is possible
        // left right order must be preserved and right side should be a single data set
        List<PlanNode> allPlans = joinEnum.allPlans;
        int numberOfTerms = joinEnum.numberOfTerms;
        PlanNode pn;
        ICost nljCost, childCosts, totalCost;

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
        childCosts = allPlans.get(leftPlan).totalCost;
        totalCost = nljCost.costAdd(childCosts);
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)) {
            pn = new PlanNode(allPlans.size(), joinEnum);
            pn.jn = this;
            pn.jnIndexes[0] = leftJn.jnArrayIndex;
            pn.jnIndexes[1] = rightJn.jnArrayIndex;
            pn.planIndexes[0] = leftPlan;
            pn.planIndexes[1] = rightPlan;
            pn.joinOp = PlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN;
            pn.joinExpr = nestedLoopJoinExpr; // save it so can be used to add the NESTED annotation in getNewTree.
            pn.opCost = nljCost;
            pn.totalCost = totalCost;
            pn.leftExchangeCost = joinEnum.getCostMethodsHandle().computeNLJOuterExchangeCost(this);
            pn.rightExchangeCost = joinEnum.getCostHandle().zeroCost();

            allPlans.add(pn);
            this.planIndexesArray.add(allPlans.size() - 1);
            this.cheapestPlanCost = totalCost;
            this.cheapestPlanIndex = allPlans.size() - 1;
            return allPlans.size() - 1;
        }
        return PlanNode.NO_PLAN;
    }

    protected int buildCPJoinPlan(JoinNode leftJn, JoinNode rightJn, ILogicalExpression hashJoinExpr,
            ILogicalExpression nestedLoopJoinExpr) {
        // Now build a cartesian product nested loops plan
        List<PlanNode> allPlans = joinEnum.allPlans;
        PlanNode pn;
        ICost cpCost, childCosts, totalCost;

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
        } else if (hashJoinExpr == null && nestedLoopJoinExpr != null) {
            cpJoinExpr = nestedLoopJoinExpr;
        } else if (Objects.equals(hashJoinExpr, nestedLoopJoinExpr) == true) {
            cpJoinExpr = hashJoinExpr;
        } else if (Objects.equals(hashJoinExpr, nestedLoopJoinExpr) == false) {
            ScalarFunctionCallExpression andExpr = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND));
            andExpr.getArguments().add(new MutableObject<>(hashJoinExpr));
            andExpr.getArguments().add(new MutableObject<>(nestedLoopJoinExpr));
            cpJoinExpr = andExpr;
        }

        cpCost = joinEnum.getCostMethodsHandle().costCartesianProductJoin(this);
        childCosts = allPlans.get(leftPlan).totalCost.costAdd(allPlans.get(rightPlan).totalCost);
        totalCost = cpCost.costAdd(childCosts);
        if (this.cheapestPlanIndex == PlanNode.NO_PLAN || totalCost.costLT(this.cheapestPlanCost)) {
            pn = new PlanNode(allPlans.size(), joinEnum);
            pn.jn = this;
            pn.jnIndexes[0] = leftJn.jnArrayIndex;
            pn.jnIndexes[1] = rightJn.jnArrayIndex;
            pn.planIndexes[0] = leftPlan;
            pn.planIndexes[1] = rightPlan;
            pn.joinOp = PlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN;
            pn.joinExpr = Objects.requireNonNullElse(cpJoinExpr, ConstantExpression.TRUE);
            pn.opCost = cpCost;
            pn.totalCost = totalCost;
            pn.leftExchangeCost = joinEnum.getCostHandle().zeroCost();
            pn.rightExchangeCost = joinEnum.getCostMethodsHandle().computeCPRightExchangeCost(this);

            allPlans.add(pn);
            this.planIndexesArray.add(allPlans.size() - 1);
            this.cheapestPlanCost = totalCost;
            this.cheapestPlanIndex = allPlans.size() - 1;
            return allPlans.size() - 1;
        }
        return PlanNode.NO_PLAN;
    }

    protected Pair<Integer, ICost> addMultiDatasetPlans(JoinNode leftJn, JoinNode rightJn, int level)
            throws AlgebricksException {
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

        HashJoinExpressionAnnotation.BuildSide hintHashJoin = joinEnum.findHashJoinHint(newJoinConditions);
        BroadcastExpressionAnnotation.BroadcastSide hintBroadcastHashJoin = null;
        boolean hintNLJoin = false;
        if (hintHashJoin == null) {
            hintBroadcastHashJoin = joinEnum.findBroadcastHashJoinHint(newJoinConditions);
            if (hintBroadcastHashJoin == null) {
                hintNLJoin = joinEnum.findNLJoinHint(newJoinConditions);
            }
        }

        if (leftJn.cheapestPlanIndex == PlanNode.NO_PLAN || rightJn.cheapestPlanIndex == PlanNode.NO_PLAN) {
            return new Pair<>(PlanNode.NO_PLAN, noJoinCost);
        }

        if (hintHashJoin != null) {
            hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr);
            if (!joinEnum.forceJoinOrderMode && hintHashJoin != HashJoinExpressionAnnotation.BuildSide.RIGHT) {
                commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr);
            }
            if (hjPlan == PlanNode.NO_PLAN && commutativeHjPlan == PlanNode.NO_PLAN) {
                // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
                if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(
                                joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition
                                        .getSourceLocation(),
                                ErrorCode.INAPPLICABLE_HINT, "Hash join hint not applicable and was ignored"));
                    }
                }
                bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr);
                }
                nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr);
                }
                cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
                }
            }
        } else if (hintBroadcastHashJoin != null) {
            bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr);
            if (!joinEnum.forceJoinOrderMode
                    && hintBroadcastHashJoin != BroadcastExpressionAnnotation.BroadcastSide.RIGHT) {
                commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr);
            }
            if (bcastHjPlan == PlanNode.NO_PLAN && commutativeBcastHjPlan == PlanNode.NO_PLAN) {
                // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
                if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(
                                joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition
                                        .getSourceLocation(),
                                ErrorCode.INAPPLICABLE_HINT,
                                "Broadcast hash join hint not applicable and was ignored"));
                    }
                }

                hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr);
                }
                nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr);
                }
                cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
                }
            }
        } else if (hintNLJoin) {
            nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr);
            }
            if (nljPlan == PlanNode.NO_PLAN && commutativeNljPlan == PlanNode.NO_PLAN) {
                // Hints are attached to predicates, so newJoinConditions should not be empty, but adding the check to be safe.
                if (!joinEnum.getJoinConditions().isEmpty() && !newJoinConditions.isEmpty()) {
                    IWarningCollector warningCollector = joinEnum.optCtx.getWarningCollector();
                    if (warningCollector.shouldWarn()) {
                        warningCollector.warn(Warning.of(
                                joinEnum.getJoinConditions().get(newJoinConditions.get(0)).joinCondition
                                        .getSourceLocation(),
                                ErrorCode.INAPPLICABLE_HINT, "Index nested join hint not applicable and was ignored"));
                    }
                }
                hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr);
                }
                bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr);
                }
                cpPlan = buildCPJoinPlan(leftJn, rightJn, hashJoinExpr, nestedLoopJoinExpr);
                if (!joinEnum.forceJoinOrderMode) {
                    commutativeCpPlan = buildCPJoinPlan(rightJn, leftJn, hashJoinExpr, nestedLoopJoinExpr);
                }
            }
        } else {
            hjPlan = buildHashJoinPlan(leftJn, rightJn, hashJoinExpr);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeHjPlan = buildHashJoinPlan(rightJn, leftJn, hashJoinExpr);
            }
            bcastHjPlan = buildBroadcastHashJoinPlan(leftJn, rightJn, hashJoinExpr);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeBcastHjPlan = buildBroadcastHashJoinPlan(rightJn, leftJn, hashJoinExpr);
            }
            nljPlan = buildNLJoinPlan(leftJn, rightJn, nestedLoopJoinExpr);
            if (!joinEnum.forceJoinOrderMode) {
                commutativeNljPlan = buildNLJoinPlan(rightJn, leftJn, nestedLoopJoinExpr);
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

        return new Pair<>(this.cheapestPlanIndex, this.cheapestPlanCost);
    }

    protected int findCheapestPlan() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        ICost cheapestCost = joinEnum.getCostHandle().maxCost();
        int cheapestIndex = PlanNode.NO_PLAN;

        for (int planIndex : this.planIndexesArray) {
            if (allPlans.get(planIndex).totalCost.costLT(cheapestCost)) {
                cheapestCost = allPlans.get(planIndex).totalCost;
                cheapestIndex = planIndex;
            }
        }
        return cheapestIndex;
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
        for (int j = 0; j < datasetNames.size(); j++) {
            // Need to not print newline
            sb.append(datasetNames.get(j)).append(' ');
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
            sb.append("Printing PlanNode ").append(k);
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

    public void printCostOfAllPlans() {
        List<PlanNode> allPlans = joinEnum.allPlans;
        for (int planIndex : planIndexesArray) {
            LOGGER.trace("plan " + planIndex + " cost is " + allPlans.get(planIndex).totalCost.computeTotalCost());
        }
    }
}
