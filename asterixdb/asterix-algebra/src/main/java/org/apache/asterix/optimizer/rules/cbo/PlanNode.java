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

import java.util.List;

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class PlanNode {

    protected static int NO_PLAN = -1;

    private final JoinEnum joinEnum;

    protected String datasetName;
    protected ILogicalOperator leafInput;

    protected JoinNode jn;
    protected boolean outerJoin;
    protected int allPlansIndex;
    protected int[] jnIndexes;
    protected int[] planIndexes;

    protected ScanMethod scanOp;
    protected boolean indexHint;
    Index indexUsed;

    protected JoinMethod joinOp;

    protected ILogicalExpression joinExpr;

    Pair<AbstractFunctionCallExpression, IndexedNLJoinExpressionAnnotation> exprAndHint;

    // Used to indicate which side to build for HJ and which side to broadcast for BHJ.
    protected HashJoinExpressionAnnotation.BuildSide side;
    protected IExpressionAnnotation joinHint;
    protected int numHintsUsed;
    protected ICost opCost;
    protected ICost totalCost;
    protected ICost leftExchangeCost;
    protected ICost rightExchangeCost;

    public enum ScanMethod {
        INDEX_SCAN,
        TABLE_SCAN
    }

    protected enum JoinMethod {
        HYBRID_HASH_JOIN,
        BROADCAST_HASH_JOIN,
        INDEX_NESTED_LOOP_JOIN,
        CARTESIAN_PRODUCT_JOIN
    }

    public int getIndex() {
        return allPlansIndex;
    }

    public Index getSoleAccessIndex() {
        return indexUsed;
    }

    private int[] getPlanIndexes() {
        return planIndexes;
    }

    public PlanNode getLeftPlanNode() {
        if (planIndexes[0] == NO_PLAN) {
            return null;
        }
        return joinEnum.allPlans.get(planIndexes[0]);
    }

    public PlanNode getRightPlanNode() {
        if (planIndexes[1] == NO_PLAN) {
            return null;
        }
        return joinEnum.allPlans.get(planIndexes[1]);
    }

    protected JoinNode getJoinNode() {
        return jn;
    }

    protected void setJoinNode(JoinNode jn) {
        this.jn = jn;
    }

    public int getLeftJoinIndex() {
        return jnIndexes[0];
    }

    protected void setLeftJoinIndex(int index) {
        this.jnIndexes[0] = index;
    }

    public int getRightJoinIndex() {
        return jnIndexes[1];
    }

    protected void setRightJoinIndex(int index) {
        this.jnIndexes[1] = index;
    }

    protected int getLeftPlanIndex() {
        return planIndexes[0];
    }

    protected void setLeftPlanIndex(int index) {
        this.planIndexes[0] = index;
    }

    protected int getRightPlanIndex() {
        return planIndexes[1];
    }

    protected void setRightPlanIndex(int index) {
        this.planIndexes[1] = index;
    }

    public boolean IsScanNode() {
        return getLeftPlanIndex() == NO_PLAN && getRightPlanIndex() == NO_PLAN;
    }

    public boolean IsJoinNode() {
        return getLeftPlanIndex() != NO_PLAN && getRightPlanIndex() != NO_PLAN;
    }

    public Pair<String, String> joinMethod() {
        if (this.joinOp == PlanNode.JoinMethod.HYBRID_HASH_JOIN) {
            return new Pair<>("HASH JOIN", "HJ");
        } else if (this.joinOp == PlanNode.JoinMethod.BROADCAST_HASH_JOIN) {
            return new Pair<>("BROADCAST HASH JOIN", "BHJ");
        } else if (this.joinOp == PlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN) {
            return new Pair<>("INDEX NESTED LOOPS JOIN", "INLJ");
        } else if (this.joinOp == PlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN) {
            return new Pair<>("CARTESIAN PRODUCT JOIN", "CPJ");
        }
        return new Pair<>("", "");
    }

    private String getDatasetName() {
        return datasetName;
    }

    protected void setDatasetName(String dsName) {
        this.datasetName = dsName;
    }

    protected ILogicalOperator getLeafInput() {
        return leafInput; // This applies only to singleDataSetPlans
    }

    protected void setLeafInput(ILogicalOperator leafInput) {
        this.leafInput = leafInput; // This applies only to singleDataSetPlans
    }

    public ICost getOpCost() {
        return opCost;
    }

    protected void setOpCost(ICost cost) {
        this.opCost = cost;
    }

    protected double computeOpCost() {
        return opCost.computeTotalCost();
    }

    public ICost getTotalCost() {
        return totalCost;
    }

    protected void setTotalCost(ICost tc) {
        this.totalCost = tc;
    }

    public ICost getLeftExchangeCost() {
        return leftExchangeCost;
    }

    public ICost getRightExchangeCost() {
        return rightExchangeCost;
    }

    protected double computeTotalCost() {
        return totalCost.computeTotalCost();
    }

    public ScanMethod getScanOp() {
        return scanOp;
    }

    protected void setScanMethod(ScanMethod sm) {
        this.scanOp = sm;
    }

    protected JoinMethod getJoinOp() {
        return joinOp;
    }

    public ILogicalExpression getJoinExpr() {
        return joinExpr;
    }

    // Constructor for DUMMY plan nodes.
    public PlanNode(int planIndex, JoinEnum joinE) {
        this.allPlansIndex = planIndex;
        joinEnum = joinE;
        jn = null;
        planIndexes = new int[2]; // 0 is for left, 1 is for right
        jnIndexes = new int[2]; // join node index(es)
        indexUsed = null;
        setLeftJoinIndex(JoinNode.NO_JN);
        setRightJoinIndex(JoinNode.NO_JN);
        setLeftPlanIndex(PlanNode.NO_PLAN);
        setRightPlanIndex(PlanNode.NO_PLAN);
        opCost = totalCost = joinEnum.getCostHandle().zeroCost();
        outerJoin = false;
        indexHint = false;
        joinHint = null;
        numHintsUsed = 0;
    }

    // Constructor for SCAN plan nodes.
    public PlanNode(int planIndex, JoinEnum joinE, JoinNode joinNode, String datasetName, ILogicalOperator leafInput) {
        this.allPlansIndex = planIndex;
        joinEnum = joinE;
        setJoinNode(joinNode);
        this.datasetName = datasetName;
        this.leafInput = leafInput;
        planIndexes = new int[2]; // 0 is for left, 1 is for right
        jnIndexes = new int[2]; // join node index(es)
        indexUsed = null;
        setLeftJoinIndex(jn.jnArrayIndex);
        setRightJoinIndex(JoinNode.NO_JN);
        setLeftPlanIndex(PlanNode.NO_PLAN); // There ane no plans below this plan.
        setRightPlanIndex(PlanNode.NO_PLAN); // There ane no plans below this plan.
        indexHint = false;
        joinHint = null;
        numHintsUsed = 0;
    }

    // Constructor for JOIN plan nodes.
    public PlanNode(int planIndex, JoinEnum joinE, JoinNode joinNode, PlanNode leftPlan, PlanNode rightPlan,
            boolean outerJoin) {
        this.allPlansIndex = planIndex;
        joinEnum = joinE;
        setJoinNode(joinNode);
        this.outerJoin = outerJoin;
        planIndexes = new int[2]; // 0 is for left, 1 is for right
        jnIndexes = new int[2]; // join node index(es)
        indexUsed = null; // used for NL costing
        setLeftJoinIndex(leftPlan.jn.jnArrayIndex);
        setRightJoinIndex(rightPlan.jn.jnArrayIndex);
        setLeftPlanIndex(leftPlan.allPlansIndex);
        setRightPlanIndex(rightPlan.allPlansIndex);
        indexHint = false;
        joinHint = null;
        numHintsUsed = 0;
    }

    protected void setScanAndHintInfo(ScanMethod scanMethod,
            List<Triple<Index, Double, AbstractFunctionCallExpression>> mandatoryIndexesInfo,
            List<Triple<Index, Double, AbstractFunctionCallExpression>> optionalIndexesInfo) {
        setScanMethod(scanMethod);
        if (mandatoryIndexesInfo.size() > 0) {
            indexHint = true;
            numHintsUsed = 1;
        }
        // keeping things simple. When multiple indexes are used, we cannot be sure of the order.
        // So seeing if only index is used.
        if (optionalIndexesInfo.size() + mandatoryIndexesInfo.size() == 1) {
            if (optionalIndexesInfo.size() == 1) {
                indexUsed = optionalIndexesInfo.get(0).first;
            } else {
                indexUsed = mandatoryIndexesInfo.get(0).first;
            }
        }
    }

    protected void setScanCosts(ICost opCost) {
        this.opCost = opCost;
        this.totalCost = opCost;
    }

    protected void setJoinAndHintInfo(JoinMethod joinMethod, ILogicalExpression joinExpr,
            Pair<AbstractFunctionCallExpression, IndexedNLJoinExpressionAnnotation> exprAndHint,
            HashJoinExpressionAnnotation.BuildSide side, IExpressionAnnotation hint) {
        joinOp = joinMethod;
        this.joinExpr = joinExpr;
        this.exprAndHint = exprAndHint;
        this.side = side;
        joinHint = hint;
        numHintsUsed = joinEnum.allPlans.get(getLeftPlanIndex()).numHintsUsed
                + joinEnum.allPlans.get(getRightPlanIndex()).numHintsUsed;
        if (hint != null) {
            numHintsUsed++;
        }
    }

    protected void setJoinCosts(ICost opCost, ICost totalCost, ICost leftExchangeCost, ICost rightExchangeCost) {
        this.opCost = opCost;
        this.totalCost = totalCost;
        this.leftExchangeCost = leftExchangeCost;
        this.rightExchangeCost = rightExchangeCost;
    }
}
