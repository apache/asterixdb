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

import org.apache.asterix.optimizer.cost.ICost;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;

public class PlanNode {

    protected static int NO_PLAN = -1;

    private final JoinEnum joinEnum;
    protected int allPlansIndex;
    protected int[] planIndexes;
    protected int[] jnIndexes;
    protected JoinNode jn;
    protected String datasetName;
    protected ICost opCost;
    protected ICost totalCost;
    protected ICost leftExchangeCost;
    protected ICost rightExchangeCost;
    protected JoinMethod joinOp;
    protected IExpressionAnnotation joinHint;
    // Used to indicate which side to build for HJ and which side to broadcast for BHJ.
    protected HashJoinExpressionAnnotation.BuildSide side;
    protected ScanMethod scanOp;
    protected ILogicalExpression joinExpr;
    private DataSourceScanOperator correspondingDataSourceScanOp;
    protected EmptyTupleSourceOperator correspondingEmptyTupleSourceOp;

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

    public PlanNode(int planIndex, JoinEnum joinE) {
        this.allPlansIndex = planIndex;
        joinEnum = joinE;
        planIndexes = new int[2]; // 0 is for left, 1 is for right
        jnIndexes = new int[2]; // join node index(es)
    }

    public int getIndex() {
        return allPlansIndex;
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

    protected boolean IsJoinNode() {
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

    private DataSourceScanOperator getDataSourceScanOp() {
        return correspondingDataSourceScanOp; // This applies only to singleDataSetPlans
    }

    protected EmptyTupleSourceOperator getEmptyTupleSourceOp() {
        return correspondingEmptyTupleSourceOp; // This applies only to singleDataSetPlans
    }

    protected void setEmptyTupleSourceOp(EmptyTupleSourceOperator emptyTupleSourceOp) {
        this.correspondingEmptyTupleSourceOp = emptyTupleSourceOp; // This applies only to singleDataSetPlans
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
}
