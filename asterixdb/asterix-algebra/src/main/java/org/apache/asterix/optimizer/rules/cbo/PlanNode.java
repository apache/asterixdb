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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;

public class PlanNode {

    public static int NO_PLAN = -1;

    private final JoinEnum joinEnum;
    int allPlansIndex;
    int[] planIndexes;
    int[] jnIndexes;
    JoinNode jn;
    String datasetName;
    ICost opCost;
    ICost totalCost;
    ICost leftExchangeCost;
    ICost rightExchangeCost;
    JoinMethod joinOp;
    // Used to indicate which side to build for HJ and which side to broadcast for BHJ.
    HashJoinExpressionAnnotation.BuildSide side;
    ScanMethod scanOp;
    ILogicalExpression joinExpr;
    DataSourceScanOperator correspondingDataSourceScanOp;
    EmptyTupleSourceOperator correspondingEmptyTupleSourceOp;

    public enum ScanMethod {
        INDEX_SCAN,
        TABLE_SCAN
    }

    public enum JoinMethod {
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

    public int[] getPlanIndexes() {
        return planIndexes;
    }

    public int getLeftPlanIndex() {
        return planIndexes[0];
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

    public JoinNode getJoinNode() {
        return jn;
    }

    public int getRightPlanIndex() {
        return planIndexes[1];
    }

    public int getLeftJoinIndex() {
        return jnIndexes[0];
    }

    public int getRightJoinIndex() {
        return jnIndexes[1];
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

    public String getDatasetName() {
        return datasetName;
    }

    public DataSourceScanOperator getDataSourceScanOp() {
        return correspondingDataSourceScanOp; // This applies only to singleDataSetPlans
    }

    public EmptyTupleSourceOperator getEmptyTupleSourceOp() {
        return correspondingEmptyTupleSourceOp; // This applies only to singleDataSetPlans
    }

    public ICost getOpCost() {
        return opCost;
    }

    public double computeOpCost() {
        return opCost.computeTotalCost();
    }

    public ICost getTotalCost() {
        return totalCost;
    }

    public ICost getLeftExchangeCost() {
        return leftExchangeCost;
    }

    public ICost getRightExchangeCost() {
        return rightExchangeCost;
    }

    public double computeTotalCost() {
        return totalCost.computeTotalCost();
    }

    public ScanMethod getScanOp() {
        return scanOp;
    }

    public JoinMethod getJoinOp() {
        return joinOp;
    }

    public ILogicalExpression getJoinExpr() {
        return joinExpr;
    }
}
