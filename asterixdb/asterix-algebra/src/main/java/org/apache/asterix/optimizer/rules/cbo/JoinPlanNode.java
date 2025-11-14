
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

import org.apache.asterix.common.annotations.IndexedNLJoinExpressionAnnotation;
import org.apache.asterix.optimizer.cost.ICost;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.HashJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class JoinPlanNode extends AbstractPlanNode {
    protected String datasetName;
    protected ILogicalOperator leafInput;
    protected boolean outerJoin;
    protected int[] jnIndexes;
    protected int[] planIndexes;
    protected JoinMethod joinOp;

    public enum JoinMethod {
        HYBRID_HASH_JOIN,
        BROADCAST_HASH_JOIN,
        INDEX_NESTED_LOOP_JOIN,
        CARTESIAN_PRODUCT_JOIN
    }

    private ILogicalExpression joinExpr;
    Pair<AbstractFunctionCallExpression, IndexedNLJoinExpressionAnnotation> exprAndHint;
    protected HashJoinExpressionAnnotation.BuildSide side;
    protected IExpressionAnnotation joinHint;
    protected ICost opCost;
    protected ICost totalCost;
    protected ICost leftExchangeCost;
    protected ICost rightExchangeCost;
    private AbstractPlanNode leftPlan;
    private AbstractPlanNode rightPlan;

    public JoinPlanNode(JoinNode joinNode, AbstractPlanNode left, AbstractPlanNode right, boolean outerJoin) {
        super((joinNode));
        this.outerJoin = outerJoin;
        planIndexes = new int[2]; // 0 is for left, 1 is for right
        jnIndexes = new int[2]; // join node index(es)
        this.leftPlan = left;
        this.rightPlan = right;
        setLeftJoinIndex(leftPlan.getJoinNode().jnArrayIndex);
        setRightJoinIndex(rightPlan.getJoinNode().jnArrayIndex);
    }

    void setJoinAndHintInfo(JoinPlanNode.JoinMethod joinMethod, ILogicalExpression joinExpr,
            Pair<AbstractFunctionCallExpression, IndexedNLJoinExpressionAnnotation> exprAndHint,
            HashJoinExpressionAnnotation.BuildSide side, IExpressionAnnotation hint) {
        joinOp = joinMethod;
        this.joinExpr = joinExpr;
        this.exprAndHint = exprAndHint;
        this.side = side;
        joinHint = hint;
        numHintsUsed = leftPlan.getNumHintsUsed() + rightPlan.getNumHintsUsed();
        if (hint != null) {
            numHintsUsed++;
        }
    }

    @Override
    public ICost getOpCost() {
        return opCost;
    }

    @Override
    public ICost getTotalCost() {
        return totalCost;
    }

    public Pair<String, String> joinMethod() {
        if (this.joinOp == JoinPlanNode.JoinMethod.HYBRID_HASH_JOIN) {
            return new Pair<>("HASH JOIN", "HJ");
        } else if (this.joinOp == JoinPlanNode.JoinMethod.BROADCAST_HASH_JOIN) {
            return new Pair<>("BROADCAST HASH JOIN", "BHJ");
        } else if (this.joinOp == JoinPlanNode.JoinMethod.INDEX_NESTED_LOOP_JOIN) {
            return new Pair<>("INDEX NESTED LOOPS JOIN", "INLJ");
        } else if (this.joinOp == JoinPlanNode.JoinMethod.CARTESIAN_PRODUCT_JOIN) {
            return new Pair<>("CARTESIAN PRODUCT JOIN", "CPJ");
        }
        return new Pair<>("", "");
    }

    public void setJoinCosts(ICost opCost, ICost totalCost, ICost leftExchangeCost, ICost rightExchangeCost) {
        this.opCost = opCost;
        this.totalCost = totalCost;
        this.leftExchangeCost = leftExchangeCost;
        this.rightExchangeCost = rightExchangeCost;
    }

    public AbstractPlanNode getLeftPlan() {
        return leftPlan;
    }

    public AbstractPlanNode getRightPlan() {
        return rightPlan;
    }

    public ILogicalExpression getJoinExpr() {
        return joinExpr;
    }

    public double computeTotalCost() {
        return totalCost.computeTotalCost();
    }

    public JoinPlanNode.JoinMethod getJoinOp() {
        return joinOp;
    }

    public ICost getLeftExchangeCost() {
        return leftExchangeCost;
    }

    public ICost getRightExchangeCost() {
        return rightExchangeCost;
    }

    protected double computeOpCost() {
        return opCost.computeTotalCost();
    }

    public int getRightJoinIndex() {
        return jnIndexes[1];
    }

    protected void setLeftJoinIndex(int index) {
        this.jnIndexes[0] = index;
    }

    protected void setRightJoinIndex(int index) {
        this.jnIndexes[1] = index;
    }
}
