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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;

public class AdvisorJoinPlanNode extends AbstractAdvisorPlanNode {
    private final AbstractAdvisorPlanNode left;
    private final AbstractAdvisorPlanNode right;
    private final JoinFilter joinCondition;

    public AdvisorJoinPlanNode(AbstractAdvisorPlanNode left, AbstractAdvisorPlanNode right,
            AbstractBinaryJoinOperator joinOp, IOptimizationContext context) throws AlgebricksException {
        super(PlanNodeType.JOIN);
        this.left = left;
        this.right = right;
        this.joinCondition = AdvisorConditionParser.parseJoinNode(joinOp, context);
    }

    @Override
    public List<AdvisorScanPlanNode> getLeafs() {
        List<AdvisorScanPlanNode> leafs = new ArrayList<>();
        leafs.addAll(left.getLeafs());
        leafs.addAll(right.getLeafs());
        return leafs;
    }

    @Override
    public List<AdvisorJoinPlanNode> getJoins() {
        List<AdvisorJoinPlanNode> joins = new ArrayList<>();
        joins.add(this);
        joins.addAll(left.getJoins());
        joins.addAll(right.getJoins());
        return joins;
    }

    public JoinFilter getJoinCondition() {
        return joinCondition;
    }
}
