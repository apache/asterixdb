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

import static org.apache.asterix.optimizer.rules.cbo.EnumerateJoinsRule.containsLeafInputOnly;
import static org.apache.asterix.optimizer.rules.cbo.EnumerateJoinsRule.joinClause;
import static org.apache.asterix.optimizer.rules.cbo.EnumerateJoinsRule.numVarRefExprs;
import static org.apache.asterix.optimizer.rules.cbo.EnumerateJoinsRule.skipPastAssigns;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;

public class AdvisorPlanParser {
    public static CBOPlanStateTree getCBOPlanState(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        Map<Mutable<ILogicalOperator>, Boolean> visited = new HashMap<>();
        return visit(opRef, visited, context);
    }

    private static CBOPlanStateTree visit(Mutable<ILogicalOperator> opRef,
            Map<Mutable<ILogicalOperator>, Boolean> visited, IOptimizationContext context) throws AlgebricksException {
        CBOPlanStateTree planStateTree = runGetOps(opRef, visited, context);
        if (planStateTree != null) {
            return planStateTree;
        }
        for (Mutable<ILogicalOperator> childOpRef : opRef.getValue().getInputs()) {
            planStateTree = visit(childOpRef, visited, context);

            if (planStateTree != null) {
                return planStateTree;
            }
        }
        return null;
    }

    private static CBOPlanStateTree runGetOps(Mutable<ILogicalOperator> opRef,
            Map<Mutable<ILogicalOperator>, Boolean> visited, IOptimizationContext context) throws AlgebricksException {
        if (visited.containsKey(opRef)) {
            return null;
        }
        if (!joinClause(opRef.getValue())
                && opRef.getValue().getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return null;
        }
        AbstractAdvisorPlanNode planNode = getJoinOpsAndLeafInputs(opRef.getValue(), context);
        if (planNode == null) {
            return null;
        }
        return new CBOPlanStateTree(planNode);
    }

    private static AbstractAdvisorPlanNode getJoinOpsAndLeafInputs(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        if (isAssignLeadingToJoin(op)) {
            AbstractBinaryJoinOperator skipAssignOp = (AbstractBinaryJoinOperator) skipPastAssigns(op);
            AbstractAdvisorPlanNode leftPlanNode =
                    getJoinOpsAndLeafInputs(skipAssignOp.getInputs().get(0).getValue(), context);
            AbstractAdvisorPlanNode rightPlanNode =
                    getJoinOpsAndLeafInputs(skipAssignOp.getInputs().get(1).getValue(), context);
            if (leftPlanNode == null || rightPlanNode == null) {
                return null;
            }
            return new AdvisorJoinPlanNode(leftPlanNode, rightPlanNode, skipAssignOp, context);
        } else {
            if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                return null;
            }
            Pair<EmptyTupleSourceOperator, DataSourceScanOperator> etsDataSource = containsLeafInputOnly(op);
            if (etsDataSource == null || etsDataSource.second == null) {
                return null;
            }
            return new AdvisorScanPlanNode(op, context);
        }
    }

    private static boolean isAssignLeadingToJoin(ILogicalOperator op) {
        while (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator aOp = (AssignOperator) op;
            int count = numVarRefExprs(aOp);
            if (count > 1) {
                return false;
            }
            op = op.getInputs().get(0).getValue();
        }
        return (joinClause(op));
    }
}
