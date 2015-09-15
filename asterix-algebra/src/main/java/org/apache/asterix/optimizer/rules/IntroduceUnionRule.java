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

package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * @author kereno, ecarm002, ildar.absalyamov
 *         Generates a union operator and puts it instead of "assign <- [function-call: asterix:union]"
 *         Before rule:
 *         ============
 *         assign [var] <- [asterix:union(left_branch, right_branch)]
 *         join (TRUE)
 *         left_branch
 *         right_branch
 *         After rule:
 *         ============
 *         union (left_branch, right_branch, result_var)
 *         left_branch
 *         right_branch
 */
public class IntroduceUnionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        if (!opRef.getValue().getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {
            return false;
        }

        AssignOperator assignUnion = (AssignOperator) opRef.getValue();

        if (assignUnion.getExpressions().get(0).getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL)
            return false;

        AbstractFunctionCallExpression u = (AbstractFunctionCallExpression) assignUnion.getExpressions().get(0)
                .getValue();
        if (!AsterixBuiltinFunctions.UNION.equals(u.getFunctionIdentifier())) {
            return false;
        }

        //Retrieving the logical variables for the union from the two aggregates which are inputs to the join
        Mutable<ILogicalOperator> join = assignUnion.getInputs().get(0);

        LogicalOperatorTag tag1 = join.getValue().getOperatorTag();
        if (tag1 != LogicalOperatorTag.INNERJOIN && tag1 != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join1 = (AbstractBinaryJoinOperator) join.getValue();
        ILogicalExpression cond1 = join1.getCondition().getValue();
        // don't try to push a product down
        if (!OperatorPropertiesUtil.isAlwaysTrueCond(cond1)) {
            return false;
        }

        List<Mutable<ILogicalOperator>> joinInputs = join.getValue().getInputs();

        Mutable<ILogicalOperator> left_branch = joinInputs.get(0);
        Mutable<ILogicalOperator> right_branch = joinInputs.get(1);

        List<LogicalVariable> input1Var = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(left_branch.getValue(), input1Var);

        List<LogicalVariable> input2Var = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(right_branch.getValue(), input2Var);

        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>(
                1);
        Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple = new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(
                input1Var.get(0), input2Var.get(0), assignUnion.getVariables().get(0));
        varMap.add(triple);
        UnionAllOperator unionOp = new UnionAllOperator(varMap);

        unionOp.getInputs().add(left_branch);
        unionOp.getInputs().add(right_branch);

        context.computeAndSetTypeEnvironmentForOperator(unionOp);

        opRef.setValue(unionOp);

        return true;

    }
}