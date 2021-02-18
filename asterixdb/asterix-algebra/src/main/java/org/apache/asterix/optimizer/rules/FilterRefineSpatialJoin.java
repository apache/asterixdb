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

import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FilterRefineSpatialJoin implements IAlgebraicRewriteRule {

    private static final int LEFT = 0;
    private static final int RIGHT = 1;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        ILogicalExpression joinCondition = joinConditionRef.getValue();

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression STFuncExpr = (AbstractFunctionCallExpression) joinCondition;
        if (!BuiltinFunctions.isSTFilterRefineFunction(STFuncExpr.getFunctionIdentifier())) {
            return false;
        }

        List<Mutable<ILogicalExpression>> inputExprs = STFuncExpr.getArguments();
        if (inputExprs.size() != 2) {
            return false;
        }

        ILogicalExpression leftOperatingExpr = inputExprs.get(LEFT).getValue();
        ILogicalExpression rightOperatingExpr = inputExprs.get(RIGHT).getValue();

        if (leftOperatingExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || rightOperatingExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        LogicalVariable inputVar0 = ((VariableReferenceExpression) leftOperatingExpr).getVariableReference();
        LogicalVariable inputVar1 = ((VariableReferenceExpression) rightOperatingExpr).getVariableReference();

        ScalarFunctionCallExpression left =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR),
                        new MutableObject<>(new VariableReferenceExpression(inputVar0)));

        ScalarFunctionCallExpression right =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.ST_MBR),
                        new MutableObject<>(new VariableReferenceExpression(inputVar1)));

        ScalarFunctionCallExpression spatialIntersect = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SPATIAL_INTERSECT), new MutableObject<>(left),
                new MutableObject<>(right));

        ScalarFunctionCallExpression updatedJoinCondition =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND),
                        new MutableObject<>(spatialIntersect), new MutableObject<>(STFuncExpr));

        joinConditionRef.setValue(updatedJoinCondition);

        return true;
    }

}
