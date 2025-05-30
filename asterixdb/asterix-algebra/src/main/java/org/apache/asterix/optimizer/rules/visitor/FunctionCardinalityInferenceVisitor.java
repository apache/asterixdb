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
package org.apache.asterix.optimizer.rules.visitor;

import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.CardinalityInferenceVisitor;

public class FunctionCardinalityInferenceVisitor extends CardinalityInferenceVisitor {

    private FunctionCardinalityInferenceVisitor() {
        super();
    }

    public static boolean isCardinalityZeroOrOne(ILogicalOperator operator) throws AlgebricksException {
        FunctionCardinalityInferenceVisitor visitor = new FunctionCardinalityInferenceVisitor();
        long cardinality = operator.accept(visitor, null);
        return cardinality == ZERO_OR_ONE || cardinality == ONE;
    }

    @Override
    public Long visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        // While translation a limit expression is encapsulated as a switch-case expression.
        // switch-case(treat-as-integer(user_value_expr) > 0, true, treat-as-integer(user_value_expr), 0)
        // Here we extract the user_value_expr from the switch-case expression
        ScalarFunctionCallExpression inputExpr = (ScalarFunctionCallExpression) op.getMaxObjects().getValue();
        ILogicalExpression expr = ((ScalarFunctionCallExpression) inputExpr.getArguments().get(2).getValue())
                .getArguments().get(0).getValue();
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            AsterixConstantValue constantValue = (AsterixConstantValue) ((ConstantExpression) expr).getValue();
            if (constantValue.getObject() instanceof AInt64
                    && ((AInt64) constantValue.getObject()).getLongValue() <= 1) {
                return ZERO_OR_ONE;
            }
        }
        return adjustCardinalityForTupleReductionOperator(op.getInputs().get(0).getValue().accept(this, arg));
    }
}
