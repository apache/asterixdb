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

import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class CountVarToCountOneRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator groupBy = (GroupByOperator) op1;
            for (ILogicalPlan p : groupBy.getNestedPlans()) {
                for (Mutable<ILogicalOperator> aggRef : p.getRoots()) {
                    // make sure we do not trigger this rule for aggregate operators with group by
                    if (aggRef.getValue().getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                        context.addToDontApplySet(this, aggRef.getValue());
                    }
                }
            }
        }
        return false;
    }

    // It is for a group-by having just one count or a single count without group-by
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator g = (GroupByOperator) op1;
            if (g.getNestedPlans().size() != 1) {
                return false;
            }
            ILogicalPlan p = g.getNestedPlans().get(0);
            if (p.getRoots().size() != 1) {
                return false;
            }
            AbstractLogicalOperator op2 = (AbstractLogicalOperator) p.getRoots().get(0).getValue();
            if (op2.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                return false;
            }
            AggregateOperator agg = (AggregateOperator) op2;
            if (agg.getInputs().get(0).getValue().getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
                return false;
            }
            return rewriteCountVar(agg, context);
        } else if (op1.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
            AggregateOperator agg = (AggregateOperator) op1;
            return rewriteCountVar(agg, context);
        } else {
            return false;
        }
    }

    private boolean rewriteCountVar(AggregateOperator agg, IOptimizationContext context) throws AlgebricksException {
        if (agg.getExpressions().size() != 1) {
            return false;
        }
        ILogicalExpression exp = agg.getExpressions().get(0).getValue();
        if (exp.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) exp;
        if (fun.getArguments().size() != 1) {
            return false;
        }
        ILogicalExpression arg = fun.getArguments().get(0).getValue();
        if (arg.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        if (fun.getFunctionIdentifier() == BuiltinFunctions.COUNT) {
            // for strict count, we can always replace count(var) with count(1)
            fun.getArguments().get(0).setValue(new ConstantExpression(new AsterixConstantValue(new AInt64(1L))));
            return true;
        } else if (fun.getFunctionIdentifier() == BuiltinFunctions.SQL_COUNT) {
            // for SQL count, we can replace count(var) with count(1) only when var is not nullable
            IVariableTypeEnvironment env = context.getOutputTypeEnvironment(agg.getInputs().get(0).getValue());
            LogicalVariable countVar = ((VariableReferenceExpression) arg).getVariableReference();
            Object varType = env.getVarType(countVar);
            boolean nullable = TypeHelper.canBeUnknown((IAType) varType);
            if (!nullable) {
                fun.getArguments().get(0).setValue(new ConstantExpression(new AsterixConstantValue(new AInt64(1L))));
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }

    }

}
