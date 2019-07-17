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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PullSelectOutOfEqJoin implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op;

        ILogicalExpression expr = join.getCondition().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (!fi.equals(AlgebricksBuiltinFunctions.AND)) {
            return false;
        }
        List<Mutable<ILogicalExpression>> eqVarVarComps = new ArrayList<Mutable<ILogicalExpression>>();
        List<Mutable<ILogicalExpression>> otherPredicates = new ArrayList<Mutable<ILogicalExpression>>();
        for (Mutable<ILogicalExpression> arg : fexp.getArguments()) {
            if (isEqVarVar(arg.getValue())) {
                eqVarVarComps.add(arg);
            } else {
                otherPredicates.add(arg);
            }
        }
        if (eqVarVarComps.isEmpty() || otherPredicates.isEmpty()) {
            return false;
        }
        // pull up
        ILogicalExpression pulledCond = makeCondition(otherPredicates, context);
        SelectOperator select = new SelectOperator(new MutableObject<ILogicalExpression>(pulledCond), false, null);
        ILogicalExpression newJoinCond = makeCondition(eqVarVarComps, context);
        join.getCondition().setValue(newJoinCond);
        select.getInputs().add(new MutableObject<ILogicalOperator>(join));
        select.recomputeSchema();
        opRef.setValue(select);
        context.computeAndSetTypeEnvironmentForOperator(select);
        return true;
    }

    private ILogicalExpression makeCondition(List<Mutable<ILogicalExpression>> predList, IOptimizationContext context) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        } else {
            return predList.get(0).getValue();
        }
    }

    private boolean isEqVarVar(ILogicalExpression expr) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
        if (!f.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.EQ)) {
            return false;
        }
        ILogicalExpression e1 = f.getArguments().get(0).getValue();
        if (e1.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        } else {
            ILogicalExpression e2 = f.getArguments().get(1).getValue();
            return e2.getExpressionTag() == LogicalExpressionTag.VARIABLE;
        }
    }

}
