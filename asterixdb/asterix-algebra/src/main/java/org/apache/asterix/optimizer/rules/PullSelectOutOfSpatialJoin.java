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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * If the join predicate contains multiple conditions, in which one of them is spatial_intersect($x, $y), all other
 * condition will be pull out to a SELECT operator after the SPATIAL_JOIN operator.
 *
 * For example:<br/>
 * join (and(spatial-intersect($$52, $$53), lt(st-distance($$56, $$57), 1.0))
 * -- SPATIAL_JOIN [$$62, $$52] [$$63, $$53]  |PARTITIONED|
 *
 * Becomes,
 *
 * select (lt(st-distance($$56, $$57), 1.0))
 * -- STREAM_SELECT  |PARTITIONED|
 *     exchange
 *     -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
 *         join (spatial-intersect($$52, $$53))
 *         -- SPATIAL_JOIN [$$62, $$52] [$$63, $$53]  |PARTITIONED|
 */

public class PullSelectOutOfSpatialJoin implements IAlgebraicRewriteRule {
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

        if (op.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.SPATIAL_JOIN) {
            return false;
        }

        ILogicalExpression expr = join.getCondition().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (!fi.equals(AlgebricksBuiltinFunctions.AND)) {
            return false;
        }
        List<Mutable<ILogicalExpression>> spatialVarVarComps = new ArrayList<Mutable<ILogicalExpression>>();
        List<Mutable<ILogicalExpression>> otherPredicates = new ArrayList<Mutable<ILogicalExpression>>();
        for (Mutable<ILogicalExpression> arg : fexp.getArguments()) {
            if (isSpatialVarVar(arg.getValue(), join, context)) {
                spatialVarVarComps.add(arg);
            } else {
                otherPredicates.add(arg);
            }
        }
        if (spatialVarVarComps.isEmpty() || otherPredicates.isEmpty()) {
            return false;
        }
        // pull up
        ILogicalExpression pulledCond = makeCondition(otherPredicates, context, op);
        SelectOperator select = new SelectOperator(new MutableObject<ILogicalExpression>(pulledCond));
        select.setSourceLocation(op.getSourceLocation());
        ILogicalExpression newJoinCond = makeCondition(spatialVarVarComps, context, op);
        join.getCondition().setValue(newJoinCond);
        select.getInputs().add(new MutableObject<ILogicalOperator>(join));
        context.computeAndSetTypeEnvironmentForOperator(select);
        select.recomputeSchema();
        opRef.setValue(select);

        return true;
    }

    private ILogicalExpression makeCondition(List<Mutable<ILogicalExpression>> predList, IOptimizationContext context,
            AbstractLogicalOperator op) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
            ScalarFunctionCallExpression conditionExpr = new ScalarFunctionCallExpression(finfo, predList);
            conditionExpr.setSourceLocation(op.getSourceLocation());
            return conditionExpr;
        } else {
            return predList.get(0).getValue();
        }
    }

    private boolean isSpatialVarVar(ILogicalExpression expr, AbstractBinaryJoinOperator join,
            IOptimizationContext context) throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
        if (!f.getFunctionIdentifier().equals(BuiltinFunctions.SPATIAL_INTERSECT)) {
            return false;
        }

        // We only apply this rule if the arguments of spatial_intersect are ARectangle
        IVariableTypeEnvironment typeEnvironment = join.computeInputTypeEnvironment(context);
        IAType leftType = (IAType) context.getExpressionTypeComputer().getType(f.getArguments().get(0).getValue(),
                context.getMetadataProvider(), typeEnvironment);
        IAType rightType = (IAType) context.getExpressionTypeComputer().getType(f.getArguments().get(1).getValue(),
                context.getMetadataProvider(), typeEnvironment);
        if ((leftType.getTypeTag() != BuiltinType.ARECTANGLE.getTypeTag())
                || (rightType.getTypeTag() != BuiltinType.ARECTANGLE.getTypeTag())) {
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
