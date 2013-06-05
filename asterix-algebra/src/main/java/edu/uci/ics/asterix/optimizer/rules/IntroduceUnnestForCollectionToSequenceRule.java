/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule introduces a unnest operator for the collection-to-sequence function (if the input to the function is a collection).
 * 
 * @author yingyib
 */
public class IntroduceUnnestForCollectionToSequenceRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;
        List<Mutable<ILogicalExpression>> exprs = assign.getExpressions();
        if (exprs.size() != 1) {
            return false;
        }
        ILogicalExpression expr = exprs.get(0).getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) expr;
        if (func.getFunctionIdentifier() != AsterixBuiltinFunctions.COLLECTION_TO_SEQUENCE) {
            return false;
        }

        IVariableTypeEnvironment env = assign.computeInputTypeEnvironment(context);
        ILogicalExpression argExpr = func.getArguments().get(0).getValue();
        IAType outerExprType = (IAType) env.getType(expr);
        IAType innerExprType = (IAType) env.getType(argExpr);
        if (outerExprType.equals(innerExprType)) {
            /** nothing is changed with the collection-to-sequence function, remove the collection-sequence function call */
            assign.getExpressions().set(0, new MutableObject<ILogicalExpression>(argExpr));
            return true;
        }
        /** change the assign operator to an unnest operator */
        LogicalVariable var = assign.getVariables().get(0);
        @SuppressWarnings("unchecked")
        UnnestOperator unnest = new UnnestOperator(var, new MutableObject<ILogicalExpression>(
                new UnnestingFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION),
                        new MutableObject<ILogicalExpression>(argExpr))));
        unnest.getInputs().addAll(assign.getInputs());
        opRef.setValue(unnest);
        context.computeAndSetTypeEnvironmentForOperator(unnest);
        return true;
    }
}
