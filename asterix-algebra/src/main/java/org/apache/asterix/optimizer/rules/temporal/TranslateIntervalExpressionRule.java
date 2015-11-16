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
package org.apache.asterix.optimizer.rules.temporal;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Finds interval conditional expressions and convert them to interval start and end conditional statements.
 * The translation exposes the condition to the Algebricks optimizer.
 */
public class TranslateIntervalExpressionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator selectOp = (SelectOperator) op;

        Mutable<ILogicalExpression> exprRef = selectOp.getCondition();
        boolean modified = false;
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (funcExpr.getArguments().size() != 2) {
            return false;
        }
        ILogicalExpression interval1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression interval2 = funcExpr.getArguments().get(1).getValue();
        if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_MEETS)) {
            exprRef.setValue(getEqualExpr(getIntervalEndExpr(interval1), getIntervalStartExpr(interval2)));
            modified = true;
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_MET_BY)) {
            exprRef.setValue(getEqualExpr(getIntervalStartExpr(interval1), getIntervalEndExpr(interval2)));
            modified = true;
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_STARTS)) {
            ILogicalExpression startExpr = getEqualExpr(getIntervalStartExpr(interval1),
                    getIntervalStartExpr(interval2));
            ILogicalExpression endExpr = getLessThanOrEqualExpr(getIntervalEndExpr(interval1),
                    getIntervalEndExpr(interval2));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
            modified = true;
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_STARTED_BY)) {
            ILogicalExpression startExpr = getEqualExpr(getIntervalStartExpr(interval1),
                    getIntervalStartExpr(interval2));
            ILogicalExpression endExpr = getLessThanOrEqualExpr(getIntervalEndExpr(interval2),
                    getIntervalEndExpr(interval1));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
            modified = true;
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_ENDS)) {
            ILogicalExpression endExpr = getEqualExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            ILogicalExpression startExpr = getLessThanOrEqualExpr(getIntervalStartExpr(interval1),
                    getIntervalStartExpr(interval2));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
            modified = true;
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_ENDED_BY)) {
            ILogicalExpression endExpr = getEqualExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            ILogicalExpression startExpr = getLessThanOrEqualExpr(getIntervalStartExpr(interval2),
                    getIntervalStartExpr(interval1));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
            modified = true;
        } else if (funcExpr.getFunctionInfo().equals(AsterixBuiltinFunctions.INTERVAL_BEFORE)) {
            // Requires new strategy, no translation for this interval and the remaining listed.
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_AFTER)) {
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_OVERLAPS)) {
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_OVERLAPPING)) {
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_COVERS)) {
        } else if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.INTERVAL_COVERED_BY)) {
        }

        return modified;
    }

    private ILogicalExpression getAndExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.AND, arg1, arg2);
    }

    private ILogicalExpression getEqualExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.EQ, arg1, arg2);
    }

    private ILogicalExpression getLessThanOrEqualExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.LE, arg1, arg2);
    }

    private ILogicalExpression getIntervalStartExpr(ILogicalExpression interval) {
        return getScalarExpr(AsterixBuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START, interval);
    }

    private ILogicalExpression getIntervalEndExpr(ILogicalExpression interval) {
        return getScalarExpr(AsterixBuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END, interval);
    }

    private ILogicalExpression getScalarExpr(FunctionIdentifier func, ILogicalExpression interval) {
        List<Mutable<ILogicalExpression>> intervalArg = new ArrayList<Mutable<ILogicalExpression>>();
        intervalArg.add(new MutableObject<ILogicalExpression>(interval));
        return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(func), intervalArg);
    }

    private ILogicalExpression getScalarExpr(FunctionIdentifier func, ILogicalExpression interval1,
            ILogicalExpression interval2) {
        List<Mutable<ILogicalExpression>> intervalArg = new ArrayList<Mutable<ILogicalExpression>>();
        intervalArg.add(new MutableObject<ILogicalExpression>(interval1));
        intervalArg.add(new MutableObject<ILogicalExpression>(interval2));
        return new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(func), intervalArg);
    }

}
