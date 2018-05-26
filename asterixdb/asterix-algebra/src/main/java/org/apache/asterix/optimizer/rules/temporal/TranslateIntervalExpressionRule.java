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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
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

    private static final Set<FunctionIdentifier> TRANSLATABLE_INTERVALS = new HashSet<>();
    {
        TRANSLATABLE_INTERVALS.add(BuiltinFunctions.INTERVAL_MEETS);
        TRANSLATABLE_INTERVALS.add(BuiltinFunctions.INTERVAL_MET_BY);
        TRANSLATABLE_INTERVALS.add(BuiltinFunctions.INTERVAL_STARTS);
        TRANSLATABLE_INTERVALS.add(BuiltinFunctions.INTERVAL_STARTED_BY);
        TRANSLATABLE_INTERVALS.add(BuiltinFunctions.INTERVAL_ENDS);
        TRANSLATABLE_INTERVALS.add(BuiltinFunctions.INTERVAL_ENDED_BY);
    }

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
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (funcExpr.getArguments().size() != 2) {
            return false;
        }
        if (!hasTranslatableInterval(funcExpr)) {
            return false;
        }

        return translateIntervalExpression(exprRef, funcExpr);
    }

    private boolean hasTranslatableInterval(AbstractFunctionCallExpression funcExpr) {
        if (TRANSLATABLE_INTERVALS.contains(funcExpr.getFunctionIdentifier())) {
            return true;
        }
        return false;
    }

    private boolean translateIntervalExpression(Mutable<ILogicalExpression> exprRef,
            AbstractFunctionCallExpression funcExpr) {
        // All interval relations are translated unless specified in a hint.
        // TODO A new strategy may be needed instead of just a simple translation.
        ILogicalExpression interval1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression interval2 = funcExpr.getArguments().get(1).getValue();
        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_MEETS)) {
            exprRef.setValue(getEqualExpr(getIntervalEndExpr(interval1), getIntervalStartExpr(interval2)));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_MET_BY)) {
            exprRef.setValue(getEqualExpr(getIntervalStartExpr(interval1), getIntervalEndExpr(interval2)));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_STARTS)) {
            ILogicalExpression startExpr =
                    getEqualExpr(getIntervalStartExpr(interval1), getIntervalStartExpr(interval2));
            ILogicalExpression endExpr =
                    getLessThanOrEqualExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_STARTED_BY)) {
            ILogicalExpression startExpr =
                    getEqualExpr(getIntervalStartExpr(interval1), getIntervalStartExpr(interval2));
            ILogicalExpression endExpr =
                    getLessThanOrEqualExpr(getIntervalEndExpr(interval2), getIntervalEndExpr(interval1));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_ENDS)) {
            ILogicalExpression endExpr = getEqualExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            ILogicalExpression startExpr =
                    getLessThanOrEqualExpr(getIntervalStartExpr(interval1), getIntervalStartExpr(interval2));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_ENDED_BY)) {
            ILogicalExpression endExpr = getEqualExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            ILogicalExpression startExpr =
                    getLessThanOrEqualExpr(getIntervalStartExpr(interval2), getIntervalStartExpr(interval1));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            exprRef.setValue(getLessThanExpr(getIntervalEndExpr(interval1), getIntervalStartExpr(interval2)));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_AFTER)) {
            exprRef.setValue(getGreaterThanExpr(getIntervalStartExpr(interval1), getIntervalEndExpr(interval2)));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            ILogicalExpression expr1 =
                    getLessThanExpr(getIntervalStartExpr(interval1), getIntervalStartExpr(interval2));
            ILogicalExpression expr2 = getGreaterThanExpr(getIntervalEndExpr(interval2), getIntervalEndExpr(interval1));
            ILogicalExpression expr3 =
                    getGreaterThanExpr(getIntervalEndExpr(interval1), getIntervalStartExpr(interval2));
            exprRef.setValue(getAndExpr(getAndExpr(expr1, expr2), expr3));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            ILogicalExpression expr1 =
                    getLessThanExpr(getIntervalStartExpr(interval2), getIntervalStartExpr(interval1));
            ILogicalExpression expr2 = getGreaterThanExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            ILogicalExpression expr3 =
                    getGreaterThanExpr(getIntervalEndExpr(interval2), getIntervalStartExpr(interval1));
            exprRef.setValue(getAndExpr(getAndExpr(expr1, expr2), expr3));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            ILogicalExpression startExpr =
                    getLessThanOrEqualExpr(getIntervalStartExpr(interval1), getIntervalEndExpr(interval2));
            ILogicalExpression endExpr =
                    getGreaterThanOrEqualExpr(getIntervalEndExpr(interval1), getIntervalStartExpr(interval2));
            ILogicalExpression startPointExpr =
                    getNotEqualExpr(getIntervalEndExpr(interval1), getIntervalStartExpr(interval2));
            ILogicalExpression endPointExpr =
                    getNotEqualExpr(getIntervalStartExpr(interval1), getIntervalEndExpr(interval2));
            exprRef.setValue(getAndExpr(getAndExpr(startExpr, endExpr), getAndExpr(startPointExpr, endPointExpr)));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_COVERS)) {
            ILogicalExpression startExpr =
                    getLessThanOrEqualExpr(getIntervalStartExpr(interval1), getIntervalStartExpr(interval2));
            ILogicalExpression endExpr =
                    getGreaterThanOrEqualExpr(getIntervalEndExpr(interval1), getIntervalEndExpr(interval2));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            ILogicalExpression startExpr =
                    getLessThanOrEqualExpr(getIntervalStartExpr(interval2), getIntervalStartExpr(interval1));
            ILogicalExpression endExpr =
                    getGreaterThanOrEqualExpr(getIntervalEndExpr(interval2), getIntervalEndExpr(interval1));
            exprRef.setValue(getAndExpr(startExpr, endExpr));
        } else {
            return false;
        }
        return true;
    }

    private ILogicalExpression getAndExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.AND, arg1, arg2);
    }

    private ILogicalExpression getEqualExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.EQ, arg1, arg2);
    }

    private ILogicalExpression getNotEqualExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.NEQ, arg1, arg2);
    }

    private ILogicalExpression getLessThanExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.LT, arg1, arg2);
    }

    private ILogicalExpression getLessThanOrEqualExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.LE, arg1, arg2);
    }

    private ILogicalExpression getGreaterThanExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.GT, arg1, arg2);
    }

    private ILogicalExpression getGreaterThanOrEqualExpr(ILogicalExpression arg1, ILogicalExpression arg2) {
        return getScalarExpr(AlgebricksBuiltinFunctions.GE, arg1, arg2);
    }

    private ILogicalExpression getIntervalStartExpr(ILogicalExpression interval) {
        return getScalarExpr(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START, interval);
    }

    private ILogicalExpression getIntervalEndExpr(ILogicalExpression interval) {
        return getScalarExpr(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END, interval);
    }

    private ILogicalExpression getScalarExpr(FunctionIdentifier func, ILogicalExpression interval) {
        List<Mutable<ILogicalExpression>> intervalArg = new ArrayList<>();
        intervalArg.add(new MutableObject<ILogicalExpression>(interval));
        ScalarFunctionCallExpression fnExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(func), intervalArg);
        fnExpr.setSourceLocation(interval.getSourceLocation());
        return fnExpr;
    }

    private ILogicalExpression getScalarExpr(FunctionIdentifier func, ILogicalExpression interval1,
            ILogicalExpression interval2) {
        List<Mutable<ILogicalExpression>> intervalArg = new ArrayList<>();
        intervalArg.add(new MutableObject<ILogicalExpression>(interval1));
        intervalArg.add(new MutableObject<ILogicalExpression>(interval2));
        ScalarFunctionCallExpression fnExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(func), intervalArg);
        fnExpr.setSourceLocation(interval1.getSourceLocation());
        return fnExpr;
    }

}
