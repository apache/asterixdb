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

import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.CommonFunctionMapUtil;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.rules.visitor.ConstantFoldingVisitor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentTemporalValueUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestNonMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;

/**
 * Reduces an {@code AT SNAPSHOT} / {@code AT TIMESTAMP} value to a constant, evaluating every function in it.
 * <p>
 * The value selects which data files the scan reads, so it is consumed while the plan is being built: there
 * are no rows and no job yet, and the only thing a function can mean in this position is "the value now". That
 * makes it safe to evaluate functions here that ordinary constant folding refuses -- the clock functions above
 * all -- without changing what they mean anywhere else, because nothing about the functions themselves changes.
 * Only this one expression is treated this way.
 * <p>
 * A value that does not reduce is reported here, naming what could not be resolved -- and, for the plain
 * clock functions, the {@code _immediate} variant that can be. Variable references are never evaluated; the
 * translator has already rejected any that could occur here.
 * <p>
 * Runs after {@code ConstantFoldingRule} and before {@code UnnestToDataScanRule}, where the value is read.
 */
public class ResolveTimeTravelValueRule implements IAlgebraicRewriteRule {

    private final ConstantFoldingVisitor cfv;

    public ResolveTimeTravelValueRule(ICcApplicationContext appCtx) {
        cfv = new TimeTravelValueFoldingVisitor(appCtx);
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (!(op instanceof AbstractUnnestNonMapOperator)) {
            return false;
        }
        TimeTravel timeTravel = ((AbstractUnnestNonMapOperator) op).getTimeTravel();
        if (timeTravel == null || timeTravel.getValueExpression().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }
        cfv.reset(context);
        Mutable<ILogicalExpression> valueRef = timeTravel.getValueExpressionRef();
        if (!valueRef.getValue().isFunctional()) {
            // the folded value is only right for this compilation; a cached plan would keep reading it on
            // every later run of the statement, e.g. a clock read hours ago
            context.markPlanNotReusable();
        }
        replaceWallClockCalls(valueRef, System.currentTimeMillis());
        // a function that needs state only a running job has fails with a checked exception, which constant
        // folding absorbs by leaving the call unfolded; it is reported below as unresolved
        cfv.transform(valueRef);
        ILogicalExpression value = valueRef.getValue();
        if (value.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return true;
        }
        SourceLocation sourceLoc =
                value.getSourceLocation() != null ? value.getSourceLocation() : op.getSourceLocation();
        FunctionIdentifier jobClock = findJobClockCall(value);
        if (jobClock != null) {
            throw new CompilationException(ErrorCode.TIME_TRAVEL_VALUE_UNSUPPORTED_FUNCTION, sourceLoc,
                    sqlppNameWithAliases(jobClock), sqlppName(BuiltinFunctions.getImmediateVariant(jobClock)));
        }
        throw new CompilationException(ErrorCode.TIME_TRAVEL_VALUE_NOT_RESOLVED, sourceLoc,
                LogRedactionUtil.userData(value.toString()));
    }

    /**
     * The wall clock functions take their zone from the job, which does not exist yet, so their evaluators cannot
     * run here. The value is read as UTC, so the clock is read in UTC: in the controller's own zone it would name
     * an instant off by that zone's offset.
     */
    private static void replaceWallClockCalls(Mutable<ILogicalExpression> exprRef, long nowMillis) {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        AbstractFunctionCallExpression call = (AbstractFunctionCallExpression) expr;
        IAObject now = wallClockInUtc(call.getFunctionIdentifier(), nowMillis);
        if (now != null) {
            ConstantExpression constant = new ConstantExpression(new AsterixConstantValue(now));
            constant.setSourceLocation(call.getSourceLocation());
            exprRef.setValue(constant);
            return;
        }
        for (Mutable<ILogicalExpression> arg : call.getArguments()) {
            replaceWallClockCalls(arg, nowMillis);
        }
    }

    /**
     * @return the value {@code fid} would produce at {@code nowMillis} in UTC, or {@code null} if {@code fid} is
     *         not a wall clock function
     */
    public static IAObject wallClockInUtc(FunctionIdentifier fid, long nowMillis) {
        ZoneRules utc = ZoneOffset.UTC.getRules();
        if (fid.equals(BuiltinFunctions.CURRENT_DATETIME_IMMEDIATE)) {
            return new ADateTime(CurrentTemporalValueUtil.valueAt(ATypeTag.DATETIME, nowMillis, utc));
        } else if (fid.equals(BuiltinFunctions.CURRENT_DATE_IMMEDIATE)) {
            return new ADate((int) CurrentTemporalValueUtil.valueAt(ATypeTag.DATE, nowMillis, utc));
        } else if (fid.equals(BuiltinFunctions.CURRENT_TIME_IMMEDIATE)) {
            return new ATime((int) CurrentTemporalValueUtil.valueAt(ATypeTag.TIME, nowMillis, utc));
        }
        return null;
    }

    /**
     * A job clock function reads the job start time. A job does not exist while a time travel value is resolved,
     * so it can never be reduced here; the immediate variant registered for it reads the wall clock and can.
     */
    private static FunctionIdentifier findJobClockCall(ILogicalExpression expr) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression call = (AbstractFunctionCallExpression) expr;
        if (BuiltinFunctions.getImmediateVariant(call.getFunctionIdentifier()) != null) {
            return call.getFunctionIdentifier();
        }
        for (Mutable<ILogicalExpression> arg : call.getArguments()) {
            FunctionIdentifier found = findJobClockCall(arg.getValue());
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static String sqlppName(FunctionIdentifier fid) {
        return fid.getName().replace('-', '_');
    }

    /**
     * The call may have been written under an alias, which is resolved away before this rule runs, so every
     * spelling that leads to the function is named.
     */
    private static String sqlppNameWithAliases(FunctionIdentifier fid) {
        String name = sqlppName(fid);
        List<String> aliases =
                CommonFunctionMapUtil.getAliases(fid.getName()).stream().filter(alias -> !alias.equals(name)).toList();
        return aliases.isEmpty() ? name : name + " (or " + String.join(", ", aliases) + ")";
    }

    /**
     * Constant folding that evaluates every call whose arguments are constants, functional or not.
     */
    private static final class TimeTravelValueFoldingVisitor extends ConstantFoldingVisitor {

        TimeTravelValueFoldingVisitor(ICcApplicationContext appCtx) {
            super(appCtx);
        }

        @Override
        protected boolean isFoldable(ScalarFunctionCallExpression expr) {
            return true;
        }
    }
}
