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
package org.apache.asterix.metadata.utils.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.IcebergTableFilterEvaluatorFactory;
import org.apache.asterix.external.util.MillisecondChronon;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IcebergTableFilterBuilder extends AbstractFilterBuilder {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * The width of the window below, taken from the read path rather than redeclared, because the two must agree:
     * this builder widens a predicate to exactly the chronon {@link MillisecondChronon#narrow} produces.
     */
    private static final long MICROS_PER_MILLI = MillisecondChronon.MICROS_PER_MILLI;

    /**
     * Reference name -> the path segments it was built from, before they were joined with dots. The join is lossy: a
     * field literally named {@code "a.b"} and a nested {@code a -> b} both render as {@code "a.b"}, and a consumer that
     * splits the name back apart cannot tell them apart. A name that two different paths produced maps to an EMPTY
     * list, meaning "ambiguous within this query".
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Preserves the unjoined filter path segments so a VARIANT sub-field named with a dot cannot be mistaken for nesting during predicate pushdown")
    private final Map<String, List<String>> pathSegments = new HashMap<>();

    public IcebergTableFilterBuilder(ExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getFilterExpression(), context,
                typeEnv);
    }

    public IExternalFilterEvaluatorFactory build() throws AlgebricksException {
        Expression icebergTablePredicate = null;
        if (filterExpression != null) {
            try {
                icebergTablePredicate = createIcebergExpression(filterExpression, false);
            } catch (Exception e) {
                LOGGER.warn("Error creating IcebergTable filter expression, skipping filter pushdown", e);
            }
        }
        return new IcebergTableFilterEvaluatorFactory(icebergTablePredicate, pathSegments);
    }

    /**
     * Recursively converts an AsterixDB logical expression into an Iceberg {@link Expression}.
     *
     * @return an Iceberg Expression, or {@code null} if the expression cannot be converted
     */
    protected Expression createIcebergExpression(ILogicalExpression expression, boolean insideNot)
            throws AlgebricksException {
        if (filterPaths.containsKey(expression)) {
            return null;
        } else if (expression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return createBooleanConstantExpression(expression);
        } else if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return handleFunction(expression, insideNot);
        }
        LOGGER.debug("Unsupported expression type {}, skipping filter pushdown", expression);
        return null;
    }

    /**
     * Extracts a Java value from a constant expression for use in comparisons.
     *
     * @return a Java value (String, Number, etc.), or {@code null} for null/missing
     */
    private Object createLiteralValue(ILogicalExpression expression) {
        ConstantExpression constExpr = (ConstantExpression) expression;
        if (constExpr.getValue().isNull() || constExpr.getValue().isMissing()) {
            return null;
        }
        if (!(constExpr.getValue() instanceof AsterixConstantValue)) {
            return null;
        }
        AsterixConstantValue constantValue = (AsterixConstantValue) constExpr.getValue();
        IAObject obj = constantValue.getObject();
        switch (obj.getType().getTypeTag()) {
            case STRING:
                return ((AString) obj).getStringValue();
            case TINYINT:
                return (int) ((AInt8) obj).getByteValue();
            case SMALLINT:
                return (int) ((AInt16) obj).getShortValue();
            case INTEGER:
                return ((AInt32) obj).getIntegerValue();
            case BIGINT:
                return ((AInt64) obj).getLongValue();
            case FLOAT:
                return ((AFloat) obj).getFloatValue();
            case DOUBLE:
                return ((ADouble) obj).getDoubleValue();
            case BOOLEAN:
                return constantValue.isTrue();
            case DATE:
                // Iceberg DATE is represented as days from epoch (int)
                return ((ADate) obj).getChrononTimeInDays();
            case DATETIME:
                // Iceberg TIMESTAMP is represented as microseconds from epoch
                return TimeUnit.MILLISECONDS.toMicros(((ADateTime) obj).getChrononTime());
            case TIME:
                // Iceberg TIME is represented as microseconds from midnight
                return TimeUnit.MILLISECONDS.toMicros(((ATime) obj).getChrononTime());
            default:
                LOGGER.debug("Unsupported literal type: {}", obj.getType());
                return null;
        }
    }

    /**
     * Converts a bare boolean constant to an Iceberg Expression (alwaysTrue / alwaysFalse).
     */
    private Expression createBooleanConstantExpression(ILogicalExpression expression) {
        ConstantExpression constExpr = (ConstantExpression) expression;
        if (constExpr.getValue().isTrue()) {
            return Expressions.alwaysTrue();
        } else if (constExpr.getValue().isFalse()) {
            return Expressions.alwaysFalse();
        }
        // null/missing constant — cannot determine truth value; do not push down
        return null;
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        return null;
    }

    private Expression handleFunction(ILogicalExpression expr, boolean insideNot) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        // Logical Connectives
        if (fid.equals(AlgebricksBuiltinFunctions.AND)) {
            return handleAnd(funcExpr, insideNot);
        } else if (fid.equals(AlgebricksBuiltinFunctions.OR)) {
            return handleOr(funcExpr, insideNot);
        } else if (fid.equals(AlgebricksBuiltinFunctions.NOT)) {
            return handleNot(funcExpr, insideNot);
        }

        // Null check
        if (fid.equals(AlgebricksBuiltinFunctions.IS_NULL)) {
            return handleIsNull(funcExpr);
        }

        // Comparison operators
        if (fid.equals(AlgebricksBuiltinFunctions.EQ) || fid.equals(AlgebricksBuiltinFunctions.NEQ)
                || fid.equals(AlgebricksBuiltinFunctions.LT) || fid.equals(AlgebricksBuiltinFunctions.LE)
                || fid.equals(AlgebricksBuiltinFunctions.GT) || fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return handleComparison(funcExpr, fid);
        }

        // String functions
        if (fid.equals(BuiltinFunctions.STRING_STARTS_WITH)) {
            return handleStringStartsWith(funcExpr);
        }
        LOGGER.trace("Unsupported function for Iceberg filter pushdown: {}", fid);
        return null;
    }

    private Expression handleAnd(AbstractFunctionCallExpression funcExpr, boolean insideNot)
            throws AlgebricksException {
        Expression result = null;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            Expression argExpr = createIcebergExpression(argRef.getValue(), insideNot);
            if (argExpr == null) {
                if (insideNot) {
                    // Under an odd number of NOTs, partial AND pushdown is unsafe.
                    // NOT(AND(e1, e2)) ≡ NOT(e1) OR NOT(e2), so dropping e2 and pushing
                    // NOT(e1) would incorrectly prune rows where e2 is false.
                    return null;
                }
                // Under an even number of NOTs (including zero — top-level or inside OR):
                // skip un-pushable children safely.
                // - Top-level / inside OR at top context: the residual filter in the plan
                //   evaluates skipped conjuncts.
                // - Inside OR: AND(e1, e2) reduced to e1 makes the disjunct weaker, so the
                //   overall OR passes more rows — a safe over-approximation.
                // - Under double-NOT: NOT(NOT(AND(e1,e2))) reduced to NOT(NOT(e1)) = e1,
                //   which is also a safe over-approximation.
                continue;
            }
            result = (result == null) ? argExpr : Expressions.and(result, argExpr);
        }
        return result;
    }

    private Expression handleOr(AbstractFunctionCallExpression funcExpr, boolean insideNot) throws AlgebricksException {
        Expression result = null;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            // insideNot (odd-NOT parity) propagates unchanged through OR: OR itself does
            // not introduce or remove negation.
            Expression argExpr = createIcebergExpression(argRef.getValue(), insideNot);
            if (argExpr == null) {
                // One un-pushable disjunct makes the whole OR un-pushable for correctness:
                // skipping a disjunct would over-prune rows matching only that disjunct.
                return null;
            }
            result = (result == null) ? argExpr : Expressions.or(result, argExpr);
        }
        return result;
    }

    private Expression handleNot(AbstractFunctionCallExpression funcExpr, boolean insideNot)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 1) {
            return null;
        }
        ILogicalExpression innerExpr = args.get(0).getValue();
        // NOT toggles the negation context: each NOT flips whether we are inside an odd
        // or even number of negations.  Partial AND pushdown is only unsafe under an odd
        // number of NOTs (insideNot=true), because:
        //   NOT(AND(e1,e2))  — partial push of e1 yields NOT(e1), which is too strong.
        //   NOT(NOT(AND(e1,e2))) — partial push of e1 yields NOT(NOT(e1)) = e1, which is
        //   a safe over-approximation (passes more rows than the full AND).
        Expression inner = createIcebergExpression(innerExpr, !insideNot);
        return (inner != null) ? Expressions.not(inner) : null;
    }

    private Expression handleIsNull(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 1) {
            return null;
        }
        ILogicalExpression arg = args.get(0).getValue();
        String columnName = tryGetColumnName(arg);
        if (columnName == null) {
            return null;
        }
        return Expressions.isNull(columnName);
    }

    private Expression handleComparison(AbstractFunctionCallExpression funcExpr, FunctionIdentifier fid)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 2) {
            return null;
        }
        ILogicalExpression left = args.get(0).getValue();
        ILogicalExpression right = args.get(1).getValue();
        String columnName = tryGetColumnName(left);
        Object literalValue;
        boolean flipped = false;
        if (columnName != null) {
            // Normal: column <op> literal
            literalValue = tryGetLiteralValue(right);
        } else {
            // Try flipped: literal <op> column
            columnName = tryGetColumnName(right);
            if (columnName == null) {
                // Neither side is a known column; could be column <op> column — not supported
                return null;
            }
            literalValue = tryGetLiteralValue(left);
            flipped = true;
        }
        if (literalValue == null) {
            return null;
        }
        // When operands are flipped, reverse the comparison direction
        FunctionIdentifier effectiveFid = flipped ? flipComparison(fid) : fid;
        return buildComparisonExpression(effectiveFid, columnName, literalValue,
                tryGetLiteralTypeTag(flipped ? left : right));
    }

    /** @return the type tag behind a constant literal, or {@code null} if the expression is not one. */
    private ATypeTag tryGetLiteralTypeTag(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        ConstantExpression constExpr = (ConstantExpression) expression;
        if (!(constExpr.getValue() instanceof AsterixConstantValue)) {
            return null;
        }
        return ((AsterixConstantValue) constExpr.getValue()).getObject().getType().getTypeTag();
    }

    /**
     * @implNote package-private so {@code IcebergTemporalPredicateWideningTest} can pin the fail-closed guard below;
     *           it uses no instance state.
     */
    static Expression buildComparisonExpression(FunctionIdentifier fid, String columnName, Object value,
            ATypeTag literalTag) {
        if (isMillisecondTruncated(literalTag)) {
            // The column is finer-grained than the literal, so compare against the whole millisecond rather than the
            // single instant it names. Fail CLOSED: if the value is not the long of microseconds createLiteralValue
            // produces today, decline rather than dropping through to the exact form below, which is the defect this
            // method exists to prevent. Reachable only if createLiteralValue changes -- ATime's chronon is an int, so
            // narrowing the TIME case to match it would otherwise switch this widening off silently.
            if (!(value instanceof Long)) {
                LOGGER.debug(
                        "Temporal literal of type {} is {}, not a Long; skipping pushdown rather than "
                                + "pushing an un-widened comparison",
                        literalTag, value == null ? "null" : value.getClass());
                return null;
            }
            return buildTruncatedTemporalComparison(fid, columnName, (Long) value, literalTag == ATypeTag.TIME);
        }
        if (fid.equals(AlgebricksBuiltinFunctions.EQ)) {
            return Expressions.equal(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.NEQ)) {
            return Expressions.notEqual(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return Expressions.lessThan(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return Expressions.lessThanOrEqual(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return Expressions.greaterThan(columnName, value);
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return Expressions.greaterThanOrEqual(columnName, value);
        }
        return null;
    }

    /**
     * @return {@code true} if reading this literal's Iceberg counterpart truncates it. {@code DATETIME} and
     *         {@code TIME} are read back through {@code ADateTime}/{@code ATime}, which hold <b>milliseconds</b>,
     *         while the Iceberg column is microseconds (or nanoseconds), so the stored sub-millisecond digits are
     *         dropped. {@code DATE} is whole days on both sides and loses nothing.
     */
    static boolean isMillisecondTruncated(ATypeTag literalTag) {
        return literalTag == ATypeTag.DATETIME || literalTag == ATypeTag.TIME;
    }

    /**
     * Builds a pushed comparison for a millisecond-precision literal against a finer-grained Iceberg column.
     * <p>
     * The engine decides the answer from the <b>truncated</b> value, so the pushed filter must admit every stored
     * value that truncates into the literal's millisecond — otherwise Iceberg drops a data file, row group or
     * <b>delete file</b> holding a row the engine would have matched, and the query silently returns the wrong rows.
     * Each operator below is the <em>exact</em> set rather than merely a wider one, which is what lets {@code AND},
     * {@code OR} and {@code NOT} compose: {@link #handleNot} wraps the result in {@code Expressions.not(..)}, and
     * negating a merely-wider predicate yields a narrower one.
     * <p>
     * Both bounds are whole milliseconds on purpose. Iceberg rescales a pushed microsecond bound when binding it to a
     * {@code timestamp_ns} column, so a closed microsecond window ({@code [L, L+999us]}) would still drop the last
     * 999 nanoseconds of the millisecond; a whole-millisecond bound converts exactly at any target precision.
     *
     * <p>
     * <b>Worked example the comments below refer to.</b> {@code L = 1773565200123000} microseconds, i.e.
     * {@code 2026-03-15T09:00:00.123Z}. Its window is {@code [1773565200123000, 1773565200124000)}, so stored values
     * {@code ...123000}, {@code ...123456} and {@code ...123999} are all inside it and all read back as
     * {@code .123}; {@code ...124000} is outside and reads back as {@code .124}.
     * <p>
     * <b>Partition transforms are safe, including the one that looks dangerous.</b> {@code year}/{@code month}
     * /{@code day}/{@code hour} have whole-second boundaries, so a window at most one millisecond wide cannot
     * straddle one. {@code bucket[N]} hashes the raw microseconds and preserves no ordering — which makes it safe
     * rather than hazardous: an ordered range cannot be projected onto it at all, so every bucket is kept and
     * nothing is wrongly pruned. The exact-instant form this replaced <em>did</em> project onto the bucket, pruning
     * to a single one and losing same-millisecond rows that hash elsewhere, so the widening removes a latent
     * partition-level version of this same defect. Pinned by
     * {@code IcebergTemporalPredicateWideningTest#bucketPartitionedTimestampIsNeverWronglyPruned}.
     * <p>
     * <b>Not measured</b>: {@code timestamp_ns} <em>without</em> time zone, and whether the {@code !=} disjunction
     * projects correctly onto the <em>ordered</em> transforms — it is covered for {@code bucket}.
     *
     * @param nonNegativeDomain {@code true} for {@code TIME}, whose values cannot be negative, so truncation is
     *                          always a floor and the window is uniform.
     * @implNote package-private and static so {@code IcebergTemporalPredicateWideningTest} can evaluate the
     *           produced expression directly against stored values; it uses no instance state.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Pushes the whole millisecond a truncated temporal literal denotes, so Iceberg cannot prune a "
            + "file, row group or delete file that holds a row the engine matches")
    static Expression buildTruncatedTemporalComparison(FunctionIdentifier fid, String columnName, long literal,
            boolean nonNegativeDomain) {
        if (literal > Long.MAX_VALUE - MICROS_PER_MILLI || literal < Long.MIN_VALUE + MICROS_PER_MILLI) {
            // Handles literals within one millisecond of the long bounds, e.g. L = Long.MAX_VALUE: the upper bound
            // would be L + 1000, which wraps to Long.MIN_VALUE + 999 and inverts the comparison, so "before the end
            // of the millisecond" would suddenly mean "before the beginning of time". Reachable because
            // TimeUnit.toMicros SATURATES at the long bounds rather than overflowing, so an absurd datetime arrives
            // here already clamped rather than as an out-of-range value. Push nothing rather than something
            // meaningless -- declining loses pruning, pushing a wrapped bound loses rows.
            LOGGER.debug("Temporal literal {} too close to the long bounds to widen, skipping pushdown", literal);
            return null;
        }
        Expression atOrAfterStart, beforeEnd, beforeStart, atOrAfterEnd;
        if (nonNegativeDomain || literal > 0) {
            // Handles every TIME, and every DATETIME at or after 1970-01-01T00:00:00.001Z. Truncation toward zero is
            // a plain floor for non-negative values, so the window runs UPWARD from the literal:
            //   [L, L + 1000)  ->  for L = 1773565200123000, values 1773565200123000 .. 1773565200123999
            // ...123456 and ...123999 are admitted (both read back as .123); ...124000 is not (it reads back as
            // .124), and neither is ...122999. TIME joins this branch even at L = 0 because its domain is
            // non-negative -- midnight's window is [0, 1000), never the double-width one below.
            atOrAfterStart = Expressions.greaterThanOrEqual(columnName, literal);
            beforeEnd = Expressions.lessThan(columnName, literal + MICROS_PER_MILLI);
            beforeStart = Expressions.lessThan(columnName, literal);
            atOrAfterEnd = Expressions.greaterThanOrEqual(columnName, literal + MICROS_PER_MILLI);
        } else if (literal == 0) {
            // Handles the epoch millisecond alone, which is DOUBLE WIDTH because truncation runs toward zero rather
            // than flooring: toMillis(-999) == 0 and toMillis(999) == 0, so both sides of the epoch read back as
            // 1970-01-01T00:00:00.000. The window is therefore open at both ends:
            //   (-1000, +1000)  ->  values -999 .. 999
            // -999 and +999 are admitted; -1000 is not (it reads back as -1ms) and 1000 is not (it reads back as
            // +1ms). Using the branch above here would silently drop every row in the negative half.
            atOrAfterStart = Expressions.greaterThan(columnName, -MICROS_PER_MILLI);
            beforeEnd = Expressions.lessThan(columnName, MICROS_PER_MILLI);
            beforeStart = Expressions.lessThanOrEqual(columnName, -MICROS_PER_MILLI);
            atOrAfterEnd = Expressions.greaterThanOrEqual(columnName, MICROS_PER_MILLI);
        } else {
            // Handles every DATETIME before 1970-01-01T00:00:00.000Z, where truncating toward zero rounds UP (toward
            // less negative), so the window runs DOWNWARD from the literal -- the mirror of the first branch:
            //   (L - 1000, L]  ->  for L = -1000 (1969-12-31T23:59:59.999Z), values -1999 .. -1000
            // -1999 and -1500 are admitted (both read back as -1ms); -2000 is not (it reads back as -2ms) and -999
            // is not (it reads back as 0ms). This asymmetry is why the operators that are unsafe FLIP below the
            // epoch: above it <= and = lose rows, at and below it >= and = do.
            atOrAfterStart = Expressions.greaterThan(columnName, literal - MICROS_PER_MILLI);
            beforeEnd = Expressions.lessThanOrEqual(columnName, literal);
            beforeStart = Expressions.lessThanOrEqual(columnName, literal - MICROS_PER_MILLI);
            atOrAfterEnd = Expressions.greaterThan(columnName, literal);
        }
        // Each operator below is the EXACT set of stored values whose truncation satisfies it, not an
        // over-approximation -- see this method's javadoc for why exactness is what makes NOT compose. Values quoted
        // are for the worked example L = 1773565200123000 (window [...123000, ...124000)).
        if (fid.equals(AlgebricksBuiltinFunctions.EQ)) {
            // Handles "= L": everything inside the window, since all of it reads back as L.
            // >= 1773565200123000 AND < 1773565200124000 -- admits ...123000, ...123456, ...123999.
            return Expressions.and(atOrAfterStart, beforeEnd);
        } else if (fid.equals(AlgebricksBuiltinFunctions.NEQ)) {
            // Handles "!= L": everything OUTSIDE the window, on either side, so it must be a disjunction rather
            // than Iceberg's notEqual -- < 1773565200123000 OR >= 1773565200124000.
            return Expressions.or(beforeStart, atOrAfterEnd);
        } else if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            // Handles "< L": strictly below the window's start, because anything inside it reads back AS L and so
            // is not less than it -- < 1773565200123000. Unchanged from the un-widened form above the epoch.
            return beforeStart;
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            // Handles "<= L": everything up to the window's end, so ...123999 is included even though it is
            // numerically greater than the literal -- < 1773565200124000. This is the commonest lost-row case.
            return beforeEnd;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            // Handles "> L": strictly above the window's end, since ...123999 reads back as L and is therefore NOT
            // greater -- >= 1773565200124000. The un-widened form over-read here rather than losing rows.
            return atOrAfterEnd;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            // Handles ">= L": from the window's start upward -- >= 1773565200123000. Correct above the epoch even
            // un-widened, which is why this defect looked narrower than it was until the negative cases were run.
            return atOrAfterStart;
        }
        // Any other function identifier (a logical connective, startsWith, an unmodelled comparison): decline, so
        // handleComparison returns null and nothing is pushed for this conjunct.
        return null;
    }

    /**
     * Flips a binary comparison operator to handle reversed operand order.
     * e.g. {@code literal < column} becomes {@code column > literal}
     */
    private FunctionIdentifier flipComparison(FunctionIdentifier fid) {
        if (fid.equals(AlgebricksBuiltinFunctions.LT)) {
            return AlgebricksBuiltinFunctions.GT;
        } else if (fid.equals(AlgebricksBuiltinFunctions.LE)) {
            return AlgebricksBuiltinFunctions.GE;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GT)) {
            return AlgebricksBuiltinFunctions.LT;
        } else if (fid.equals(AlgebricksBuiltinFunctions.GE)) {
            return AlgebricksBuiltinFunctions.LE;
        }
        // EQ and NEQ are symmetric; return as-is
        return fid;
    }

    private Expression handleStringStartsWith(AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        if (args.size() != 2) {
            return null;
        }
        String columnName = tryGetColumnName(args.get(0).getValue());
        if (columnName == null) {
            return null;
        }
        Object prefix = tryGetLiteralValue(args.get(1).getValue());
        if (!(prefix instanceof String)) {
            return null;
        }
        return Expressions.startsWith(columnName, (String) prefix);
    }

    /**
     * Returns the Iceberg column name for an expression if it refers to a pushed-down filter path,
     * or {@code null} if the expression is not a column reference.
     */
    private String tryGetColumnName(ILogicalExpression expression) {
        if (!filterPaths.containsKey(expression)) {
            return null;
        }
        try {
            return (String) createColumnExpression(expression);
        } catch (Exception e) {
            LOGGER.debug("Failed to create column expression for {}", expression, e);
            return null;
        }
    }

    /**
     * Returns the Java literal value from a constant expression, or {@code null} if the
     * expression is not a constant or the constant type is unsupported.
     */
    private Object tryGetLiteralValue(ILogicalExpression expression) {
        if (expression.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        return createLiteralValue(expression);
    }

    protected Object createColumnExpression(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Unsupported column expression: " + expression);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            // The field could be a nested field
            List<String> fieldList = new ArrayList<>();
            fieldList = createPathExpression(path, fieldList);
            return recordSegments(String.join(".", fieldList), fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            String name = path.getFieldNames()[0];
            return recordSegments(name, List.of(name));
        } else {
            throw new RuntimeException("Unsupported column expression: " + expression);
        }
    }

    /**
     * Remembers which segments produced {@code name}, so a consumer never has to split the dotted name back apart.
     * A name two different paths can produce is recorded as ambiguous (an empty list) rather than resolved to either.
     *
     * @return {@code name}, so callers can return it directly
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Records unjoined path segments per reference name, marking collisions ambiguous so pushdown declines instead of guessing")
    private String recordSegments(String name, List<String> segments) {
        List<String> existing = pathSegments.get(name);
        if (existing == null) {
            pathSegments.put(name, new ArrayList<>(segments));
        } else if (!existing.equals(segments)) {
            pathSegments.put(name, List.of());
        }
        return name;
    }

    private List<String> createPathExpression(ARecordType path, List<String> fieldList) {
        if (path.getFieldNames().length != 1) {
            throw new RuntimeException("Error creating column expression");
        } else {
            fieldList.add(path.getFieldNames()[0]);
        }
        if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.OBJECT) {
            return createPathExpression((ARecordType) path.getFieldTypes()[0], fieldList);
        } else if (path.getFieldTypes()[0].getTypeTag() == ATypeTag.ANY) {
            return fieldList;
        } else {
            throw new RuntimeException("Error creating column expression");
        }
    }
}
