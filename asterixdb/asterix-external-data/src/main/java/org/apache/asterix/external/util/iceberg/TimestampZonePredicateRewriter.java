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
package org.apache.asterix.external.util.iceberg;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.external.util.TimestampZoneProjector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts a pushed temporal literal out of the frame the engine displays and back into the frame Iceberg stores,
 * for columns whose values the read path shifts by a time zone offset.
 * <p>
 * When a collection is created with a {@code timezone} option, {@code IcebergParquetDataParser} shifts every
 * UTC-adjusted timestamp by the zone offset in effect at that instant, so the value the engine compares is
 * {@code stored + offset}. The predicate pushed into Iceberg is evaluated against the manifest and row-group
 * statistics, which hold {@code stored}. Without this rewrite the two are compared across different frames and
 * Iceberg prunes files holding rows the engine matches — measured as <b>every row lost</b> on a
 * {@code timestamptz} column under {@code America/Los_Angeles}, since the seven-hour error dwarfs any file's bounds.
 * <p>
 * <b>Only the UTC-adjusted types are rewritten.</b> {@code timestamp} and {@code timestamp_ns} without a zone are
 * wall-clock readings that the read path never shifts, and {@code date} and {@code time} carry no zone at all;
 * measured, all four are correct today, and shifting their literals would break them. This is why the rewrite lives
 * here rather than in the filter builder: the Iceberg column type is only known once the schema is resolved. Nor is
 * anything rewritten under {@code timestamp-to-long}, which emits the stored epoch value unshifted. Which values are
 * shifted is decided by {@link org.apache.asterix.external.util.TimestampZoneProjector#isShiftedOnRead}, the rule the
 * reader itself applies, so the two cannot disagree.
 * <p>
 * <b>The inverse is not always a function</b>, which is why {@link java.time.zone.ZoneRules#getValidOffsets} does the
 * work rather than a single subtraction:
 * <ul>
 * <li>one offset — the ordinary case; the literal is shifted back exactly;</li>
 * <li>two offsets — an hour repeated by a daylight-saving fall-back, so two stored instants display the same
 * reading. Both would have to be pushed as a disjunction to stay exact, so the predicate is dropped instead;</li>
 * <li>no offsets — an hour skipped by a spring-forward, so no stored instant displays this reading at all. Dropping
 * is correct and loses nothing, since the engine matches no row either.</li>
 * </ul>
 * Dropping means {@code alwaysTrue}, which only ever admits more files; the engine still applies the real predicate
 * to the rows, so the result stays correct and only pruning is lost — for at most the couple of hours a year those
 * cases cover.
 * <p>
 * The weakening rules are the same ones {@link VariantPredicateRewriter#withoutVariantSubFieldPredicates} documents,
 * and for the same reason: a scan filter may only ever admit more files, never fewer.
 */
public final class TimestampZonePredicateRewriter {

    private static final long MICROS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);

    private TimestampZonePredicateRewriter() {
    }

    /**
     * @param expression the pushed filter, or {@code null}
     * @param schema     the Iceberg schema the filter will be bound against
     * @param zone       the collection's configured zone, or {@code null} when none is set — in which case the read
     *                   path shifts nothing and the expression is returned unchanged
     * @param timestampAsLong whether the collection emits timestamps as their epoch long ({@code timestamp-to-long})
     * @return an expression that is correct to compare against stored statistics
     */
    public static Expression rewrite(Expression expression, Schema schema, ZoneId zone, boolean timestampAsLong) {
        if (expression == null || schema == null || zone == null) {
            return expression;
        }
        if (expression instanceof And) {
            And and = (And) expression;
            return Expressions.and(rewrite(and.left(), schema, zone, timestampAsLong),
                    rewrite(and.right(), schema, zone, timestampAsLong));
        } else if (expression instanceof Or) {
            Or or = (Or) expression;
            // Dropping one side of a disjunction strengthens the filter, so an unconvertible descendant anywhere
            // under it forces the whole disjunction to be given up.
            return hasUnconvertible(or, schema, zone, timestampAsLong) ? Expressions.alwaysTrue()
                    : Expressions.or(rewrite(or.left(), schema, zone, timestampAsLong),
                            rewrite(or.right(), schema, zone, timestampAsLong));
        } else if (expression instanceof Not) {
            Not not = (Not) expression;
            // A weaker child yields a stronger negation, so the same applies.
            return hasUnconvertible(not, schema, zone, timestampAsLong) ? Expressions.alwaysTrue()
                    : Expressions.not(rewrite(not.child(), schema, zone, timestampAsLong));
        } else if (expression instanceof UnboundPredicate) {
            UnboundPredicate<?> predicate = (UnboundPredicate<?>) expression;
            if (!isZoneAdjusted(predicate, schema, timestampAsLong)) {
                return expression;
            }
            Long shifted = toStoredFrame(literalMicros(predicate), zone);
            return shifted == null ? Expressions.alwaysTrue() : withLiteral(predicate, shifted);
        }
        return expression;
    }

    /** @return {@code true} if any predicate under {@code expression} is one this rewrite would have to give up. */
    private static boolean hasUnconvertible(Expression expression, Schema schema, ZoneId zone,
            boolean timestampAsLong) {
        if (expression instanceof And) {
            return hasUnconvertible(((And) expression).left(), schema, zone, timestampAsLong)
                    || hasUnconvertible(((And) expression).right(), schema, zone, timestampAsLong);
        } else if (expression instanceof Or) {
            return hasUnconvertible(((Or) expression).left(), schema, zone, timestampAsLong)
                    || hasUnconvertible(((Or) expression).right(), schema, zone, timestampAsLong);
        } else if (expression instanceof Not) {
            return hasUnconvertible(((Not) expression).child(), schema, zone, timestampAsLong);
        } else if (expression instanceof UnboundPredicate) {
            UnboundPredicate<?> predicate = (UnboundPredicate<?>) expression;
            return isZoneAdjusted(predicate, schema, timestampAsLong)
                    && toStoredFrame(literalMicros(predicate), zone) == null;
        }
        return false;
    }

    /**
     * @return {@code true} when the predicate names a column the read path shifts, by the same
     *         {@link TimestampZoneProjector#isShiftedOnRead} rule the reader applies. A predicate with no literal (IS
     *         NULL and friends) is excluded: there is no value to convert and null-ness does not move with the zone.
     */
    private static boolean isZoneAdjusted(UnboundPredicate<?> predicate, Schema schema, boolean timestampAsLong) {
        if (literalMicros(predicate) == null) {
            return false;
        }
        String name = referenceName(predicate);
        if (name == null) {
            return false;
        }
        Type type = schema.findType(name);
        boolean utcAdjusted = type instanceof Types.TimestampType && ((Types.TimestampType) type).shouldAdjustToUTC()
                || type instanceof Types.TimestampNanoType && ((Types.TimestampNanoType) type).shouldAdjustToUTC();
        return TimestampZoneProjector.isShiftedOnRead(utcAdjusted, timestampAsLong);
    }

    /**
     * Converts a displayed value back to the stored one.
     *
     * @param displayMicros the literal as pushed — microseconds, in the frame the engine displays
     * @return the stored microseconds, or {@code null} when the reading is ambiguous or impossible in this zone, or
     *         the shift overflows
     */
    private static Long toStoredFrame(Long displayMicros, ZoneId zone) {
        if (displayMicros == null) {
            return null;
        }
        // The displayed value is a wall-clock reading carried as an epoch offset, so reading it back as UTC recovers
        // the local date-time the user wrote.
        LocalDateTime local = Instant
                .ofEpochSecond(Math.floorDiv(displayMicros, MICROS_PER_SECOND),
                        TimeUnit.MICROSECONDS.toNanos(Math.floorMod(displayMicros, MICROS_PER_SECOND)))
                .atOffset(ZoneOffset.UTC).toLocalDateTime();
        List<ZoneOffset> offsets = zone.getRules().getValidOffsets(local);
        if (offsets.size() != 1) {
            return null;
        }
        try {
            return Math.subtractExact(displayMicros, offsets.get(0).getTotalSeconds() * MICROS_PER_SECOND);
        } catch (ArithmeticException e) {
            // A far-out datetime literal saturates to the long bounds when widened to micros; wrapping it would push
            // a literal on the wrong side of every file.
            return null;
        }
    }

    /** @return the predicate's literal as microseconds, or {@code null} if it has none or it is not a number. */
    private static Long literalMicros(UnboundPredicate<?> predicate) {
        try {
            if (predicate.literal() == null) {
                return null;
            }
            Object value = predicate.literal().value();
            return value instanceof Long ? (Long) value : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** @return the predicate's reference name, or {@code null} if it does not have a simple named reference. */
    private static String referenceName(UnboundPredicate<?> predicate) {
        try {
            return predicate.ref().name();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** @return the same comparison against {@code literal}; unmodelled operations are given up rather than guessed. */
    private static Expression withLiteral(UnboundPredicate<?> predicate, long literal) {
        String name = referenceName(predicate);
        switch (predicate.op()) {
            case EQ:
                return Expressions.equal(name, literal);
            case NOT_EQ:
                return Expressions.notEqual(name, literal);
            case LT:
                return Expressions.lessThan(name, literal);
            case LT_EQ:
                return Expressions.lessThanOrEqual(name, literal);
            case GT:
                return Expressions.greaterThan(name, literal);
            case GT_EQ:
                return Expressions.greaterThanOrEqual(name, literal);
            default:
                return Expressions.alwaysTrue();
        }
    }
}
