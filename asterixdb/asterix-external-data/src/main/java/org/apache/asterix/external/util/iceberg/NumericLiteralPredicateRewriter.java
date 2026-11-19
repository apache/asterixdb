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

import java.math.BigDecimal;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Gives up a pushed comparison that Iceberg would decide differently from the engine because of how it converts or
 * orders a numeric literal.
 * <p>
 * Binding converts the literal to the column's type, and for a {@code float} or {@code decimal} column that
 * conversion can round: a double becomes {@code floatValue()}, and anything becomes a decimal through
 * {@code setScale(scale, HALF_UP)}. The engine does not round the literal. It compares a float against a double by
 * widening the float, and reads a decimal as a double. Once the literal has been rounded across a stored value, the
 * two disagree and Iceberg skips a file holding a row the engine matches:
 * <ul>
 * <li>{@code decimal(10,2)} value {@code 1.23}, {@code WHERE d > 1.225}: the engine matches, while Iceberg compares
 * against {@code 1.23} and skips the file. {@code d != 1.225} loses the same row.</li>
 * <li>{@code float} value {@code 0.1f}, {@code WHERE f > 0.1}: the engine compares {@code 0.10000000149 > 0.1} and
 * matches, while Iceberg compares {@code 0.1f > 0.1f} and skips the file.</li>
 * </ul>
 * Zero is the other case, on {@code float} and {@code double} columns alike, and needs no rounding: the engine treats
 * {@code -0.0} and {@code 0.0} as equal, while Iceberg orders file bounds with {@link Float#compareTo} and
 * {@link Double#compareTo}, which put {@code -0.0} below {@code 0.0}. A file holding only {@code -0.0} has bounds
 * {@code [-0.0, -0.0]}, so {@code WHERE f = 0} or {@code WHERE f >= 0} skips it while the engine matches its row.
 * <p>
 * NaN is the last, again on {@code float} and {@code double} columns: the engine orders NaN above every number, so
 * {@code NaN > 1} holds, while Iceberg treats NaN as never matching an ordering comparison and leaves it out of the
 * upper bound. A file holding NaN and nothing above {@code 1} is skipped by {@code WHERE f > 1}. So {@code >} and
 * {@code >=} on those columns are pushed together with {@code isNaN}, which Iceberg answers from each file's NaN count,
 * and a NOT over such a comparison is given up, since Iceberg would turn {@code NOT (f < 1)} into {@code f >= 1}.
 * <p>
 * A comparison is kept only when Iceberg's converted literal provably orders every stored value the way the engine
 * does, and is otherwise replaced by {@code alwaysTrue}, which only ever admits more files; the engine still applies
 * the real predicate to the rows. The weakening rules are those of {@link TimestampZonePredicateRewriter}.
 * <p>
 * Columns of every other type are left alone: apart from zero, a {@code double} column receives every numeric literal
 * exactly as the engine widens it, and an integral column either receives an integral literal exactly or rejects a
 * fractional one, which {@code IcebergParquetRecordReaderFactory#bindableOrNoFilter} then drops.
 */
public final class NumericLiteralPredicateRewriter {

    /**
     * Every decimal of at most this many significant digits converts to a distinct double, so over such decimals the
     * conversion preserves order strictly and comparing two of them as doubles agrees with comparing them exactly.
     */
    private static final int MAX_EXACT_DECIMAL_DIGITS = 15;

    private NumericLiteralPredicateRewriter() {
    }

    /**
     * @param expression the pushed filter, or {@code null}
     * @param schema     the Iceberg schema the filter will be bound against
     * @return an expression in which Iceberg decides no comparison differently from the engine
     */
    public static Expression rewrite(Expression expression, Schema schema) {
        if (expression == null || schema == null) {
            return expression;
        }
        if (expression instanceof And) {
            And and = (And) expression;
            return Expressions.and(rewrite(and.left(), schema), rewrite(and.right(), schema));
        } else if (expression instanceof Or) {
            // Dropping one side of a disjunction strengthens it, so it is given up whole when anything under it would
            // have to be dropped; widening a side only widens it.
            if (hasInexact(expression, schema)) {
                return Expressions.alwaysTrue();
            }
            Or or = (Or) expression;
            return Expressions.or(rewrite(or.left(), schema), rewrite(or.right(), schema));
        } else if (expression instanceof Not) {
            // A weaker child yields a stronger negation, so neither dropping nor widening under it is safe
            Expression child = ((Not) expression).child();
            return hasInexact(child, schema) || hasFloatingComparison(child, schema) ? Expressions.alwaysTrue()
                    : expression;
        } else if (expression instanceof UnboundPredicate) {
            UnboundPredicate<?> predicate = (UnboundPredicate<?>) expression;
            return isInexact(predicate, schema) ? Expressions.alwaysTrue() : admittingNaN(predicate, schema);
        }
        return expression;
    }

    /** @return {@code predicate}, or it together with the files holding NaN where the engine would match NaN rows */
    private static Expression admittingNaN(UnboundPredicate<?> predicate, Schema schema) {
        Expression.Operation op = predicate.op();
        if ((op == Expression.Operation.GT || op == Expression.Operation.GT_EQ)
                && isFloatingComparison(predicate, schema)) {
            return Expressions.or(predicate, Expressions.isNaN(((NamedReference<?>) predicate.term()).name()));
        }
        return predicate;
    }

    private static boolean hasFloatingComparison(Expression expression, Schema schema) {
        if (expression instanceof And) {
            return hasFloatingComparison(((And) expression).left(), schema)
                    || hasFloatingComparison(((And) expression).right(), schema);
        } else if (expression instanceof Or) {
            return hasFloatingComparison(((Or) expression).left(), schema)
                    || hasFloatingComparison(((Or) expression).right(), schema);
        } else if (expression instanceof Not) {
            return hasFloatingComparison(((Not) expression).child(), schema);
        } else if (expression instanceof UnboundPredicate) {
            return isFloatingComparison((UnboundPredicate<?>) expression, schema);
        }
        return false;
    }

    /** @return whether {@code predicate} compares a {@code float} or {@code double} column with a literal */
    private static boolean isFloatingComparison(UnboundPredicate<?> predicate, Schema schema) {
        if (!(predicate.term() instanceof NamedReference) || predicate.literal() == null) {
            return false;
        }
        Type type = schema.findType(((NamedReference<?>) predicate.term()).name());
        return type != null && (type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE);
    }

    private static boolean hasInexact(Expression expression, Schema schema) {
        if (expression instanceof And) {
            return hasInexact(((And) expression).left(), schema) || hasInexact(((And) expression).right(), schema);
        } else if (expression instanceof Or) {
            return hasInexact(((Or) expression).left(), schema) || hasInexact(((Or) expression).right(), schema);
        } else if (expression instanceof Not) {
            return hasInexact(((Not) expression).child(), schema);
        } else if (expression instanceof UnboundPredicate) {
            return isInexact((UnboundPredicate<?>) expression, schema);
        }
        return false;
    }

    private static boolean isInexact(UnboundPredicate<?> predicate, Schema schema) {
        if (!(predicate.term() instanceof NamedReference) || predicate.literal() == null) {
            return false;
        }
        Type type = schema.findType(((NamedReference<?>) predicate.term()).name());
        if (type == null) {
            return false;
        }
        switch (type.typeId()) {
            case FLOAT:
                return !floatExact(predicate.literal()) || convertsToZero(predicate.literal(), type);
            case DOUBLE:
                return convertsToZero(predicate.literal(), type);
            case DECIMAL:
                return !decimalExact(predicate.literal(), (Types.DecimalType) type);
            default:
                return false;
        }
    }

    /**
     * @return whether comparing a {@code float} column against Iceberg's conversion of {@code literal} agrees with the
     *         engine's comparison against {@code literal}, for every stored value
     */
    private static boolean floatExact(Literal<?> literal) {
        Object value = literal.value();
        Float converted = convertedValue(literal, Types.FloatType.get(), Float.class);
        if (converted == null) {
            // not convertible, or out of the float range: Iceberg would compare against a sentinel, never the value
            return false;
        }
        if (value instanceof Double) {
            // the engine widens the stored float and compares doubles
            return converted.doubleValue() == (Double) value;
        } else if (value instanceof Float) {
            return true;
        } else if (value instanceof Integer || value instanceof Long) {
            // the engine narrows the integer to a float through a double; Iceberg narrows it directly
            return Float.compare(converted, (float) ((Number) value).doubleValue()) == 0;
        }
        return false;
    }

    /**
     * @return whether comparing a decimal column against Iceberg's conversion of {@code literal} agrees with the
     *         engine's comparison of the column, read as a double, against {@code literal}, for every stored value
     */
    private static boolean decimalExact(Literal<?> literal, Types.DecimalType type) {
        if (type.precision() > MAX_EXACT_DECIMAL_DIGITS) {
            // two distinct stored values may read back as the same double, which the engine then calls equal
            return false;
        }
        Object value = literal.value();
        if (!(value instanceof Double || value instanceof Float || value instanceof Integer || value instanceof Long)) {
            return false;
        }
        BigDecimal converted = convertedValue(literal, type, BigDecimal.class);
        return converted != null && converted.precision() <= MAX_EXACT_DECIMAL_DIGITS
                && converted.doubleValue() == ((Number) value).doubleValue();
    }

    /** @return whether binding turns {@code literal} into a zero of either sign, which Iceberg and the engine order apart */
    private static boolean convertsToZero(Literal<?> literal, Type type) {
        Number converted = convertedValue(literal, type, Number.class);
        return converted != null && converted.doubleValue() == 0.0;
    }

    /**
     * @return the literal converted to {@code type} exactly as binding converts it, or {@code null} when binding would
     *         reject it or replace it with an above-max or below-min sentinel
     */
    private static <T> T convertedValue(Literal<?> literal, Type type, Class<T> expected) {
        try {
            Literal<?> converted = literal.to(type);
            Object value = converted == null ? null : converted.value();
            return expected.isInstance(value) ? expected.cast(value) : null;
        } catch (RuntimeException e) {
            return null;
        }
    }
}
