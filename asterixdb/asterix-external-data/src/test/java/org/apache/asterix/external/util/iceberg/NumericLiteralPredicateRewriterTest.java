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

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * How {@link NumericLiteralPredicateRewriter} gives up a comparison. Which literals round, and that no row is lost, is
 * checked against real Iceberg tables and the engine's comparator by {@code IcebergNumericLiteralPushdownTest}.
 */
public class NumericLiteralPredicateRewriterTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.optional(1, "f", Types.FloatType.get()),
            Types.NestedField.optional(2, "d", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(3, "i", Types.IntegerType.get()),
            Types.NestedField.optional(4, "s", Types.StringType.get()),
            Types.NestedField.optional(5, "x", Types.DoubleType.get()));
    private static final String DROPPED = Expressions.alwaysTrue().toString();

    @Test
    public void roundedLiteralIsGivenUp() {
        Assert.assertEquals(DROPPED, rewrite(Expressions.greaterThan("f", 0.1)).toString());
        Assert.assertEquals(DROPPED, rewrite(Expressions.greaterThan("d", 1.225)).toString());
    }

    @Test
    public void exactLiteralIsKept() {
        assertKept(Expressions.lessThan("f", 1.5));
        assertKept(Expressions.greaterThan("d", 1.23));
        assertKept(Expressions.equal("d", 20));
    }

    /** The engine orders NaN above every number; Iceberg never matches NaN in an ordering comparison. */
    @Test
    public void greaterThanOnFloatingColumnsAlsoAdmitsNaN() {
        Assert.assertEquals(Expressions.or(Expressions.greaterThan("f", 1.5), Expressions.isNaN("f")).toString(),
                rewrite(Expressions.greaterThan("f", 1.5)).toString());
        Assert.assertEquals(Expressions.or(Expressions.greaterThanOrEqual("x", 2.0), Expressions.isNaN("x")).toString(),
                rewrite(Expressions.greaterThanOrEqual("x", 2.0)).toString());
        assertKept(Expressions.lessThanOrEqual("x", 2.0));
        assertKept(Expressions.equal("x", 2.0));
        assertKept(Expressions.notEqual("x", 2.0));
        assertKept(Expressions.greaterThan("i", 2));
    }

    /** Iceberg turns NOT (x < 2) into x >= 2, which never matches NaN, while the engine's NOT (NaN < 2) holds. */
    @Test
    public void notOverAFloatingComparisonIsGivenUp() {
        Assert.assertEquals(DROPPED, rewrite(Expressions.not(Expressions.lessThan("x", 2.0))).toString());
        Assert.assertEquals(DROPPED,
                rewrite(Expressions.not(Expressions.and(Expressions.equal("i", 3), Expressions.lessThan("f", 1.5))))
                        .toString());
        assertKept(Expressions.not(Expressions.greaterThan("d", 1.23)));
    }

    /** The engine treats -0.0 and 0.0 as equal; Iceberg orders them apart in file bounds. */
    @Test
    public void zeroLiteralIsGivenUpOnFloatingColumns() {
        Assert.assertEquals(DROPPED, rewrite(Expressions.equal("f", 0)).toString());
        Assert.assertEquals(DROPPED, rewrite(Expressions.greaterThanOrEqual("x", -0.0)).toString());
        assertKept(Expressions.equal("d", 0));
        assertKept(Expressions.equal("i", 0));
    }

    /** Only float and decimal columns can round; every other column is left to binding. */
    @Test
    public void otherColumnsAreLeftAlone() {
        assertKept(Expressions.greaterThan("i", 0.1));
        assertKept(Expressions.equal("s", "0.1"));
        assertKept(Expressions.greaterThan("nosuchcolumn", 0.1));
        assertKept(Expressions.isNull("f"));
    }

    /** Losing a conjunct weakens the filter, so the rest of an AND stays pushed. */
    @Test
    public void andKeepsTheExactSide() {
        Expression rewritten = rewrite(Expressions.and(Expressions.greaterThan("f", 0.1), Expressions.equal("i", 3)));
        Assert.assertEquals(Expressions.equal("i", 3).toString(), rewritten.toString());
    }

    /** Losing a disjunct, or weakening under a NOT, would strengthen the filter, so the whole of it is given up. */
    @Test
    public void orAndNotAreGivenUpWhole() {
        Assert.assertEquals(DROPPED,
                rewrite(Expressions.or(Expressions.greaterThan("f", 0.1), Expressions.equal("i", 3))).toString());
        Assert.assertEquals(DROPPED, rewrite(Expressions.not(Expressions.lessThanOrEqual("d", 1.225))).toString());
        Assert.assertEquals(DROPPED,
                rewrite(Expressions.not(Expressions.and(Expressions.greaterThan("f", 0.1), Expressions.equal("i", 3))))
                        .toString());
        assertKept(Expressions.or(Expressions.lessThan("f", 1.5), Expressions.equal("i", 3)));
    }

    private static Expression rewrite(Expression expression) {
        return NumericLiteralPredicateRewriter.rewrite(expression, SCHEMA);
    }

    private static void assertKept(Expression expression) {
        Assert.assertEquals(expression.toString(), rewrite(expression).toString());
    }
}
