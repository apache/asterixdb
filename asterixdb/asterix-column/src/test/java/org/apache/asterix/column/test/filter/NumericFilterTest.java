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
package org.apache.asterix.column.test.filter;

import org.apache.asterix.column.filter.range.IColumnRangeFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.column.filter.range.accessor.ColumnRangeFilterValueAccessor;
import org.apache.asterix.column.filter.range.accessor.ConstantColumnRangeFilterValueAccessorFactory;
import org.apache.asterix.column.filter.range.compartor.GEColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.compartor.GTColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.compartor.LEColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.compartor.LTColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.range.evaluator.ANDColumnFilterEvaluatorFactory;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.DoubleColumnFilterWriter;
import org.apache.asterix.column.values.writer.filters.LongColumnFilterWriter;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.Test;

/**
 * A range filter that is too wide never produces a wrong answer, it only stops mega leaf nodes from being skipped,
 * so query tests cannot detect it. These tests check the stored bounds and the skip decision directly, against the
 * order SQL++ compares doubles in: NaN above +INF, and -0.0 equal to 0.0. The comparator for each predicate is the
 * one {@code ColumnRangeFilterBuilder} creates for it with the constant on the right; the runtime test
 * {@code column/filter/constant-left} covers the builder with the constant on the left.
 */
public class NumericFilterTest {

    @Test
    public void testDoubleMinMax() {
        assertDoubleBounds(100.0, 200.0, 100.0, 150.0, 200.0);
        assertDoubleBounds(-50.0, -10.0, -50.0, -30.0, -10.0);
        assertDoubleBounds(-0.5, 0.5, 0.5, -0.5, 0.0);
        assertDoubleBounds(7.25, 7.25, 7.25);
    }

    @Test
    public void testDoubleMinMaxAfterReset() {
        DoubleColumnFilterWriter writer = doubleLeaf(100.0, 200.0);
        writer.reset();
        writer.addDouble(-50.0);
        writer.addDouble(-10.0);
        assertDoubleBounds(writer, -50.0, -10.0);
    }

    @Test
    public void testLongMinMax() {
        assertLongBounds(100, 200, 100, 150, 200);
        assertLongBounds(-50, -10, -50, -30, -10);
        assertLongBounds(7, 7, 7);
    }

    @Test
    public void testDoubleLeafSkipping() throws HyracksDataException {
        DoubleColumnFilterWriter leaf = doubleLeaf(100.0, 150.0, 200.0);

        Assert.assertFalse("x > 500", isRead(gt(new ADouble(500.0), leaf)));
        Assert.assertFalse("x >= 500", isRead(ge(new ADouble(500.0), leaf)));
        Assert.assertFalse("x < 50", isRead(lt(new ADouble(50.0), leaf)));
        Assert.assertFalse("x <= 50", isRead(le(new ADouble(50.0), leaf)));
        Assert.assertFalse("x = 300", isRead(eq(new ADouble(300.0), leaf)));
        Assert.assertFalse("x = 50", isRead(eq(new ADouble(50.0), leaf)));

        Assert.assertTrue("x > 150", isRead(gt(new ADouble(150.0), leaf)));
        Assert.assertTrue("x >= 200", isRead(ge(new ADouble(200.0), leaf)));
        Assert.assertTrue("x < 150", isRead(lt(new ADouble(150.0), leaf)));
        Assert.assertTrue("x <= 100", isRead(le(new ADouble(100.0), leaf)));
        Assert.assertTrue("x = 150", isRead(eq(new ADouble(150.0), leaf)));
    }

    @Test
    public void testDoubleMinMaxWithNaN() {
        assertDoubleBounds(5.0, Double.NaN, 5.0, Double.NaN, 7.0);
        assertDoubleBounds(-0.5, Double.NaN, Double.NaN, -0.5);
    }

    @Test
    public void testDoubleLeafWithNaNSkipping() throws HyracksDataException {
        DoubleColumnFilterWriter leaf = doubleLeaf(5.0, Double.NaN, 7.0);

        Assert.assertTrue("x > 100", isRead(gt(new ADouble(100.0), leaf)));
        Assert.assertTrue("x > INF", isRead(gt(new ADouble(Double.POSITIVE_INFINITY), leaf)));
        Assert.assertTrue("x >= NaN", isRead(ge(new ADouble(Double.NaN), leaf)));
        Assert.assertTrue("x = NaN", isRead(eq(new ADouble(Double.NaN), leaf)));
        Assert.assertTrue("x < 6", isRead(lt(new ADouble(6.0), leaf)));

        Assert.assertFalse("x < 5", isRead(lt(new ADouble(5.0), leaf)));
        Assert.assertFalse("x = 3", isRead(eq(new ADouble(3.0), leaf)));
    }

    @Test
    public void testDoubleAllNaNLeafSkipping() throws HyracksDataException {
        DoubleColumnFilterWriter leaf = doubleLeaf(Double.NaN, Double.NaN);

        Assert.assertTrue("x > 5", isRead(gt(new ADouble(5.0), leaf)));
        Assert.assertTrue("x = NaN", isRead(eq(new ADouble(Double.NaN), leaf)));

        Assert.assertFalse("x < 5", isRead(lt(new ADouble(5.0), leaf)));
        Assert.assertFalse("x = 5", isRead(eq(new ADouble(5.0), leaf)));
    }

    @Test
    public void testDoubleLeafWrittenWithNaNBoundsIsRead() throws HyracksDataException {
        long nan = Double.doubleToLongBits(Double.NaN);
        AbstractColumnFilterWriter leaf = storedLeaf(nan, nan);

        Assert.assertTrue("x < 1", isRead(lt(new ADouble(1.0), leaf)));
        Assert.assertTrue("x <= 1", isRead(le(new ADouble(1.0), leaf)));
        Assert.assertTrue("x = 3", isRead(eq(new ADouble(3.0), leaf)));
        Assert.assertTrue("x > 1", isRead(gt(new ADouble(1.0), leaf)));
    }

    @Test
    public void testDoubleMissingColumnIsSkipped() throws HyracksDataException {
        AbstractColumnFilterWriter leaf = storedLeaf(Long.MAX_VALUE, Long.MIN_VALUE);

        Assert.assertFalse("x < 1", isRead(lt(new ADouble(1.0), leaf)));
        Assert.assertFalse("x = 3", isRead(eq(new ADouble(3.0), leaf)));
        Assert.assertFalse("x > 1", isRead(gt(new ADouble(1.0), leaf)));
    }

    @Test
    public void testDoubleNaNConstantSkipping() throws HyracksDataException {
        DoubleColumnFilterWriter leaf = doubleLeaf(100.0, 150.0, 200.0);

        Assert.assertTrue("x < NaN", isRead(lt(new ADouble(Double.NaN), leaf)));
        Assert.assertTrue("x <= NaN", isRead(le(new ADouble(Double.NaN), leaf)));

        Assert.assertFalse("x > NaN", isRead(gt(new ADouble(Double.NaN), leaf)));
        Assert.assertFalse("x >= NaN", isRead(ge(new ADouble(Double.NaN), leaf)));
        Assert.assertFalse("x = NaN", isRead(eq(new ADouble(Double.NaN), leaf)));
    }

    @Test
    public void testDoubleSignedZeroSkipping() throws HyracksDataException {
        DoubleColumnFilterWriter leaf = doubleLeaf(0.0);

        Assert.assertTrue("x = -0.0", isRead(eq(new ADouble(-0.0), leaf)));
        Assert.assertTrue("x <= -0.0", isRead(le(new ADouble(-0.0), leaf)));
        Assert.assertTrue("x >= -0.0", isRead(ge(new ADouble(-0.0), leaf)));

        Assert.assertFalse("x < -0.0", isRead(lt(new ADouble(-0.0), leaf)));
        Assert.assertFalse("x > -0.0", isRead(gt(new ADouble(-0.0), leaf)));
    }

    @Test
    public void testLongLeafSkipping() throws HyracksDataException {
        LongColumnFilterWriter leaf = longLeaf(100, 150, 200);

        Assert.assertFalse("x > 500", isRead(gt(new AInt64(500), leaf)));
        Assert.assertFalse("x < 50", isRead(lt(new AInt64(50), leaf)));
        Assert.assertFalse("x = 300", isRead(eq(new AInt64(300), leaf)));

        Assert.assertTrue("x > 150", isRead(gt(new AInt64(150), leaf)));
        Assert.assertTrue("x < 150", isRead(lt(new AInt64(150), leaf)));
        Assert.assertTrue("x = 150", isRead(eq(new AInt64(150), leaf)));
    }

    private static DoubleColumnFilterWriter doubleLeaf(double... values) {
        DoubleColumnFilterWriter writer = new DoubleColumnFilterWriter();
        for (double value : values) {
            writer.addDouble(value);
        }
        return writer;
    }

    private static LongColumnFilterWriter longLeaf(long... values) {
        LongColumnFilterWriter writer = new LongColumnFilterWriter();
        for (long value : values) {
            writer.addLong(value);
        }
        return writer;
    }

    /**
     * A mega leaf node as found on disk, with the given normalized bounds
     */
    private static AbstractColumnFilterWriter storedLeaf(long min, long max) {
        return new AbstractColumnFilterWriter() {
            @Override
            public long getMinNormalizedValue() {
                return min;
            }

            @Override
            public long getMaxNormalizedValue() {
                return max;
            }

            @Override
            public void reset() {
            }
        };
    }

    private static void assertDoubleBounds(double expectedMin, double expectedMax, double... values) {
        assertDoubleBounds(doubleLeaf(values), expectedMin, expectedMax);
    }

    private static void assertDoubleBounds(DoubleColumnFilterWriter writer, double expectedMin, double expectedMax) {
        Assert.assertEquals(expectedMin, Double.longBitsToDouble(writer.getMinNormalizedValue()), 0.0);
        Assert.assertEquals(expectedMax, Double.longBitsToDouble(writer.getMaxNormalizedValue()), 0.0);
    }

    private static void assertLongBounds(long expectedMin, long expectedMax, long... values) {
        LongColumnFilterWriter writer = longLeaf(values);
        Assert.assertEquals(expectedMin, writer.getMinNormalizedValue());
        Assert.assertEquals(expectedMax, writer.getMaxNormalizedValue());
    }

    // x < c
    private static IColumnRangeFilterEvaluatorFactory lt(IAObject c, AbstractColumnFilterWriter leaf) {
        return new GTColumnFilterEvaluatorFactory(constant(c), min(c, leaf));
    }

    // x <= c
    private static IColumnRangeFilterEvaluatorFactory le(IAObject c, AbstractColumnFilterWriter leaf) {
        return new GEColumnFilterEvaluatorFactory(constant(c), min(c, leaf));
    }

    // x > c
    private static IColumnRangeFilterEvaluatorFactory gt(IAObject c, AbstractColumnFilterWriter leaf) {
        return new LTColumnFilterEvaluatorFactory(constant(c), max(c, leaf));
    }

    // x >= c
    private static IColumnRangeFilterEvaluatorFactory ge(IAObject c, AbstractColumnFilterWriter leaf) {
        return new LEColumnFilterEvaluatorFactory(constant(c), max(c, leaf));
    }

    // x = c
    private static IColumnRangeFilterEvaluatorFactory eq(IAObject c, AbstractColumnFilterWriter leaf) {
        return new ANDColumnFilterEvaluatorFactory(new GEColumnFilterEvaluatorFactory(constant(c), min(c, leaf)),
                new LEColumnFilterEvaluatorFactory(constant(c), max(c, leaf)));
    }

    private static IColumnRangeFilterValueAccessorFactory constant(IAObject c) {
        return ConstantColumnRangeFilterValueAccessorFactory.createFactory(c);
    }

    private static IColumnRangeFilterValueAccessorFactory min(IAObject c, AbstractColumnFilterWriter leaf) {
        return bound(c.getType().getTypeTag(), true, leaf.getMinNormalizedValue());
    }

    private static IColumnRangeFilterValueAccessorFactory max(IAObject c, AbstractColumnFilterWriter leaf) {
        return bound(c.getType().getTypeTag(), false, leaf.getMaxNormalizedValue());
    }

    private static IColumnRangeFilterValueAccessorFactory bound(ATypeTag typeTag, boolean min, long normalized) {
        return provider -> {
            ColumnRangeFilterValueAccessor accessor = new ColumnRangeFilterValueAccessor(0, typeTag, min);
            accessor.setNormalizedValue(normalized);
            return accessor;
        };
    }

    private static boolean isRead(IColumnRangeFilterEvaluatorFactory filter) throws HyracksDataException {
        return filter.create(null).evaluate();
    }
}
