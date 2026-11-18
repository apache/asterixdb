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

import static org.apache.asterix.metadata.utils.filter.IcebergTableFilterBuilder.buildComparisonExpression;
import static org.apache.asterix.metadata.utils.filter.IcebergTableFilterBuilder.buildTruncatedTemporalComparison;
import static org.apache.asterix.metadata.utils.filter.IcebergTableFilterBuilder.isMillisecondTruncated;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.external.util.MillisecondChronon;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * The pushed predicate for a millisecond-precision temporal literal must admit <b>exactly</b> the stored values the
 * engine would match.
 * <p>
 * Analytics reads an Iceberg {@code timestamp} through {@code ADateTime}, which holds milliseconds, so the stored
 * microseconds are truncated before the {@code WHERE} clause ever sees them. The predicate pushed into Iceberg is
 * evaluated against the <b>untruncated</b> value in the manifest, Parquet row-group statistics and delete-file
 * bounds. If it is narrower than the engine's own comparison, Iceberg discards a file holding a row the engine would
 * have matched — a live row goes missing, or an equality-deleted row reappears because its delete file was dropped.
 * <p>
 * Each case below evaluates the produced expression against raw stored values and compares the verdict with
 * millisecond semantics computed here, independently of the builder. That is the same property the cluster-level
 * suite checks, at unit speed, so a regression in the window arithmetic fails here first.
 * <p>
 * <b>A wrong window is not subtle in this test but is nearly invisible in production:</b> getting the epoch bucket or
 * the pre-1970 direction wrong changes results only for rows whose microseconds land in one specific millisecond, and
 * only when that row is the minimum of its file or row group.
 */
public class IcebergTemporalPredicateWideningTest {

    private static final String COLUMN = "ts";
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.optional(1, COLUMN, Types.TimestampType.withZone()));
    private static final long MICROS_PER_MILLI = 1000L;

    private static final FunctionIdentifier[] OPERATORS =
            { AlgebricksBuiltinFunctions.EQ, AlgebricksBuiltinFunctions.NEQ, AlgebricksBuiltinFunctions.LT,
                    AlgebricksBuiltinFunctions.LE, AlgebricksBuiltinFunctions.GT, AlgebricksBuiltinFunctions.GE };

    /**
     * Whole-millisecond literals, in micros, spanning both sides of the epoch and a modern timestamp. {@code -1877000}
     * is {@code 1969-12-31T23:59:58.123Z}, the millisecond holding {@code -1876544us}.
     */
    private static final long[] LITERALS =
            { 1773565200123000L, 2000L, 1000L, 0L, -1000L, -2000L, -1877000L, -1773565200123000L };

    // ---------------------------------------------------------------- the property

    @Test
    public void widenedPredicateAdmitsExactlyWhatTheEngineMatches() {
        int checked = 0;
        for (long literal : LITERALS) {
            for (FunctionIdentifier op : OPERATORS) {
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal);
                Assert.assertNotNull("no expression built for " + op.getName() + " @ " + literal, pushed);
                for (long stored : candidatesAround(literal)) {
                    boolean engineMatches = holds(op, floorMillis(stored), floorMillis(literal));
                    Assert.assertEquals("op=" + op.getName() + " literal=" + literal + " stored=" + stored
                            + " (engine sees " + floorMillis(stored) + "ms)", engineMatches, admits(pushed, stored));
                    checked++;
                }
            }
        }
        // Guards against the candidate set silently collapsing; 8 literals x 6 operators x 20 values.
        Assert.assertEquals(960, checked);
    }

    /**
     * The read path floors, so the epoch millisecond holds only {@code 0 .. 999us}; {@code -500us} is
     * {@code 1969-12-31T23:59:59.999} and belongs to the millisecond below.
     */
    @Test
    public void epochMillisecondStartsAtTheEpoch() {
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, 0L);
        Assert.assertTrue(admits(eq, 0L));
        Assert.assertTrue(admits(eq, 999L));
        Assert.assertFalse("-1us floors to -1ms", admits(eq, -1L));
        Assert.assertFalse("-500us floors to -1ms", admits(eq, -500L));
        Assert.assertFalse("+1000us floors to 1ms", admits(eq, 1000L));
    }

    /** Below the epoch the window still runs upward from the literal, exactly as it does above it. */
    @Test
    public void preEpochWindowRunsUpwardFromTheLiteral() {
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, -1000L);
        Assert.assertTrue(admits(eq, -1000L));
        Assert.assertTrue(admits(eq, -500L));
        Assert.assertTrue(admits(eq, -1L));
        Assert.assertFalse("-1001us floors to -2ms", admits(eq, -1001L));
        Assert.assertFalse("0us is the epoch", admits(eq, 0L));
        // 1969-12-31T23:59:58.123456Z reads back as .123, so a query for .123 must keep it
        Expression preEpochSubMilli =
                buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, -1877000L);
        Assert.assertTrue(admits(preEpochSubMilli, -1876544L));
        Assert.assertFalse(admits(preEpochSubMilli, -1877001L));
        Assert.assertFalse(admits(preEpochSubMilli, -1876000L));
    }

    /**
     * Widening admits more values than the old exact-instant push, so the fair question is whether it now drags in
     * <b>files that cannot contain a match</b> — paying I/O for nothing.
     * <p>
     * It does not. Iceberg keeps a file when the pushed predicate could be satisfied somewhere inside its recorded
     * minimum and maximum, and the widened form is an exact description of the matching set, so a file is read
     * <em>if and only if</em> its bounds permit a matching row. This asserts that equivalence directly against
     * {@code InclusiveMetricsEvaluator} — the same evaluator scan planning uses — over file ranges that sit before,
     * inside, across and after the window.
     * <p>
     * Note the effect is not uniformly "read more": for {@code >} and {@code !=} the widened form is <b>stricter</b>
     * than the old one. A file whose values all sit at {@code L+456} was read by the old {@code > L} even though
     * every row in it truncates to {@code L} and therefore fails the comparison; the widened form prunes it.
     */
    @Test
    public void widenedPredicateReadsAFileOnlyWhenItCouldContainAMatch() {
        long[][] ranges = { { -5000, -1000 }, { -1000, 500 }, { 1, 500 }, { 500, 999 }, { 1000, 5000 }, { 0, 0 },
                { 456, 456 }, { -5000, 5000 }, { 999, 1000 }, { -1, 1 } };
        for (long literal : new long[] { 1773565200123000L, 0L, -1773565200123000L }) {
            for (FunctionIdentifier op : OPERATORS) {
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal);
                for (long[] r : ranges) {
                    long lo = literal + r[0], hi = literal + r[1];
                    boolean couldMatch = rangeCouldMatch(op, lo, hi, floorMillis(literal));
                    Assert.assertEquals("op=" + op.getName() + " literal=" + literal + " file=[" + lo + "," + hi + "]",
                            couldMatch, readsFile(pushed, lo, hi));
                }
            }
        }
    }

    /** True if any stored value in {@code [lo, hi]} satisfies the operator under millisecond semantics. */
    private static boolean rangeCouldMatch(FunctionIdentifier op, long lo, long hi, long literalMillis) {
        // Sampling the endpoints is not enough — the satisfying set can sit strictly between them — so walk the
        // millisecond boundaries the range spans, which is where the truncated value can change.
        for (long v : boundaryValues(lo, hi)) {
            if (holds(op, floorMillis(v), literalMillis)) {
                return true;
            }
        }
        return false;
    }

    /** The endpoints plus every millisecond boundary inside the range — the only places truncation changes. */
    private static long[] boundaryValues(long lo, long hi) {
        List<Long> values = new ArrayList<>();
        values.add(lo);
        values.add(hi);
        long firstBoundary = Math.floorDiv(lo, MICROS_PER_MILLI) * MICROS_PER_MILLI;
        for (long b = firstBoundary; b <= hi + MICROS_PER_MILLI && b >= lo - MICROS_PER_MILLI; b += MICROS_PER_MILLI) {
            if (b >= lo && b <= hi) {
                values.add(b);
            }
            if (b - 1 >= lo && b - 1 <= hi) {
                values.add(b - 1);
            }
        }
        return values.stream().mapToLong(Long::longValue).toArray();
    }

    /** Runs the pushed predicate through the evaluator scan planning uses, against a file with these bounds. */
    private static boolean readsFile(Expression pushed, long lowerBound, long upperBound) {
        Map<Integer, ByteBuffer> lower = new HashMap<>();
        Map<Integer, ByteBuffer> upper = new HashMap<>();
        lower.put(1, Conversions.toByteBuffer(Types.TimestampType.withZone(), lowerBound));
        upper.put(1, Conversions.toByteBuffer(Types.TimestampType.withZone(), upperBound));
        Map<Integer, Long> valueCounts = new HashMap<>();
        valueCounts.put(1, 1L);
        Map<Integer, Long> nullCounts = new HashMap<>();
        nullCounts.put(1, 0L);
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/bounds-" + lowerBound + "-" + upperBound + ".parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024).withRecordCount(1)
                .withMetrics(new Metrics(1L, null, valueCounts, nullCounts, null, lower, upper)).build();
        return new InclusiveMetricsEvaluator(SCHEMA, pushed, true).eval(file);
    }

    // ---------------------------------------------------------------- TIME

    /** Midnight takes the same {@code [L, L+1ms)} window as every other {@code TIME}, and never reaches below it. */
    @Test
    public void timeAtMidnightUsesTheNonNegativeWindow() {
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, 0L);
        Assert.assertTrue(admits(eq, 0L));
        Assert.assertTrue(admits(eq, 999L));
        Assert.assertFalse(admits(eq, 1000L));
        Assert.assertFalse("negative times do not exist; the window must not reach below midnight", admits(eq, -1L));
    }

    @Test
    public void timeIsExactForEveryOperatorAcrossTheDay() {
        long[] timeLiterals = { 0L, 1000L, 43200123000L, 86399999000L };
        for (long literal : timeLiterals) {
            for (FunctionIdentifier op : OPERATORS) {
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal);
                Assert.assertNotNull(pushed);
                for (long stored : candidatesAround(literal)) {
                    if (stored < 0) {
                        continue; // outside TIME's domain
                    }
                    Assert.assertEquals("op=" + op.getName() + " literal=" + literal + " stored=" + stored,
                            holds(op, stored / MICROS_PER_MILLI, literal / MICROS_PER_MILLI), admits(pushed, stored));
                }
            }
        }
    }

    /**
     * The last millisecond of the day needs an upper bound of {@code 24:00:00.000}, which is one microsecond past the
     * largest valid TIME. Iceberg accepts it as a bound, so no special case is needed — this pins that.
     */
    @Test
    public void timeEndOfDayUpperBoundIsUsable() {
        long lastMillisecondOfDay = 86399999000L;
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, lastMillisecondOfDay);
        Assert.assertNotNull(eq);
        Assert.assertTrue(admits(eq, lastMillisecondOfDay));
        Assert.assertTrue("23:59:59.999999 still truncates into the last millisecond",
                admits(eq, lastMillisecondOfDay + 999));
    }

    // ---------------------------------------------------------------- partition transforms

    /**
     * Every Iceberg temporal partition transform except {@code bucket} has whole-second boundaries, so a window at
     * most one millisecond wide cannot straddle one and partition pruning stays exact. {@code bucket[N]} is the
     * exception: it hashes the raw microseconds, so two values in the same millisecond can land in different buckets.
     * <p>
     * This asserts the widened predicate is nonetheless <b>safe</b> on a bucket-partitioned timestamp — safer, in
     * fact, than the exact-instant form it replaced. Inclusive projection of an ordered range onto a non-order-
     * preserving {@code bucket} transform yields no partition filter at all, so <em>every</em> partition is kept and
     * nothing can be wrongly pruned. The old {@code equal(ts, L)} form, by contrast, projected to a single
     * {@code bucket = hash(L)} predicate — which would have pruned away the very rows that share L's millisecond but
     * hash elsewhere. So on bucket partitioning the widening removes a latent partition-level version of the same
     * bug rather than introducing one.
     */
    @Test
    public void bucketPartitionedTimestampIsNeverWronglyPruned() {
        PartitionSpec bucketSpec = PartitionSpec.builderFor(SCHEMA).bucket(COLUMN, 16).build();
        for (FunctionIdentifier op : OPERATORS) {
            Expression widened = buildTruncatedTemporalComparison(op, COLUMN, 1773565200123000L);
            Expression projected = Projections.inclusive(bucketSpec).project(widened);
            Assert.assertEquals("widened " + op.getName() + " must keep every bucket (no partition pruning on a range)",
                    Expression.Operation.TRUE, projected.op());
        }
        // Contrast: the exact-instant form this change replaced DID project onto the bucket, pruning to one bucket
        // and losing same-millisecond rows that hash elsewhere. Pinned so the distinction cannot silently regress.
        Expression exactInstant = org.apache.iceberg.expressions.Expressions.equal(COLUMN, 1773565200123000L);
        Assert.assertNotEquals(Expression.Operation.TRUE, Projections.inclusive(bucketSpec).project(exactInstant).op());
    }

    /**
     * The ordered transforms prune whole partitions, so the widened predicate's projection must keep the partition
     * holding every stored value the engine would match. A pre-epoch window that ran in the wrong direction would
     * project onto the wrong side of a day or hour boundary at the epoch: {@code -500us} lives in day {@code -1}
     * ({@code 1969-12-31}) and must be found there by {@code = 1969-12-31T23:59:59.999}.
     */
    @Test
    public void orderedPartitionTransformsKeepThePartitionOfEveryMatch() {
        long microsPerHour = TimeUnit.HOURS.toMicros(1);
        long microsPerDay = TimeUnit.DAYS.toMicros(1);
        PartitionSpec daySpec = PartitionSpec.builderFor(SCHEMA).day(COLUMN).build();
        PartitionSpec hourSpec = PartitionSpec.builderFor(SCHEMA).hour(COLUMN).build();
        int checked = 0;
        for (long literal : LITERALS) {
            for (FunctionIdentifier op : OPERATORS) {
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal);
                Expression byDay = Projections.inclusive(daySpec).project(pushed);
                Expression byHour = Projections.inclusive(hourSpec).project(pushed);
                for (long stored : candidatesAround(literal)) {
                    if (!holds(op, floorMillis(stored), floorMillis(literal))) {
                        continue;
                    }
                    Assert.assertTrue("day partition of " + stored + " pruned for " + op.getName() + " @ " + literal,
                            partitionKept(daySpec, byDay, (int) Math.floorDiv(stored, microsPerDay)));
                    Assert.assertTrue("hour partition of " + stored + " pruned for " + op.getName() + " @ " + literal,
                            partitionKept(hourSpec, byHour, (int) Math.floorDiv(stored, microsPerHour)));
                    checked++;
                }
            }
        }
        Assert.assertTrue("too few matching values to say anything: " + checked, checked > 400);
    }

    private static boolean partitionKept(PartitionSpec spec, Expression projected, int partitionValue) {
        return new Evaluator(spec.partitionType(), projected).eval(new OneValueRow(partitionValue));
    }

    // ---------------------------------------------------------------- nulls

    /**
     * Nulls affect only file pruning, not results: the {@code WHERE} clause stays in the plan, so a null timestamp is
     * excluded by the engine no matter what the pushed predicate does. The only way null handling could <em>lose</em>
     * data is if the widened predicate wrongly pruned a file that holds a real matching row — so this checks file
     * pruning, using {@link InclusiveMetricsEvaluator}, rather than row-level matching.
     * <p>
     * A file of only null timestamps holds no value that can satisfy an equality or range comparison (a null is
     * neither equal to, less than, nor greater than any datetime), so pruning it is always correct. This asserts the
     * widened value-comparison predicates do prune such a file — a small confirmation that widening did not turn a
     * harmless over-read into anything that could hide a real row. {@code !=} is excluded on purpose: an all-null
     * file is kept for it by both the old and the widened form, which is also correct.
     */
    @Test
    public void widenedPredicatePrunesAnAllNullFile() {
        FunctionIdentifier[] valueOps = { AlgebricksBuiltinFunctions.EQ, AlgebricksBuiltinFunctions.LT,
                AlgebricksBuiltinFunctions.LE, AlgebricksBuiltinFunctions.GT, AlgebricksBuiltinFunctions.GE };
        Map<Integer, ByteBuffer> noBounds = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        valueCounts.put(1, 4L);
        Map<Integer, Long> nullCounts = new HashMap<>();
        nullCounts.put(1, 4L); // every value in the file is null, so no lower/upper bound exists
        DataFile allNull = DataFiles.builder(PartitionSpec.unpartitioned()).withPath("/all-null.parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1024).withRecordCount(4)
                .withMetrics(new Metrics(4L, null, valueCounts, nullCounts, null, noBounds, noBounds)).build();
        for (long literal : LITERALS) {
            for (FunctionIdentifier op : valueOps) {
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal);
                Assert.assertFalse("all-null file cannot match " + op.getName() + " @ " + literal,
                        new InclusiveMetricsEvaluator(SCHEMA, pushed, true).eval(allNull));
            }
        }
    }

    // ---------------------------------------------------------------- declining rather than pushing something wrong

    @Test
    public void literalsNearTheLongBoundsDecline() {
        for (FunctionIdentifier op : OPERATORS) {
            Assert.assertNull("adding a millisecond to a saturated bound wraps and inverts the predicate",
                    buildTruncatedTemporalComparison(op, COLUMN, Long.MAX_VALUE));
            Assert.assertNull(buildTruncatedTemporalComparison(op, COLUMN, Long.MIN_VALUE));
            Assert.assertNull(buildTruncatedTemporalComparison(op, COLUMN, Long.MAX_VALUE - 1));
            Assert.assertNull(buildTruncatedTemporalComparison(op, COLUMN, Long.MIN_VALUE + 1));
        }
    }

    @Test
    public void unmodelledOperatorDeclinesRatherThanFallingBack() {
        Assert.assertNull(buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.AND, COLUMN, 1000L));
    }

    /**
     * A temporal literal whose value is not the long of microseconds {@code createLiteralValue} produces today must
     * make the builder <b>decline</b>, never fall through to the exact-instant comparison — that fall-through is the
     * defect the widening exists to prevent, and it would come back silently.
     * <p>
     * Unreachable as the code stands, and deliberately pinned anyway: {@code ATime}'s chronon is an {@code int}, so
     * narrowing {@code createLiteralValue}'s {@code TIME} case to match its source type is a plausible tidy-up that
     * would otherwise switch the widening off for {@code TIME} with no test noticing.
     */
    @Test
    public void nonLongTemporalLiteralDeclinesRatherThanPushingTheExactForm() {
        for (ATypeTag truncated : new ATypeTag[] { ATypeTag.DATETIME, ATypeTag.TIME }) {
            for (FunctionIdentifier op : OPERATORS) {
                Assert.assertNull("an int-valued " + truncated + " literal must not reach the un-widened form",
                        buildComparisonExpression(op, COLUMN, Integer.valueOf(1234), truncated));
                Assert.assertNull(buildComparisonExpression(op, COLUMN, null, truncated));
            }
        }
        // A genuinely non-temporal literal still takes the exact form -- timestamp-to-long exposes raw microseconds
        // as BIGINT, where both sides already agree and widening would be wrong.
        Assert.assertNotNull(
                buildComparisonExpression(AlgebricksBuiltinFunctions.EQ, COLUMN, 1773565200123456L, ATypeTag.BIGINT));
    }

    // ---------------------------------------------------------------- which types are truncated at all

    @Test
    public void onlyDatetimeAndTimeAreTruncated() {
        Assert.assertTrue(isMillisecondTruncated(ATypeTag.DATETIME));
        Assert.assertTrue(isMillisecondTruncated(ATypeTag.TIME));
        // DATE is whole days on both sides: widening it would lose pruning for no correctness gain.
        Assert.assertFalse(isMillisecondTruncated(ATypeTag.DATE));
        // timestamp-to-long exposes raw micros as BIGINT, so both sides already agree.
        Assert.assertFalse(isMillisecondTruncated(ATypeTag.BIGINT));
        Assert.assertFalse(isMillisecondTruncated(ATypeTag.STRING));
        Assert.assertFalse(isMillisecondTruncated(null));
    }

    // ---------------------------------------------------------------- the cross-module coupling

    /**
     * The window above is only correct while the read path narrows the way this test assumes. That narrowing lives in
     * another module ({@link MillisecondChronon#narrow}, called by the Iceberg parser), and the type system does not
     * tie the two together — so assert it here rather than describe it in a comment.
     * <p>
     * The oracle is {@code java.time}'s own millisecond of an instant, written independently of the production
     * helper, so agreement is evidence rather than tautology. Reverting the helper to {@code TimeUnit.toMillis},
     * which truncates toward zero, fails here at unit speed.
     */
    @Test
    public void readPathNarrowingMatchesTheWindowsAssumption() {
        Assert.assertEquals("the window's width must be the read path's chronon", MICROS_PER_MILLI,
                MillisecondChronon.MICROS_PER_MILLI);
        int checked = 0;
        for (long literal : LITERALS) {
            for (long stored : candidatesAround(literal)) {
                Assert.assertEquals("stored=" + stored, floorMillis(stored),
                        MillisecondChronon.narrow(stored, TimeUnit.MICROSECONDS));
                // The same value at nanosecond scale must land on the same millisecond, which is what lets one
                // whole-millisecond bound serve timestamp, timestamp_ns and every future precision.
                Assert.assertEquals("stored=" + stored + " at nanos", floorMillis(stored),
                        MillisecondChronon.narrow(stored * 1000L, TimeUnit.NANOSECONDS));
                checked++;
            }
        }
        Assert.assertEquals(160, checked);
        Assert.assertEquals("-500us is 1969-12-31T23:59:59.999", -1L,
                MillisecondChronon.narrow(-500L, TimeUnit.MICROSECONDS));
        Assert.assertEquals(-1877L, MillisecondChronon.narrow(-1876544L, TimeUnit.MICROSECONDS));
        Assert.assertEquals("an exact millisecond is unchanged", -1L,
                MillisecondChronon.narrow(-1000L, TimeUnit.MICROSECONDS));
        Assert.assertEquals("the epoch is unchanged", 0L, MillisecondChronon.narrow(0L, TimeUnit.MICROSECONDS));
        Assert.assertEquals("-1ns is 1969-12-31T23:59:59.999", -1L,
                MillisecondChronon.narrow(-1L, TimeUnit.NANOSECONDS));
        Assert.assertEquals(-1L, MillisecondChronon.narrow(-1_000_000L, TimeUnit.NANOSECONDS));
        Assert.assertEquals(-2L, MillisecondChronon.narrow(-1_000_001L, TimeUnit.NANOSECONDS));
        Assert.assertEquals("a millisecond value passes through", -7L,
                MillisecondChronon.narrow(-7L, TimeUnit.MILLISECONDS));
    }

    // ---------------------------------------------------------------- helpers

    /** Twenty values straddling the literal's millisecond, including both neighbouring milliseconds. */
    private static long[] candidatesAround(long literalMicros) {
        long[] offsets = { -2000, -1001, -1000, -999, -500, -1, 0, 1, 456, 499, 500, 999, 1000, 1001, 1456, 1999, 2000,
                2001, -1456, -1999 };
        List<Long> values = new ArrayList<>(offsets.length);
        for (long offset : offsets) {
            values.add(literalMicros + offset);
        }
        return values.stream().mapToLong(Long::longValue).toArray();
    }

    /** The millisecond containing an epoch-microsecond instant, as {@code java.time} computes it. */
    private static long floorMillis(long micros) {
        return Instant.EPOCH.plus(micros, ChronoUnit.MICROS).toEpochMilli();
    }

    private static boolean holds(FunctionIdentifier op, long storedMillis, long literalMillis) {
        if (op.equals(AlgebricksBuiltinFunctions.EQ)) {
            return storedMillis == literalMillis;
        } else if (op.equals(AlgebricksBuiltinFunctions.NEQ)) {
            return storedMillis != literalMillis;
        } else if (op.equals(AlgebricksBuiltinFunctions.LT)) {
            return storedMillis < literalMillis;
        } else if (op.equals(AlgebricksBuiltinFunctions.LE)) {
            return storedMillis <= literalMillis;
        } else if (op.equals(AlgebricksBuiltinFunctions.GT)) {
            return storedMillis > literalMillis;
        } else if (op.equals(AlgebricksBuiltinFunctions.GE)) {
            return storedMillis >= literalMillis;
        }
        throw new IllegalArgumentException(op.getName());
    }

    private static boolean admits(Expression pushed, long storedMicros) {
        return new Evaluator(SCHEMA.asStruct(), pushed).eval(new OneValueRow(storedMicros));
    }

    /** Minimal {@link StructLike} holding the single timestamp column under test. */
    private static final class OneValueRow implements StructLike {
        private final Object value;

        private OneValueRow(Object value) {
            this.value = value;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(value);
        }

        @Override
        public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return Arrays.toString(new Object[] { value });
        }
    }
}
