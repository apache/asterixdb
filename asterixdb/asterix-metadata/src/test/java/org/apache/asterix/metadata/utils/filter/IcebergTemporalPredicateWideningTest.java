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
import org.apache.hyracks.util.annotations.AiProvenance;
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
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Evaluates the widened predicate against stored values and compares with independently computed "
        + "millisecond semantics, covering every operator across the epoch and pre-1970 boundaries")
public class IcebergTemporalPredicateWideningTest {

    private static final String COLUMN = "ts";
    private static final Schema SCHEMA =
            new Schema(Types.NestedField.optional(1, COLUMN, Types.TimestampType.withZone()));
    private static final long MICROS_PER_MILLI = 1000L;

    private static final FunctionIdentifier[] OPERATORS =
            { AlgebricksBuiltinFunctions.EQ, AlgebricksBuiltinFunctions.NEQ, AlgebricksBuiltinFunctions.LT,
                    AlgebricksBuiltinFunctions.LE, AlgebricksBuiltinFunctions.GT, AlgebricksBuiltinFunctions.GE };

    /** Whole-millisecond literals, in micros, spanning both sides of the epoch and a modern timestamp. */
    private static final long[] LITERALS = { 1773565200123000L, 2000L, 1000L, 0L, -1000L, -2000L, -1773565200123000L };

    // ---------------------------------------------------------------- the property

    @Test
    public void widenedPredicateAdmitsExactlyWhatTheEngineMatches() {
        int checked = 0;
        for (long literal : LITERALS) {
            for (FunctionIdentifier op : OPERATORS) {
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal, false);
                Assert.assertNotNull("no expression built for " + op.getName() + " @ " + literal, pushed);
                for (long stored : candidatesAround(literal)) {
                    boolean engineMatches = holds(op, truncateTowardZero(stored), truncateTowardZero(literal));
                    Assert.assertEquals("op=" + op.getName() + " literal=" + literal + " stored=" + stored
                            + " (engine sees " + truncateTowardZero(stored) + "ms)", engineMatches,
                            admits(pushed, stored));
                    checked++;
                }
            }
        }
        // Guards against the candidate set silently collapsing; 7 literals x 6 operators x 20 values.
        Assert.assertEquals(840, checked);
    }

    /**
     * The epoch millisecond is <b>double width</b>. {@code MICROSECONDS.toMillis} truncates toward zero, so both
     * {@code -999us} and {@code +999us} read back as {@code 0ms} and a query for the epoch must return both.
     */
    @Test
    public void epochBucketSpansBothSidesOfZero() {
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, 0L, false);
        Assert.assertTrue("-999us truncates to 0ms", admits(eq, -999L));
        Assert.assertTrue(admits(eq, 0L));
        Assert.assertTrue("+999us truncates to 0ms", admits(eq, 999L));
        Assert.assertFalse("-1000us truncates to -1ms", admits(eq, -1000L));
        Assert.assertFalse("+1000us truncates to 1ms", admits(eq, 1000L));
    }

    /** Below the epoch the window sits on the other side of the literal, which flips which operators are at risk. */
    @Test
    public void preEpochWindowRunsUpwardToTheLiteral() {
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, -1000L, false);
        Assert.assertTrue(admits(eq, -1999L));
        Assert.assertTrue(admits(eq, -1500L));
        Assert.assertTrue(admits(eq, -1000L));
        Assert.assertFalse("-2000us truncates to -2ms", admits(eq, -2000L));
        Assert.assertFalse("-999us truncates to 0ms", admits(eq, -999L));
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
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal, false);
                for (long[] r : ranges) {
                    long lo = literal + r[0], hi = literal + r[1];
                    boolean couldMatch = rangeCouldMatch(op, lo, hi, truncateTowardZero(literal));
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
            if (holds(op, truncateTowardZero(v), literalMillis)) {
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

    /**
     * {@code ATime} cannot be negative, so truncation is always a floor and midnight takes the ordinary
     * {@code [L, L+1ms)} window — not the epoch's double-width one.
     */
    @Test
    public void timeAtMidnightUsesTheNonNegativeWindow() {
        Expression eq = buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, 0L, true);
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
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal, true);
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
        Expression eq =
                buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.EQ, COLUMN, lastMillisecondOfDay, true);
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
            Expression widened = buildTruncatedTemporalComparison(op, COLUMN, 1773565200123000L, false);
            Expression projected = Projections.inclusive(bucketSpec).project(widened);
            Assert.assertEquals("widened " + op.getName() + " must keep every bucket (no partition pruning on a range)",
                    Expression.Operation.TRUE, projected.op());
        }
        // Contrast: the exact-instant form this change replaced DID project onto the bucket, pruning to one bucket
        // and losing same-millisecond rows that hash elsewhere. Pinned so the distinction cannot silently regress.
        Expression exactInstant = org.apache.iceberg.expressions.Expressions.equal(COLUMN, 1773565200123000L);
        Assert.assertNotEquals(Expression.Operation.TRUE, Projections.inclusive(bucketSpec).project(exactInstant).op());
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
                Expression pushed = buildTruncatedTemporalComparison(op, COLUMN, literal, false);
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
                    buildTruncatedTemporalComparison(op, COLUMN, Long.MAX_VALUE, false));
            Assert.assertNull(buildTruncatedTemporalComparison(op, COLUMN, Long.MIN_VALUE, false));
            Assert.assertNull(buildTruncatedTemporalComparison(op, COLUMN, Long.MAX_VALUE - 1, false));
            Assert.assertNull(buildTruncatedTemporalComparison(op, COLUMN, Long.MIN_VALUE + 1, false));
        }
    }

    @Test
    public void unmodelledOperatorDeclinesRatherThanFallingBack() {
        Assert.assertNull(buildTruncatedTemporalComparison(AlgebricksBuiltinFunctions.AND, COLUMN, 1000L, false));
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
     * The oracle is this test's own {@code truncateTowardZero}, written independently of the production helper, so
     * agreement is evidence rather than tautology. Switching the helper to {@code Math.floorDiv} — the change most
     * likely to be made in good faith, since the uniform mapping is arguably the better behaviour — fails here
     * immediately, at unit speed, instead of silently mis-pruning pre-1970 sub-millisecond rows in production.
     */
    @Test
    public void readPathNarrowingMatchesTheWindowsAssumption() {
        Assert.assertEquals("the window's width must be the read path's chronon", MICROS_PER_MILLI,
                MillisecondChronon.MICROS_PER_MILLI);
        int checked = 0;
        for (long literal : LITERALS) {
            for (long stored : candidatesAround(literal)) {
                Assert.assertEquals("stored=" + stored, truncateTowardZero(stored),
                        MillisecondChronon.narrow(stored, TimeUnit.MICROSECONDS));
                // The same value at nanosecond scale must land on the same millisecond, which is what lets one
                // whole-millisecond bound serve timestamp, timestamp_ns and every future precision.
                Assert.assertEquals("stored=" + stored + " at nanos", truncateTowardZero(stored),
                        MillisecondChronon.narrow(stored * 1000L, TimeUnit.NANOSECONDS));
                checked++;
            }
        }
        Assert.assertEquals(140, checked);
        // The two properties the negative branch is built on, stated directly rather than inferred from the sweep.
        Assert.assertEquals("truncation must be toward zero, not a floor", 0L,
                MillisecondChronon.narrow(-999L, TimeUnit.MICROSECONDS));
        Assert.assertEquals("the epoch chronon is double width", 0L,
                MillisecondChronon.narrow(999L, TimeUnit.MICROSECONDS));
        Assert.assertEquals(-1L, MillisecondChronon.narrow(-1000L, TimeUnit.MICROSECONDS));
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

    /** What {@code MICROSECONDS.toMillis} does: integer division, truncating toward zero. */
    private static long truncateTowardZero(long micros) {
        return micros / MICROS_PER_MILLI;
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
        private final long value;

        private OneValueRow(long value) {
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
            return Arrays.toString(new long[] { value });
        }
    }
}
