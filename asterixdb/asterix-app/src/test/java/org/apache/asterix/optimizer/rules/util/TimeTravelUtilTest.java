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
package org.apache.asterix.optimizer.rules.util;

import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.iceberg.IcebergSnapshotUtils;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.optimizer.rules.ResolveTimeTravelValueRule;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateTimeImmediateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentTemporalValueUtil;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers the conversion of a folded {@code AT SNAPSHOT} / {@code AT TIMESTAMP} value into the string form the
 * Iceberg reader parses, and the rejection of values it could not make sense of.
 *
 * @see org.apache.asterix.external.util.iceberg.IcebergSnapshotUtils
 */
public class TimeTravelUtilTest {

    @Test
    public void testSnapshotId() throws Exception {
        Assert.assertEquals("8574821", snapshotId(new AInt64(8574821L)));
        Assert.assertEquals("42", snapshotId(new AInt32(42)));
        // a string passes straight through, which is what the original literal-only syntax produced
        Assert.assertEquals("8574821", snapshotId(new AString("8574821")));
    }

    @Test
    public void testTimestampEpochMillis() throws Exception {
        Assert.assertEquals("1755648000000", timestamp(new AInt64(1755648000000L)));
    }

    @Test
    public void testTimestampDate() throws Exception {
        int epochDay = (int) LocalDate.of(2026, 8, 20).toEpochDay();
        Assert.assertEquals("2026-08-20", timestamp(new ADate(epochDay)));
    }

    @Test
    public void testTimestampDateTimeAlwaysHasSeconds() throws Exception {
        long midnight = LocalDateTime.of(2026, 8, 20, 0, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        Assert.assertEquals("2026-08-20T00:00:00", timestamp(new ADateTime(midnight)));
    }

    @Test
    public void testTimestampDateTimeKeepsMillis() throws Exception {
        long instant = LocalDateTime.of(2026, 8, 20, 13, 45, 30, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
        Assert.assertEquals("2026-08-20T13:45:30.123", timestamp(new ADateTime(instant)));
    }

    /**
     * A snapshot id is an integer; a date there is a mistake worth naming rather than passing on to fail as an
     * unparseable number further down.
     */
    @Test
    public void testDateIsRejectedForSnapshotId() {
        assertRejected(TimeTravel.Type.SNAPSHOT_ID, new ADate((int) LocalDate.of(2026, 8, 20).toEpochDay()));
        assertRejected(TimeTravel.Type.SNAPSHOT_ID, new ADateTime(0L));
    }

    @Test
    public void testUnusableTypesAreRejected() {
        for (TimeTravel.Type type : TimeTravel.Type.values()) {
            assertRejected(type, new ADouble(1.5));
            assertRejected(type, ABoolean.TRUE);
            // a bare time of day cannot identify a snapshot
            assertRejected(type, new ATime(0));
        }
    }

    /**
     * The value normally arrives already folded; anything still holding a function call means folding could not
     * reduce it, and the scan has nothing usable.
     */
    @Test
    public void testUnfoldedValueIsRejected() {
        ScalarFunctionCallExpression notConstant =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CURRENT_DATE));
        TimeTravel timeTravel = new TimeTravel(new MutableObject<>(notConstant), TimeTravel.Type.SNAPSHOT_TIMESTAMP);
        try {
            TimeTravelUtil.resolve(timeTravel, null);
            Assert.fail("expected an unfolded value to be rejected");
        } catch (CompilationException e) {
            Assert.assertEquals(ErrorCode.EXPECTED_CONSTANT_VALUE.intValue(), e.getErrorCode());
        }
    }

    @Test
    public void testFoldedValueIsRead() throws Exception {
        TimeTravel timeTravel = new TimeTravel(
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(8574821L)))),
                TimeTravel.Type.SNAPSHOT_ID);
        Assert.assertEquals("8574821", TimeTravelUtil.resolve(timeTravel, null));
    }

    @Test
    public void testSmallIntegerWidths() throws Exception {
        Assert.assertEquals("7", snapshotId(new AInt8((byte) 7)));
        Assert.assertEquals("300", snapshotId(new AInt16((short) 300)));
    }

    /**
     * Snapshot ids are longs and Iceberg does not promise them positive, so the sign has to
     * survive rather than being formatted away.
     */
    @Test
    public void testNegativeAndExtremeSnapshotIds() throws Exception {
        Assert.assertEquals("-8574821", snapshotId(new AInt64(-8574821L)));
        Assert.assertEquals(String.valueOf(Long.MAX_VALUE), snapshotId(new AInt64(Long.MAX_VALUE)));
        Assert.assertEquals(String.valueOf(Long.MIN_VALUE), snapshotId(new AInt64(Long.MIN_VALUE)));
    }

    @Test
    public void testEpochBoundaries() throws Exception {
        Assert.assertEquals("1970-01-01", timestamp(new ADate(0)));
        Assert.assertEquals("1970-01-01T00:00:00", timestamp(new ADateTime(0L)));
        // before the epoch: the date must go backwards, not wrap
        Assert.assertEquals("1969-12-31", timestamp(new ADate(-1)));
    }

    @Test
    public void testEmptyStringPassesThroughForTheReaderToReject() throws Exception {
        // not this layer's job to validate the string's shape -- IcebergSnapshotUtils parses it
        Assert.assertEquals("", snapshotId(new AString("")));
    }

    /**
     * A clock in a time travel value is read while the query is compiled, and the reader takes the rendered
     * value as UTC. Run in a zone behind UTC so a clock taken in the controller's own zone would select an
     * instant hours in the past.
     */
    @Test
    public void testClockValueSelectsTheCurrentInstant() throws Exception {
        TimeZone saved = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
            long now = LocalDateTime.of(2026, 8, 20, 23, 45, 30, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
            IAObject clock =
                    ResolveTimeTravelValueRule.wallClockInUtc(BuiltinFunctions.CURRENT_DATETIME_IMMEDIATE, now);
            Assert.assertEquals(now, readerInstant(timestamp(clock)));

            // half an hour later it is the 21st in UTC but still the 20th in Los Angeles: the date follows UTC
            IAObject today = ResolveTimeTravelValueRule.wallClockInUtc(BuiltinFunctions.CURRENT_DATE_IMMEDIATE, now);
            Assert.assertEquals("2026-08-20", timestamp(today));
            IAObject tomorrow = ResolveTimeTravelValueRule.wallClockInUtc(BuiltinFunctions.CURRENT_DATE_IMMEDIATE,
                    now + TimeUnit.MINUTES.toMillis(30));
            Assert.assertEquals("2026-08-21", timestamp(tomorrow));

            IAObject time = ResolveTimeTravelValueRule.wallClockInUtc(BuiltinFunctions.CURRENT_TIME_IMMEDIATE, now);
            Assert.assertEquals(new ATime((int) TimeUnit.HOURS.toMillis(23) + (int) TimeUnit.MINUTES.toMillis(45)
                    + (int) TimeUnit.SECONDS.toMillis(30) + 123), time);

            Assert.assertNull(ResolveTimeTravelValueRule.wallClockInUtc(BuiltinFunctions.CURRENT_DATETIME, now));
        } finally {
            TimeZone.setDefault(saved);
        }
    }

    /**
     * The evaluators read the same instant in the job's zone through the same method; behind UTC the local day
     * and time of day trail UTC's.
     */
    @Test
    public void testCurrentValueInAZoneBehindUtc() {
        ZoneRules la = ZoneId.of("America/Los_Angeles").getRules();
        long now = LocalDateTime.of(2026, 8, 21, 0, 15, 30, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
        Assert.assertEquals(LocalDate.of(2026, 8, 20).toEpochDay(),
                CurrentTemporalValueUtil.valueAt(ATypeTag.DATE, now, la));
        Assert.assertEquals(
                TimeUnit.HOURS.toMillis(17) + TimeUnit.MINUTES.toMillis(15) + TimeUnit.SECONDS.toMillis(30) + 123,
                CurrentTemporalValueUtil.valueAt(ATypeTag.TIME, now, la));
        Assert.assertEquals(now - TimeUnit.HOURS.toMillis(7),
                CurrentTemporalValueUtil.valueAt(ATypeTag.DATETIME, now, la));
    }

    /**
     * A clock that renders a zone writes an offset, "Z" or "+03:00"; the reader must take the instant it names.
     */
    @Test
    public void testReaderTakesAnOffsetTimestampAsTheInstantItNames() throws Exception {
        long instant = LocalDateTime.of(2026, 8, 20, 13, 45, 30, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
        Assert.assertEquals(instant, readerInstant("2026-08-20T13:45:30.123Z"));
        Assert.assertEquals(instant, readerInstant("2026-08-20T16:45:30.123+03:00"));
        Assert.assertEquals(instant, readerInstant("2026-08-20T06:45:30.123-07:00"));
        Assert.assertEquals(instant, readerInstant("2026-08-20T13:45:30.123"));
    }

    /**
     * Only the time travel value reads the clock in UTC, and it does so without running the evaluator: the
     * evaluator itself still has no zone to read the clock in without a job, and keeps failing.
     */
    @Test
    public void testClockEvaluatorWithoutJobFails() {
        IEvaluatorContext noJob = new NoJobContext();
        HyracksDataException e = Assert.assertThrows(HyracksDataException.class,
                () -> evaluate(CurrentDateTimeImmediateDescriptor.FACTORY, noJob));
        Assert.assertEquals(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE.intValue(), e.getErrorCode());
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("job-start-timezone"));
    }

    private static IAObject evaluate(IFunctionDescriptorFactory factory, IEvaluatorContext ctx) throws Exception {
        IPointable result = new VoidPointable();
        factory.createFunctionDescriptor().createEvaluatorFactory(new IScalarEvaluatorFactory[0])
                .createScalarEvaluator(ctx).evaluate(null, result);
        byte[] bytes = result.getByteArray();
        int start = result.getStartOffset();
        ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[bytes[start]];
        switch (tag) {
            case DATETIME:
                return new ADateTime(LongPointable.getLong(bytes, start + 1));
            case DATE:
                return new ADate(IntegerPointable.getInteger(bytes, start + 1));
            default:
                throw new IllegalStateException("unexpected clock type " + tag);
        }
    }

    private static long readerInstant(String rendered) throws CompilationException {
        return IcebergSnapshotUtils.validateAndGetSnapshot(Map.of(ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY, rendered))
                .orElseThrow();
    }

    private static String snapshotId(IAObject value) throws CompilationException {
        return TimeTravelUtil.stringify(value, TimeTravel.Type.SNAPSHOT_ID, null);
    }

    private static String timestamp(IAObject value) throws CompilationException {
        return TimeTravelUtil.stringify(value, TimeTravel.Type.SNAPSHOT_TIMESTAMP, null);
    }

    private static void assertRejected(TimeTravel.Type type, IAObject value) {
        try {
            String resolved = TimeTravelUtil.stringify(value, type, null);
            Assert.fail("expected " + value.getType().getTypeTag() + " to be rejected, got " + resolved);
        } catch (CompilationException e) {
            Assert.assertEquals(ErrorCode.INVALID_TIME_TRAVEL_VALUE.intValue(), e.getErrorCode());
        }
    }

    private static class NoJobContext implements IEvaluatorContext {
        @Override
        public IServiceContext getServiceContext() {
            return null;
        }

        @Override
        public IHyracksTaskContext getTaskContext() {
            return null;
        }

        @Override
        public IWarningCollector getWarningCollector() {
            return null;
        }
    }
}
