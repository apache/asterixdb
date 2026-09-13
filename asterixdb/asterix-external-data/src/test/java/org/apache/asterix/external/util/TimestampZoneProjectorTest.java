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
package org.apache.asterix.external.util;

import static org.apache.asterix.external.util.TimestampZoneProjector.TimestampUnit.MICROS;
import static org.apache.asterix.external.util.TimestampZoneProjector.TimestampUnit.MILLIS;
import static org.apache.asterix.external.util.TimestampZoneProjector.TimestampUnit.NANOS;
import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Test;

/**
 * The offset applied to a UTC-adjusted timestamp has to come from the zone's rules at that instant, not from
 * the zone's standard offset. These cases are what separates the two; a fixed-offset implementation passes
 * every winter case here and fails every summer one.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Daylight-saving coverage for the shared projector: summer vs winter, values spanning a "
        + "transition through one instance, transition boundaries, and out-of-order values, none of "
        + "which the existing avro/parquet fixtures reach because they use winter timestamps only")
public class TimestampZoneProjectorTest {

    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final long JAN = Instant.parse("2024-01-15T12:00:00Z").toEpochMilli();
    private static final long JUL = Instant.parse("2024-07-15T12:00:00Z").toEpochMilli();

    /**
     * The defect itself. Standard offset is -5h all year; the July value is really -4h. A fixed-offset
     * implementation returns -5h here and is wrong by an hour for half the year.
     */
    @Test
    public void testDaylightSavingIsApplied() throws HyracksDataException {
        TimestampZoneProjector p = new TimestampZoneProjector(NEW_YORK);
        assertOffset("winter", Duration.ofHours(-5), JAN, p.projectEpochValue(JAN, MILLIS));
        assertOffset("summer", Duration.ofHours(-4), JUL, p.projectEpochValue(JUL, MILLIS));
    }

    /** The standard offset is what the old implementation used; pin that the summer value is NOT that. */
    @Test
    public void testSummerDiffersFromTheStandardOffset() throws HyracksDataException {
        long standard = TimeZone.getTimeZone("America/New_York").getRawOffset();
        long projected = new TimestampZoneProjector(NEW_YORK).projectEpochValue(JUL, MILLIS) - JUL;
        assertEquals("summer must not use the standard offset", Duration.ofHours(1).toMillis(), projected - standard);
    }

    /**
     * One collection spanning a transition, through a single projector instance -- the case the offset cache
     * exists for, and the one it could get wrong by holding a stale window.
     */
    @Test
    public void testOneInstanceSpanningATransition() throws HyracksDataException {
        TimestampZoneProjector p = new TimestampZoneProjector(NEW_YORK);
        assertOffset("winter", Duration.ofHours(-5), JAN, p.projectEpochValue(JAN, MILLIS));
        assertOffset("summer", Duration.ofHours(-4), JUL, p.projectEpochValue(JUL, MILLIS));
        assertOffset("winter again", Duration.ofHours(-5), JAN, p.projectEpochValue(JAN, MILLIS));
    }

    /** Rows do not arrive sorted -- a parquet row group can hand them over in any order. */
    @Test
    public void testOutOfOrderValues() throws HyracksDataException {
        TimestampZoneProjector p = new TimestampZoneProjector(NEW_YORK);
        long[] values = { JUL, JAN, JUL, JAN, JUL };
        Duration[] expected = { Duration.ofHours(-4), Duration.ofHours(-5), Duration.ofHours(-4), Duration.ofHours(-5),
                Duration.ofHours(-4) };
        for (int i = 0; i < values.length; i++) {
            assertOffset("value " + i, expected[i], values[i], p.projectEpochValue(values[i], MILLIS));
        }
    }

    /** Either side of the exact instant the clocks change. */
    @Test
    public void testTransitionBoundary() throws HyracksDataException {
        long transition =
                NEW_YORK.getRules().nextTransition(Instant.parse("2024-03-01T00:00:00Z")).getInstant().toEpochMilli();
        TimestampZoneProjector p = new TimestampZoneProjector(NEW_YORK);
        long before = transition - 1;
        assertOffset("just before", Duration.ofHours(-5), before, p.projectEpochValue(before, MILLIS));
        assertOffset("at the transition", Duration.ofHours(-4), transition, p.projectEpochValue(transition, MILLIS));
    }

    /** A zone with no transitions takes the fast path and must still be correct in both seasons. */
    @Test
    public void testFixedOffsetZone() throws HyracksDataException {
        TimestampZoneProjector p = new TimestampZoneProjector(ZoneId.of("GMT+05:30"));
        assertOffset("winter", Duration.ofMinutes(330), JAN, p.projectEpochValue(JAN, MILLIS));
        assertOffset("summer", Duration.ofMinutes(330), JUL, p.projectEpochValue(JUL, MILLIS));
    }

    /** No time zone configured: every value passes through untouched, in every unit. */
    @Test
    public void testUnconfiguredIsPassThrough() throws HyracksDataException {
        TimestampZoneProjector p = new TimestampZoneProjector(null);
        assertEquals(false, p.isEnabled());
        assertEquals(JUL, p.projectEpochValue(JUL, MILLIS));
        assertEquals(JUL * 1000L, p.projectEpochValue(JUL * 1000L, MICROS));
        assertEquals(JUL * 1_000_000L, p.projectEpochValue(JUL * 1_000_000L, NANOS));
    }

    /** The offset is applied in whatever unit the value arrives in -- iceberg reads micros and nanos. */
    @Test
    public void testUnitsAgree() throws HyracksDataException {
        TimestampZoneProjector p = new TimestampZoneProjector(NEW_YORK);
        long shiftMillis = p.projectEpochValue(JUL, MILLIS) - JUL;
        long micros = TimeUnit.MILLISECONDS.toMicros(JUL);
        long nanos = TimeUnit.MILLISECONDS.toNanos(JUL);
        assertEquals(TimeUnit.MILLISECONDS.toMicros(shiftMillis), p.projectEpochValue(micros, MICROS) - micros);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(shiftMillis), p.projectEpochValue(nanos, NANOS) - nanos);
    }

    private static void assertOffset(String what, Duration expected, long original, long projected) {
        assertEquals(what, expected, Duration.ofMillis(projected - original));
    }
}
