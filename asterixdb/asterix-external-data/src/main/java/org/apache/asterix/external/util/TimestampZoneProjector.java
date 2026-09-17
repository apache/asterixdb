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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Shifts a UTC-adjusted timestamp into the time zone configured on an external collection.
 * <p>
 * The offset is resolved <b>per value</b>, from the zone's transition rules, so a collection whose rows span a
 * daylight-saving boundary gets the correct offset on each side of it. A single fixed offset cannot do this:
 * {@code TimeZone.getRawOffset()} reports a zone's <em>standard</em> offset, which is simply wrong for every
 * value that falls inside a daylight-saving period.
 * <p>
 * Only values that are adjusted to UTC may be projected. A wall-clock value ({@code timestampntz},
 * avro's {@code local-timestamp-*}, delta's {@code TimestampNTZType}, parquet's {@code isAdjustedToUTC=false})
 * carries no zone and must be passed through untouched -- shifting it invents an offset the source never had.
 * <p>
 * <b>Not thread-safe by design.</b> The transition cache below is mutable and unsynchronized, so an instance
 * belongs to exactly one parser, and parsers are per-task/per-thread.
 */
public final class TimestampZoneProjector {

    /** Unit of the epoch value being projected. The offset is applied in the same unit it arrives in. */
    public enum TimestampUnit {
        MILLIS(1_000L),
        MICROS(1_000_000L),
        NANOS(1_000_000_000L);

        private final long perSecond;

        TimestampUnit(long perSecond) {
            this.perSecond = perSecond;
        }
    }

    private final boolean enabled;
    private final ZoneRules zoneRules;
    private final boolean fixedOffsetZone;
    private final int fixedOffsetSeconds;

    // Cached offset, valid for [validFrom, validUntil) -- the span between two transitions of this zone.
    private long validFromEpochSecond = Long.MIN_VALUE;
    private long validUntilEpochSecond = Long.MIN_VALUE;
    private int cachedOffsetSeconds;

    /**
     * @param zoneId the configured zone, or {@code null} when no time zone was configured, in which case every
     *               value is passed through unchanged.
     */
    public TimestampZoneProjector(ZoneId zoneId) {
        enabled = zoneId != null;
        if (enabled) {
            zoneRules = zoneId.getRules();
            fixedOffsetZone = zoneRules.isFixedOffset();
            fixedOffsetSeconds = fixedOffsetZone ? zoneRules.getOffset(Instant.EPOCH).getTotalSeconds() : 0;
        } else {
            zoneRules = null;
            fixedOffsetZone = true;
            fixedOffsetSeconds = 0;
        }
    }

    /**
     * The single rule for which timestamps the configured zone shifts on read. Every reader applies it, and so does
     * predicate pushdown, which has to convert a literal back only for the values the reader shifted — two copies
     * of this rule drifting apart is how a pushed predicate ends up in a different frame from the value it tests.
     * <p>
     * A wall-clock value has no zone to shift out of, and {@code timestamp-to-long} emits the epoch value itself,
     * an absolute instant the zone is a rendering choice for.
     *
     * @param utcAdjusted     whether the source type is adjusted to UTC
     * @param timestampAsLong whether the value is emitted as its epoch long rather than as a datetime
     * @return whether {@link #projectEpochValue} applies to the value
     */
    public static boolean isShiftedOnRead(boolean utcAdjusted, boolean timestampAsLong) {
        return utcAdjusted && !timestampAsLong;
    }

    /** @return whether a time zone was configured; when false {@link #projectEpochValue} is a no-op. */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Shifts a UTC-adjusted epoch value by the offset in effect at that instant.
     *
     * @param epochValue epoch value, in {@code unit}, of a value that is adjusted to UTC
     * @param unit       unit of {@code epochValue}; the result is returned in the same unit
     * @return the shifted value, or {@code epochValue} unchanged when no time zone was configured
     */
    public long projectEpochValue(long epochValue, TimestampUnit unit) throws HyracksDataException {
        if (!enabled) {
            return epochValue;
        }
        int offsetSeconds = getOffsetSeconds(epochValue, unit);
        try {
            return Math.addExact(epochValue, offsetSeconds * unit.perSecond);
        } catch (ArithmeticException ex) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                    "timestamp value overflows long representation after applying timezone configuration");
        }
    }

    private int getOffsetSeconds(long epochValue, TimestampUnit unit) {
        if (fixedOffsetZone) {
            return fixedOffsetSeconds;
        }
        long epochSecond = Math.floorDiv(epochValue, unit.perSecond);
        if (epochSecond >= validFromEpochSecond && epochSecond < validUntilEpochSecond) {
            return cachedOffsetSeconds;
        }
        return refreshOffsetCache(epochSecond);
    }

    private int refreshOffsetCache(long epochSecond) {
        Instant instant = Instant.ofEpochSecond(epochSecond);
        ZoneOffset offset = zoneRules.getOffset(instant);
        // Use epochSecond + 1ns so that if the record falls exactly on a transition boundary,
        // previousTransition captures that transition as the start of the current offset period.
        ZoneOffsetTransition previous = zoneRules.previousTransition(Instant.ofEpochSecond(epochSecond, 1L));
        ZoneOffsetTransition next = zoneRules.nextTransition(instant);
        validFromEpochSecond = previous == null ? Long.MIN_VALUE : previous.getInstant().getEpochSecond();
        validUntilEpochSecond = next == null ? Long.MAX_VALUE : next.getInstant().getEpochSecond();
        cachedOffsetSeconds = offset.getTotalSeconds();
        return cachedOffsetSeconds;
    }
}
