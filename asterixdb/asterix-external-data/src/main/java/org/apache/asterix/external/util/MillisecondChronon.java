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

import java.util.concurrent.TimeUnit;

/**
 * The single point at which a sub-millisecond external temporal value is narrowed to the millisecond chronon
 * {@code ADateTime} and {@code ATime} hold.
 * <p>
 * This exists because two modules have to agree on it. A reader narrows the stored value here; a filter builder
 * pushing a predicate into the external source must widen that predicate to the whole millisecond this produces, or
 * the source prunes a file, row group or delete file holding a row the engine would have matched. Nothing in the
 * type system ties those two together, so the contract is stated once, here, rather than repeated at each call site.
 * <p>
 * <b>The narrowing is a floor</b>, so every value maps to the millisecond that contains it, before the epoch as well
 * as after it: {@code -500us} is {@code 1969-12-31T23:59:59.999}, not the epoch. {@link TimeUnit#toMillis} truncates
 * toward zero instead, which moves every pre-1970 value with a sub-millisecond part one millisecond later. Because
 * the mapping is uniform, the set of stored values narrowing to a literal {@code L} is always
 * {@code [L, L + MICROS_PER_MILLI)} in microseconds, which is the window the widening side pushes.
 * {@code IcebergTemporalPredicateWideningTest} and {@code ParquetFilterConverterTest} pin that agreement.
 * <p>
 * It must truncate rather than round: rounding would shift every window by half a millisecond.
 */
public final class MillisecondChronon {

    /** The width of the chronon, in microseconds — the span of stored values that narrow to one millisecond. */
    public static final long MICROS_PER_MILLI = TimeUnit.MILLISECONDS.toMicros(1);

    private MillisecondChronon() {
    }

    /**
     * Narrows a finer-grained temporal value to whole milliseconds, rounding toward negative infinity.
     *
     * @param value a temporal value — an epoch offset or a time of day
     * @param unit  the unit {@code value} is expressed in; finer than milliseconds, or this is a plain conversion
     * @return the millisecond containing {@code value}
     */
    public static long narrow(long value, TimeUnit unit) {
        long perMilli = unit.convert(1, TimeUnit.MILLISECONDS);
        return perMilli > 1 ? Math.floorDiv(value, perMilli) : unit.toMillis(value);
    }
}
