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

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The single point at which a sub-millisecond external temporal value is narrowed to the millisecond chronon
 * {@code ADateTime} and {@code ATime} hold.
 * <p>
 * This exists because two modules have to agree on it. A reader narrows the stored value here; a filter builder
 * pushing a predicate into the external source must widen that predicate to the whole millisecond this produces, or
 * the source prunes a file, row group or delete file holding a row the engine would have matched. Nothing in the
 * type system ties those two together, so the contract is stated once, here, rather than repeated at each call site.
 * <p>
 * <b>The truncation is toward zero, not a floor</b>, because that is what {@link TimeUnit} does. Two consequences
 * the widening side depends on:
 * <ul>
 * <li>the epoch millisecond is <b>double width</b> — both {@code -999us} and {@code +999us} narrow to {@code 0};</li>
 * <li>below the epoch the set of values narrowing to a literal sits <b>above</b> it, the mirror of the positive
 * case, which flips which comparison operators are unsafe.</li>
 * </ul>
 * Changing this to {@link Math#floorDiv} would make the mapping uniform and is arguably the better behaviour, but it
 * is a change to query <em>results</em> for pre-1970 sub-millisecond data, and the widening's negative branch must
 * change with it. {@code IcebergTemporalPredicateWideningTest} pins the agreement and fails if only one side moves.
 * <p>
 * Values that cannot be negative — a time of day — are unaffected by the direction, since toward-zero and floor
 * agree there. What those still rely on is that this <em>truncates rather than rounds</em>: rounding would shift
 * every window by half a millisecond.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Extracted from the four truncation sites in IcebergParquetDataParser so the read path and the "
        + "pushdown widening derive from one documented function instead of four comments")
public final class MillisecondChronon {

    /** The width of the chronon, in microseconds — the span of stored values that narrow to one millisecond. */
    public static final long MICROS_PER_MILLI = TimeUnit.MILLISECONDS.toMicros(1);

    private MillisecondChronon() {
    }

    /**
     * Narrows a finer-grained temporal value to whole milliseconds, truncating toward zero.
     *
     * @param value a temporal value — an epoch offset or a time of day
     * @param unit  the unit {@code value} is expressed in; finer than milliseconds, or this is a no-op
     * @return {@code value} in whole milliseconds
     */
    public static long narrow(long value, TimeUnit unit) {
        return unit.toMillis(value);
    }
}
