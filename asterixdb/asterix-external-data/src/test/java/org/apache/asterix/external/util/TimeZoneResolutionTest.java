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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.asterix.common.exceptions.CompilationException;
import org.junit.Test;

/**
 * Covers the timezone id accepted by a {@code WITH} clause and the offset the readers then apply to a
 * timestamp. The two used to be resolved by different parsers, so an id could be accepted at DDL time and
 * read as GMT.
 */
public class TimeZoneResolutionTest {

    /**
     * The load-bearing one: read-time resolution must not narrow. Every id that resolves to an offset today has
     * to keep that exact offset, or an existing collection silently starts reading its timestamps differently.
     */
    @Test
    public void testEveryAvailableIdKeepsItsOffset() {
        List<String> changed = new ArrayList<>();
        for (String id : TimeZone.getAvailableIDs()) {
            TimeZone resolved = ExternalDataUtils.resolveTimeZoneOrUnset(id);
            int actual = resolved == null ? 0 : resolved.getRawOffset();
            if (TimeZone.getTimeZone(id).getRawOffset() != actual) {
                changed.add(id);
            }
        }
        assertEquals("timezone ids whose offset changed: " + changed, 0, changed.size());
    }

    /**
     * The short ids are the reason the read path cannot simply be handed to {@link java.time.ZoneId#of}, which
     * rejects all 28 of them. They predate the change and have to keep working.
     */
    @Test
    public void testShortIdsStillResolve() {
        assertOffset("EST", -5);
        assertOffset("MST", -7);
        assertOffset("HST", -10);
        assertOffset("JST", 9);
    }

    /** Previously resolved to GMT, because the lookup was case-sensitive while the DDL check was not. */
    @Test
    public void testCaseInsensitive() {
        assertOffset("asia/kolkata", 5, 30);
        assertOffset("america/new_york", -5);
        assertOffset("AMERICA/NEW_YORK", -5);
        assertOffset("est", -5);
    }

    /** Accepted by the DDL check via ZoneId, previously read as GMT because TimeZone cannot parse them. */
    @Test
    public void testBareOffsetForms() {
        assertOffset("+05:30", 5, 30);
        assertOffset("UTC+05:30", 5, 30);
        assertOffset("GMT+05:30", 5, 30);
    }

    /**
     * Offset forms are case-insensitive like region ids, via an upper-cased retry of the fallback. Only the
     * fallback needs it: every region id is caught by the canonical lookup, which already ignores case.
     */
    @Test
    public void testOffsetFormsAreCaseInsensitive() {
        assertOffset("gmt+05:30", 5, 30);
        assertOffset("Gmt+5", 5);
        assertOffset("utc-8", -8);
        assertOffset("ut+1", 1);
        assertOffset("z", 0);
    }

    /**
     * Unset rather than an exception: a collection created before the DDL-time check existed may hold an
     * unusable id, and it has to keep reading with the offset it has always had.
     */
    @Test
    public void testUnusableIdIsUnsetAndNeverThrows() {
        assertNull(ExternalDataUtils.resolveTimeZoneOrUnset("dummy"));
        assertNull(ExternalDataUtils.resolveTimeZoneOrUnset("Not/AZone"));
        assertNull(ExternalDataUtils.resolveTimeZoneOrUnset(""));
        assertNull(ExternalDataUtils.resolveTimeZoneOrUnset(null));
    }

    /**
     * The invariant tying the two halves together: the DDL check rejects an id exactly when the read path
     * would have applied no offset to it. Neither side may accept what the other silently drops.
     */
    @Test
    public void testValidationAgreesWithReadPath() {
        List<String> disagreed = new ArrayList<>();
        String[] ids = { "EST", "asia/kolkata", "+05:30", "UTC+05:30", "GMT+05:30", "Asia/Kolkata", "UTC", "Z",
                "gmt+05:30", "utc-8", "z", "dummy", "Not/AZone", "GMT+99:99", "America/Nowhere", "america/nowhere" };
        for (String id : ids) {
            boolean accepted;
            try {
                ExternalDataUtils.resolveTimeZone(id);
                accepted = true;
            } catch (CompilationException e) {
                accepted = false;
            }
            if (accepted != (ExternalDataUtils.resolveTimeZoneOrUnset(id) != null)) {
                disagreed.add(id);
            }
        }
        assertEquals("ids the two resolvers disagree on: " + disagreed, 0, disagreed.size());
    }

    @Test
    public void testInvalidIdIsRejectedAtDdlTime() {
        try {
            ExternalDataUtils.resolveTimeZone("dummy");
            fail("expected an invalid timezone to be rejected");
        } catch (CompilationException e) {
            assertEquals(org.apache.asterix.common.exceptions.ErrorCode.INVALID_TIMEZONE.intValue(), e.getErrorCode());
        }
    }

    private static void assertOffset(String id, int hours) {
        assertOffset(id, hours, 0);
    }

    private static void assertOffset(String id, int hours, int minutes) {
        TimeZone resolved = ExternalDataUtils.resolveTimeZoneOrUnset(id);
        assertNotNull(id + " did not resolve", resolved);
        int expected =
                (int) java.util.concurrent.TimeUnit.MINUTES.toMillis(hours * 60L + (hours < 0 ? -minutes : minutes));
        assertEquals(id, expected, resolved.getRawOffset());
    }
}
