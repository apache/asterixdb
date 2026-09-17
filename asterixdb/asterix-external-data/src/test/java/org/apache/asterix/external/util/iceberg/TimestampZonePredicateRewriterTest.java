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

import java.time.ZoneId;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TimestampZonePredicateRewriterTest {

    private static final Schema SCHEMA =
            new Schema(Types.NestedField.optional(1, "tsz", Types.TimestampType.withZone()));
    private static final ZoneId ZONE = ZoneId.of("America/Los_Angeles");

    /**
     * A datetime literal too large for micros saturates to {@code Long.MAX_VALUE}, and shifting it back from a
     * negative offset overflows. Wrapped, {@code tsz < MAX} would become {@code tsz < <huge negative>} and prune
     * every file.
     */
    @Test
    public void saturatedLiteralIsGivenUp() {
        Expression rewritten = TimestampZonePredicateRewriter.rewrite(Expressions.lessThan("tsz", Long.MAX_VALUE),
                SCHEMA, ZONE, false);
        Assert.assertEquals(Expressions.alwaysTrue().toString(), rewritten.toString());
    }

    @Test
    public void ordinaryLiteralIsShiftedByTheOffset() {
        // 2026-03-15T02:00:00.123456 displayed under PDT (UTC-7) is 09:00:00.123456 stored.
        Expression rewritten = TimestampZonePredicateRewriter.rewrite(Expressions.lessThan("tsz", 1773540000123456L),
                SCHEMA, ZONE, false);
        Assert.assertEquals(Expressions.lessThan("tsz", 1773565200123456L).toString(), rewritten.toString());
    }

    /** timestamp-to-long emits the stored value unshifted, so there is no frame to convert the literal out of. */
    @Test
    public void timestampAsLongLeavesTheLiteralAlone() {
        Expression pushed = Expressions.lessThan("tsz", 1773565200123456L);
        Expression rewritten = TimestampZonePredicateRewriter.rewrite(pushed, SCHEMA, ZONE, true);
        Assert.assertEquals(pushed.toString(), rewritten.toString());
    }
}
