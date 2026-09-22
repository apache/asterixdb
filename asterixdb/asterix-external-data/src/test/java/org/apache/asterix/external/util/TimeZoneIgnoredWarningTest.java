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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.input.record.reader.aws.iceberg.converter.IcebergConverterContext;
import org.apache.asterix.external.input.record.reader.stream.AvroConverterContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.junit.Test;

/**
 * A collection created before the DDL-time timezone check existed may hold an id the reader cannot resolve. It
 * keeps reading -- deliberately -- but must say so rather than silently applying no offset. These cases pin that
 * the warning fires exactly then, and never when no timezone was asked for.
 */
public class TimeZoneIgnoredWarningTest {

    /** Captures what a reader would report, so a test can assert on it without a cluster. */
    private static final class CapturingCollector implements IWarningCollector {
        private final List<Warning> warnings = new ArrayList<>();

        @Override
        public void warn(Warning warning) {
            warnings.add(warning);
        }

        @Override
        public boolean shouldWarn() {
            return true;
        }

        @Override
        public long getTotalWarningsCount() {
            return warnings.size();
        }
    }

    @Test
    public void testUnresolvableIdIsReported() {
        List<Warning> warnings = new ArrayList<>();
        assertNull("an unusable id must still resolve to unset",
                ExternalDataUtils.resolveTimeZoneOrWarn("dummy", warnings::add));
        assertEquals(1, warnings.size());
        Warning w = warnings.get(0);
        assertEquals(ErrorCode.TIME_ZONE_ID_IGNORED.intValue(), w.getCode());
        assertTrue("the warning must name the offending value, got: " + w.getMessage(),
                w.getMessage().contains("dummy"));
    }

    /** No timezone was asked for, so there is nothing to ignore and nothing to report. */
    @Test
    public void testUnsetIsNotReported() {
        List<Warning> warnings = new ArrayList<>();
        assertNull(ExternalDataUtils.resolveTimeZoneOrWarn(null, warnings::add));
        assertNull(ExternalDataUtils.resolveTimeZoneOrWarn("", warnings::add));
        assertEquals("unset must never warn", 0, warnings.size());
    }

    @Test
    public void testResolvableIdsAreNotReported() {
        List<Warning> warnings = new ArrayList<>();
        for (String id : new String[] { "Asia/Kolkata", "asia/kolkata", "PST", "pst", "GMT+05:30", "gmt+05:30",
                "+05:30", "Z" }) {
            assertNotNull(id + " should resolve", ExternalDataUtils.resolveTimeZoneOrWarn(id, warnings::add));
        }
        assertEquals("a resolvable id must never warn: " + warnings, 0, warnings.size());
    }

    /** The avro reader's warnings used to go to a list nothing ever drained; it now holds a real collector. */
    @Test
    public void testAvroContextReportsThroughItsCollector() {
        CapturingCollector collector = new CapturingCollector();
        new AvroConverterContext(config("dummy"), collector);
        assertEquals(1, collector.warnings.size());
        assertEquals(ErrorCode.TIME_ZONE_ID_IGNORED.intValue(), collector.warnings.get(0).getCode());

        CapturingCollector ok = new CapturingCollector();
        new AvroConverterContext(config("Asia/Kolkata"), ok);
        assertEquals(0, ok.warnings.size());
    }

    /** The iceberg reader had no warning plumbing at all before this. */
    @Test
    public void testIcebergContextReportsThroughItsCollector() {
        CapturingCollector collector = new CapturingCollector();
        new IcebergConverterContext(config("dummy"), collector);
        assertEquals(1, collector.warnings.size());
        assertEquals(ErrorCode.TIME_ZONE_ID_IGNORED.intValue(), collector.warnings.get(0).getCode());

        CapturingCollector ok = new CapturingCollector();
        new IcebergConverterContext(config("pst"), ok);
        assertEquals(0, ok.warnings.size());
    }

    private static Map<String, String> config(String timeZoneId) {
        Map<String, String> conf = new HashMap<>();
        conf.put(ExternalDataConstants.KEY_TIMEZONE, timeZoneId);
        return conf;
    }
}
