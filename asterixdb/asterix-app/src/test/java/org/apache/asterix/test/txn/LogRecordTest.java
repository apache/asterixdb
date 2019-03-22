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
package org.apache.asterix.test.txn;

import static org.apache.asterix.common.transactions.LogConstants.LOG_SOURCE_MAX;
import static org.apache.asterix.common.transactions.LogConstants.LOG_SOURCE_MIN;
import static org.apache.asterix.common.transactions.LogConstants.VERSION_MAX;
import static org.apache.asterix.common.transactions.LogConstants.VERSION_MIN;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class LogRecordTest {
    private static ByteBuffer buffer;

    @BeforeClass
    public static void setup() {
        buffer = ByteBuffer.allocate(100);
    }

    @Test
    @SuppressWarnings("squid:S3415")
    public void testVersionIdLogSourceRange() {
        Assert.assertEquals("min version", 0, VERSION_MIN);
        Assert.assertEquals("max version", 62, VERSION_MAX);
        Assert.assertEquals("min source", 0, LOG_SOURCE_MIN);
        Assert.assertEquals("max source", 3, LOG_SOURCE_MAX);
        IntStream.rangeClosed(LOG_SOURCE_MIN, LOG_SOURCE_MAX).forEach(
                s -> IntStream.rangeClosed(VERSION_MIN, VERSION_MAX).forEach(v -> testVersionSourceCombo(v, (byte) s)));
    }

    @Test
    public void testIllegalVersionIds() {
        try {
            testVersionSourceCombo(63, LogSource.LOCAL);
            Assert.fail("expected IllegalArgumentException on version overflow not found");
        } catch (IllegalArgumentException e) {
            // ignore - expected
        }
        try {
            testVersionSourceCombo(-1, LogSource.LOCAL);
            Assert.fail("expected IllegalArgumentException on version underflow not found");
        } catch (IllegalArgumentException e) {
            // ignore - expected
        }
    }

    @Test
    public void testIllegalLogSources() {
        LogRecord record = new LogRecord();
        try {
            record.setLogSource((byte) -1);
            Assert.fail("expected IllegalArgumentException on log source underflow not found");
        } catch (IllegalArgumentException e) {
            // ignore - expected
        }
        try {
            record.setLogSource((byte) 4);
            Assert.fail("expected IllegalArgumentException on log source overflow not found");
        } catch (IllegalArgumentException e) {
            // ignore - expected
        }
    }

    private void testVersionSourceCombo(int version, byte source) {
        buffer.clear();
        LogRecord record = new LogRecord();
        record.setLogType(LogType.FLUSH);
        record.setVersion(version);
        record.setLogSource(source);
        record.computeAndSetLogSize();
        Assert.assertEquals("input version", version, record.getVersion());
        Assert.assertEquals("input source", source, record.getLogSource());
        record.writeLogRecord(buffer);

        buffer.flip();
        LogRecord read = new LogRecord();
        read.readLogRecord(buffer);
        Assert.assertEquals("read version", version, read.getVersion());
        Assert.assertEquals("read source", source, read.getLogSource());
    }
}
