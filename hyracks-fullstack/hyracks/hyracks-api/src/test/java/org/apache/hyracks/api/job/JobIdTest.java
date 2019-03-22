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
package org.apache.hyracks.api.job;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.control.CcIdPartitionedLongFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobIdTest {

    private static Field idField;

    @BeforeClass
    public static void setup() throws NoSuchFieldException {
        idField = CcIdPartitionedLongFactory.class.getDeclaredField("id");
        idField.setAccessible(true);
    }

    @Test
    public void testCcIds() {
        JobIdFactory factory = new JobIdFactory(CcId.valueOf(0));
        for (int i = 0; i < 1000; i++) {
            final JobId jobId = factory.create();
            Assert.assertEquals(0, jobId.getCcId().shortValue());
            Assert.assertEquals(i, jobId.getIdOnly());
        }
    }

    @Test
    public void testNegativeCcId() {
        JobIdFactory factory = new JobIdFactory(CcId.valueOf(0xFFFF));
        for (int i = 0; i < 1000; i++) {
            final JobId jobId = factory.create();
            Assert.assertEquals((short) 0xFFFF, jobId.getCcId().shortValue());
            Assert.assertEquals(i, jobId.getIdOnly());
            Assert.assertTrue("JID not negative", jobId.getId() < 0);
            Assert.assertEquals(0xFFFF000000000000L + i, jobId.getId());
        }
    }

    @Test
    public void testOverflow() throws IllegalAccessException {
        testOverflow(0);
        testOverflow(0xFFFF);
        testOverflow(Short.MAX_VALUE);
    }

    private void testOverflow(int id) throws IllegalAccessException {
        CcId ccId = CcId.valueOf(id);
        long expected = (long) id << 48;
        JobIdFactory factory = new JobIdFactory(ccId);
        AtomicLong theId = (AtomicLong) idField.get(factory);
        Assert.assertEquals(expected, theId.get());
        theId.set((((long) 1 << 48) - 1) | expected);
        JobId jobId = factory.create();
        Assert.assertEquals(ccId, jobId.getCcId());
        Assert.assertEquals(CcIdPartitionedLongFactory.MAX_ID, jobId.getIdOnly());
        jobId = factory.create();
        Assert.assertEquals(ccId, jobId.getCcId());
        Assert.assertEquals(0, jobId.getIdOnly());
    }

    @Test
    public void testComparability() throws IllegalAccessException {
        JobIdFactory factory = new JobIdFactory(CcId.valueOf(0));
        compareLoop(factory, false);
        factory = new JobIdFactory(CcId.valueOf(0xFFFF));
        compareLoop(factory, false);
        AtomicLong theId = (AtomicLong) idField.get(factory);
        theId.set(0xFFFFFFFFFFFFFFF0L);
        compareLoop(factory, true);
    }

    private void compareLoop(JobIdFactory factory, boolean overflow) {
        Set<Boolean> overflowed = new HashSet<>(Collections.singleton(false));
        JobId prevMax = null;
        for (int i = 0; i < 1000; i++) {
            final JobId jobId = factory.create();
            Assert.assertTrue("max == last", factory.maxJobId().compareTo(jobId) == 0);
            if (i > 0) {
                Assert.assertTrue("last > previous max", prevMax.compareTo(jobId) < 0 || overflowed.add(overflow));
            }
            prevMax = factory.maxJobId();
        }
    }

    @Test
    public void testTooLarge() {
        try {
            CcId.valueOf(0x10000);
            Assert.assertTrue("expected exception", false);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testParse() throws HyracksDataException {
        for (int ccId : Arrays.asList(0xFFFF, 0, (int) Short.MAX_VALUE)) {
            JobIdFactory factory = new JobIdFactory(CcId.valueOf(ccId));
            for (int i = 0; i < 1000; i++) {
                final JobId jobId = factory.create();
                Assert.assertEquals(jobId.getId(), JobId.parse(jobId.toString()).getId());
                Assert.assertEquals(jobId, JobId.parse(jobId.toString()));
                Assert.assertFalse(jobId.toString(), jobId.toString().contains("-"));
                System.err.println(jobId.toString());
            }
        }
    }
}
