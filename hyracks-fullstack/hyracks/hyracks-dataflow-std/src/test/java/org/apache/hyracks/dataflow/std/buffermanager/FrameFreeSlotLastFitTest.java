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

package org.apache.hyracks.dataflow.std.buffermanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class FrameFreeSlotLastFitTest {

    FrameFreeSlotLastFit zeroPolicy;
    FrameFreeSlotLastFit unifiedPolicy;
    FrameFreeSlotLastFit ascPolicy;
    FrameFreeSlotLastFit dscPolicy;

    static final int size = 10;
    static final int medium = 5;

    @Before
    public void setUp() throws Exception {
        zeroPolicy = new FrameFreeSlotLastFit(0);
        unifiedPolicy = new FrameFreeSlotLastFit(size);
        ascPolicy = new FrameFreeSlotLastFit(size);
        dscPolicy = new FrameFreeSlotLastFit(size);
    }

    @Test
    public void testPushAndPop() throws Exception {
        for (int i = 0; i < size; i++) {
            unifiedPolicy.pushNewFrame(i, medium);
        }
        for (int i = 0; i < size; i++) {
            assertTrue(unifiedPolicy.popBestFit(medium) == size - i - 1);
        }
        assertTrue(unifiedPolicy.popBestFit(0) == -1);

        for (int i = 0; i < size / 2; i++) {
            ascPolicy.pushNewFrame(i, i);
            assertEquals(ascPolicy.popBestFit(medium), -1);
            dscPolicy.pushNewFrame(i, size - i - 1);
            assertEquals(dscPolicy.popBestFit(medium), i);
        }

        for (int i = size / 2; i < size; i++) {
            ascPolicy.pushNewFrame(i, i);
            assertEquals(ascPolicy.popBestFit(medium), i);
            dscPolicy.pushNewFrame(i, size - i - 1);
            assertEquals(dscPolicy.popBestFit(medium), -1);
        }

        ascPolicy.reset();
        for (int i = 0; i < size; i++) {
            ascPolicy.pushNewFrame(size - i, size - i);
        }

        for (int i = 0; i < size; i++) {
            assertEquals(size - i, ascPolicy.popBestFit(size - i));
        }
    }

    @Test
    public void testReset() throws Exception {
        testPushAndPop();

        zeroPolicy.reset();
        unifiedPolicy.reset();
        ascPolicy.reset();
        dscPolicy.reset();
        testPushAndPop();
    }
}
