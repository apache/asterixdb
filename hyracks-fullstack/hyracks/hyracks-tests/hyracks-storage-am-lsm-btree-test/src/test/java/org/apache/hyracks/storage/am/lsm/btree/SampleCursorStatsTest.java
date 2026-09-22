/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.lsm.btree;

import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.SampleCursorStats;
import org.junit.Assert;
import org.junit.Test;

public class SampleCursorStatsTest {

    @Test
    public void livenessShareIsComputedFromThePhaseTimings() {
        SampleCursorStats stats = new SampleCursorStats();
        stats.livenessNanos = 800;
        stats.pkSeekNanos = 100;
        stats.phase2Nanos = 100;
        Assert.assertEquals(80.0, stats.livenessSharePct(), 0.0001);
    }

    @Test
    public void livenessShareIsZeroWhenNothingWasMeasured() {
        Assert.assertEquals(0.0, new SampleCursorStats().livenessSharePct(), 0.0001);
    }

    @Test
    public void resetClearsEveryCounter() {
        SampleCursorStats stats = new SampleCursorStats();
        stats.livenessNanos = 5;
        stats.livenessCalls = 5;
        stats.livenessKeys = 5;
        stats.pkSeekNanos = 5;
        stats.pkSeekCalls = 5;
        stats.phase2Nanos = 5;
        stats.phase2PagePins = 5;
        stats.pagesAccepted = 5;
        stats.pagesRejected = 5;
        stats.attempts = 5;
        stats.reset();
        Assert.assertEquals(0, stats.livenessNanos);
        Assert.assertEquals(0, stats.livenessCalls);
        Assert.assertEquals(0, stats.livenessKeys);
        Assert.assertEquals(0, stats.pkSeekNanos);
        Assert.assertEquals(0, stats.pkSeekCalls);
        Assert.assertEquals(0, stats.phase2Nanos);
        Assert.assertEquals(0, stats.phase2PagePins);
        Assert.assertEquals(0, stats.pagesAccepted);
        Assert.assertEquals(0, stats.pagesRejected);
        Assert.assertEquals(0, stats.attempts);
        Assert.assertEquals(0.0, stats.livenessSharePct(), 0.0001);
    }
}
