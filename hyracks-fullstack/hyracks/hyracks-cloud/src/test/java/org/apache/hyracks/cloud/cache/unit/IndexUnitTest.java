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
package org.apache.hyracks.cloud.cache.unit;

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.storage.common.LocalResource;
import org.junit.Assert;
import org.junit.Test;

/**
 * An index is inactive, and its local files may be evicted, only once it has gone unused for the threshold. It is in
 * use for as long as a job has it open, however long ago that was, and closing it is a use too.
 */
public class IndexUnitTest {
    private static final long THRESHOLD = TimeUnit.HOURS.toNanos(6);

    @Test
    public void anOpenIndexIsNeverInactive() {
        AbstractIndexUnit unit = newUnit();
        unit.open();
        unit.open();
        unit.close();
        Assert.assertTrue("another job still has it open", unit.isOpen());
        Assert.assertFalse(unit.isInactive(System.nanoTime() + THRESHOLD, THRESHOLD));
    }

    @Test
    public void closingAnIndexIsAUse() throws InterruptedException {
        AbstractIndexUnit unit = newUnit();
        unit.open();
        long opened = unit.getLastAccessTime();
        Thread.sleep(2);
        unit.close();
        long closed = unit.getLastAccessTime();
        Assert.assertFalse(unit.isOpen());
        Assert.assertTrue(closed > opened);
        Assert.assertFalse("opened long ago, but closed just now", unit.isInactive(opened + THRESHOLD, THRESHOLD));
        Assert.assertFalse(unit.isInactive(closed + THRESHOLD - 1, THRESHOLD));
        Assert.assertTrue(unit.isInactive(closed + THRESHOLD, THRESHOLD));
    }

    @Test
    public void aRegisteredIndexNeverOpenedIsInactiveOnlyAfterTheThreshold() {
        long before = System.nanoTime();
        AbstractIndexUnit unit = newUnit();
        Assert.assertFalse(unit.isInactive(System.nanoTime(), THRESHOLD));
        Assert.assertTrue(unit.isInactive(before + THRESHOLD + TimeUnit.MINUTES.toNanos(1), THRESHOLD));
    }

    // the bookkeeping under test is AbstractIndexUnit's own, whatever its subclasses need to be built
    private static AbstractIndexUnit newUnit() {
        return new AbstractIndexUnit(new LocalResource(1L, 0, true, null)) {
            @Override
            public void drop() {
                // NoOp
            }

            @Override
            protected boolean isSweepable() {
                return false;
            }
        };
    }
}
