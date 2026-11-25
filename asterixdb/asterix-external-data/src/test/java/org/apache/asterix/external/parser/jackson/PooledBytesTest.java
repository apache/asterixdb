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
package org.apache.asterix.external.parser.jackson;

import java.lang.ref.Reference;

import org.junit.Assert;
import org.junit.Test;

public class PooledBytesTest {

    @Test
    public void poolReportsTheSizeItLastPublished() {
        ObjectPool<byte[], Void> pool = new ObjectPool<>(null, null, array -> array.length);
        pool.recycle(new byte[100]);
        pool.recycle(new byte[50]);
        Assert.assertEquals(0, pool.getPooledBytes());
        pool.publishPooledBytes();
        Assert.assertEquals(150, pool.getPooledBytes());
        // a borrowed object stays in the published size until its owner publishes again
        Assert.assertEquals(50, pool.getInstance().length);
        Assert.assertEquals(150, pool.getPooledBytes());
        pool.recycle(new byte[70]);
        pool.publishPooledBytes();
        Assert.assertEquals(170, pool.getPooledBytes());
        Assert.assertEquals(70, pool.getInstance().length);
        Assert.assertEquals(100, pool.getInstance().length);
        Assert.assertNull(pool.getInstance());
        pool.publishPooledBytes();
        Assert.assertEquals(0, pool.getPooledBytes());
    }

    @Test
    public void softPoolStopsCountingWhatTheGcReclaims() {
        SoftObjectPool<byte[], Void> pool = new SoftObjectPool<>(unused -> new byte[8], null, array -> array.length);
        pool.recycle(new byte[100]);
        pool.recycle(new byte[50]);
        pool.publishPooledBytes();
        reclaim(pool, 100);
        // without the owner publishing again
        Assert.assertEquals(50, pool.getPooledBytes());
        Assert.assertEquals(50, pool.getInstance().length);
        // the cleared entry yields a new instance and is not subtracted a second time
        Assert.assertEquals(8, pool.getInstance().length);
        pool.publishPooledBytes();
        Assert.assertEquals(0, pool.getPooledBytes());
    }

    @Test
    public void softPoolNeverReportsLessThanNothing() {
        SoftObjectPool<byte[], Void> pool = new SoftObjectPool<>(null, null, array -> array.length);
        pool.recycle(new byte[100]);
        reclaim(pool, 100);
        Assert.assertEquals(0, pool.getPooledBytes());
        pool.recycle(new byte[30]);
        pool.publishPooledBytes();
        Assert.assertEquals(30, pool.getPooledBytes());
    }

    @Test
    public void unmeasuredSoftPoolReportsNothing() {
        SoftObjectPool<byte[], Void> pool = new SoftObjectPool<>();
        pool.recycle(new byte[100]);
        pool.publishPooledBytes();
        reclaim(pool, 100);
        Assert.assertEquals(0, pool.getPooledBytes());
        Assert.assertNull(pool.getInstance());
    }

    /** Does to the pooled arrays of {@code length} what the GC does to a softly reachable referent it reclaims. */
    private static void reclaim(SoftObjectPool<byte[], Void> pool, int length) {
        for (Reference<byte[]> ref : pool.pooledEntries()) {
            byte[] array = ref.get();
            if (array != null && array.length == length) {
                ref.enqueue();
            }
        }
    }
}
