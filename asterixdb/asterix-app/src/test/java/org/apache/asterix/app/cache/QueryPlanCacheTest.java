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
package org.apache.asterix.app.cache;

import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.cache.IQueryPlanCacheValue;
import org.apache.asterix.common.cache.QueryPlanCacheKey;
import org.apache.asterix.common.cache.QueryPlanCacheValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link QueryPlanCache} capacity handling.
 */
public class QueryPlanCacheTest {

    @Test
    public void getReturnsValueForEqualKey() {
        QueryPlanCache cache = new QueryPlanCache(1);
        IQueryPlanCacheValue value = value();
        cache.put(key("q1"), value);
        Assert.assertSame(value, cache.get(key("q1")));
        Assert.assertNull(cache.get(key("q2")));
    }

    @Test
    public void evictsLeastRecentlyUsedWhenFull() {
        QueryPlanCache cache = new QueryPlanCache(2);
        cache.put(key("q1"), value());
        cache.put(key("q2"), value());
        // touch q1 so q2 becomes the eviction candidate
        cache.get(key("q1"));
        cache.put(key("q3"), value());
        Assert.assertNull(cache.get(key("q2")));
        Assert.assertNotNull(cache.get(key("q1")));
        Assert.assertNotNull(cache.get(key("q3")));
    }

    @Test
    public void zeroCapacityDisablesCache() {
        QueryPlanCache cache = new QueryPlanCache(0);
        cache.put(key("q1"), value());
        Assert.assertNull(cache.get(key("q1")));
        Assert.assertEquals(0, cache.clear());
    }

    @Test
    public void clearReturnsEntryCountAndEmptiesCache() {
        QueryPlanCache cache = new QueryPlanCache(2);
        cache.put(key("q1"), value());
        cache.put(key("q2"), value());
        Assert.assertEquals(2, cache.clear());
        Assert.assertNull(cache.get(key("q1")));
        Assert.assertEquals(0, cache.clear());
    }

    @Test
    public void resizeShrinkRetainsMostRecentlyUsedEntries() {
        QueryPlanCache cache = new QueryPlanCache(3);
        cache.put(key("q1"), value());
        cache.put(key("q2"), value());
        cache.put(key("q3"), value());
        cache.resize(2);
        Assert.assertNull(cache.get(key("q1")));
        Assert.assertNotNull(cache.get(key("q2")));
        Assert.assertNotNull(cache.get(key("q3")));
    }

    @Test
    public void resizeGrowRaisesCapacity() {
        QueryPlanCache cache = new QueryPlanCache(2);
        cache.put(key("q1"), value());
        cache.put(key("q2"), value());
        cache.resize(3);
        cache.put(key("q3"), value());
        Assert.assertNotNull(cache.get(key("q1")));
        Assert.assertNotNull(cache.get(key("q2")));
        Assert.assertNotNull(cache.get(key("q3")));
    }

    @Test
    public void resizeToZeroDisablesAndDropsEntries() {
        QueryPlanCache cache = new QueryPlanCache(1);
        cache.put(key("q1"), value());
        cache.resize(0);
        Assert.assertNull(cache.get(key("q1")));
        cache.resize(1);
        Assert.assertNull(cache.get(key("q1")));
        cache.put(key("q1"), value());
        Assert.assertNotNull(cache.get(key("q1")));
    }

    private static QueryPlanCacheKey key(String queryString) {
        return new QueryPlanCacheKey(queryString, true, Map.of(), 0, 0, null, null);
    }

    private static QueryPlanCacheValue value() {
        return new QueryPlanCacheValue(null, Set.of(), null, null);
    }
}
