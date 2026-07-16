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

import java.util.Collections;
import java.util.Map;

import org.apache.asterix.common.cache.IQueryPlanCache;
import org.apache.asterix.common.cache.IQueryPlanCacheKey;
import org.apache.asterix.common.cache.IQueryPlanCacheValue;
import org.apache.commons.collections4.map.LRUMap;

/**
 * Bounded query plan cache backed by an LRU map wrapped for thread safety. A capacity of zero disables the cache.
 */
public class QueryPlanCache implements IQueryPlanCache {

    // null when disabled; resize() swaps in a new map, hence volatile
    private volatile Map<IQueryPlanCacheKey, IQueryPlanCacheValue> map;

    // TODO: Add data structures for fine-grained cache invalidation

    public QueryPlanCache(int capacity) {
        map = newBoundedMap(capacity);
    }

    @Override
    public IQueryPlanCacheValue get(IQueryPlanCacheKey key) {
        Map<IQueryPlanCacheKey, IQueryPlanCacheValue> current = map;
        return current != null ? current.get(key) : null;
    }

    @Override
    public void put(IQueryPlanCacheKey key, IQueryPlanCacheValue value) {
        Map<IQueryPlanCacheKey, IQueryPlanCacheValue> current = map;
        if (current != null) {
            current.put(key, value);
        }
    }

    @Override
    public int clear() {
        Map<IQueryPlanCacheKey, IQueryPlanCacheValue> current = map;
        if (current == null) {
            return 0;
        }
        synchronized (current) {
            int cleared = current.size();
            current.clear();
            return cleared;
        }
    }

    @Override
    public synchronized void resize(int capacity) {
        Map<IQueryPlanCacheKey, IQueryPlanCacheValue> current = map;
        Map<IQueryPlanCacheKey, IQueryPlanCacheValue> resized = newBoundedMap(capacity);
        if (current != null && resized != null) {
            synchronized (current) {
                // LRUMap iterates least-to-most recently used, so shrinking retains the most recent entries
                resized.putAll(current);
            }
        }
        map = resized;
    }

    private static Map<IQueryPlanCacheKey, IQueryPlanCacheValue> newBoundedMap(int capacity) {
        return capacity > 0 ? Collections.synchronizedMap(new LRUMap<>(capacity)) : null;
    }

    @Override
    public String toString() {
        Map<IQueryPlanCacheKey, IQueryPlanCacheValue> current = map;
        if (current == null) {
            return "";
        }
        StringBuilder buffer = new StringBuilder();
        int cnt = 1;
        synchronized (current) {
            for (Map.Entry<IQueryPlanCacheKey, IQueryPlanCacheValue> entry : current.entrySet()) {
                buffer.append("Entry ").append(cnt).append(" query:\n");
                buffer.append(entry.getKey().queryString()).append("\n\n");
                cnt++;
            }
        }
        return buffer.toString();
    }
}
