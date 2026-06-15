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
 * Bounded query plan cache backed by an LRU map wrapped for thread safety.
 */
public class QueryPlanCache implements IQueryPlanCache {

    private final Map<IQueryPlanCacheKey, IQueryPlanCacheValue> map;

    // TODO: Add data structures for fine-grained cache invalidation

    public QueryPlanCache(int maxLength) {
        map = Collections.synchronizedMap(new LRUMap<>(maxLength));
    }

    @Override
    public IQueryPlanCacheValue get(IQueryPlanCacheKey key) {
        return map.get(key);
    }

    @Override
    public void put(IQueryPlanCacheKey key, IQueryPlanCacheValue value) {
        map.put(key, value);
    }

    @Override
    public int clear() {
        synchronized (map) {
            int cleared = map.size();
            map.clear();
            return cleared;
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        int cnt = 1;
        synchronized (map) {
            for (Map.Entry<IQueryPlanCacheKey, IQueryPlanCacheValue> entry : map.entrySet()) {
                buffer.append("Entry ").append(cnt).append(" query:\n");
                buffer.append(entry.getKey().queryString()).append("\n\n");
                cnt++;
            }
        }
        return buffer.toString();
    }
}
