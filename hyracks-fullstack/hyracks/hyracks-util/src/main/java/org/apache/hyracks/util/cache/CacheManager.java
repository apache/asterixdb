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
package org.apache.hyracks.util.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class CacheManager implements ICacheManager {

    private final Map<String, ICacheableValue> registry = new ConcurrentHashMap<>();

    @Override
    public void put(String key, ICacheableValue value) {
        registry.put(key, value);
        value.cache();
    }

    @Override
    public Object get(String key) {
        final ICacheableValue value = registry.get(key);
        if (value == null) {
            return null;
        }
        synchronized (value) {
            if (value.getPolicy().expired()) {
                value.cache();
            }
            return value.get();
        }
    }

    @Override
    public void invalidate(String key) {
        registry.remove(key);
    }
}
