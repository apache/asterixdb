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

public interface ICacheManager {

    /**
     * Puts the key and value in the cache and evaluates the value at the time it is put
     * in the cache.
     *
     * @param key
     * @param value
     */
    void put(String key, ICacheableValue value);

    /**
     * Gets the cached value associated with {@code key}
     *
     * @param key
     * @return
     */
    Object get(String key);

    /**
     * Invalidates the cached value associated with {@code key}
     *
     * @param key
     */
    void invalidate(String key);
}