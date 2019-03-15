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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

public class CacheManagerTest {

    @Test
    public void expiryTest() throws Exception {
        CacheManager cacheManager = new CacheManager();
        String key = "someKey";
        AtomicInteger realValue = new AtomicInteger(100);
        final TimeBasedCachePolicy policy = TimeBasedCachePolicy.of(5, TimeUnit.SECONDS);
        cacheManager.put(key, new CacheableValue<>(policy, realValue::get));
        realValue.set(200);
        Object cachedValue = null;
        for (int i = 0; i < 10; i++) {
            cachedValue = cacheManager.get(key);
            if ((int) cachedValue == realValue.get()) {
                break;
            }
            TimeUnit.SECONDS.sleep(1);
        }
        Assert.assertEquals((int) cachedValue, realValue.get());
    }
}
