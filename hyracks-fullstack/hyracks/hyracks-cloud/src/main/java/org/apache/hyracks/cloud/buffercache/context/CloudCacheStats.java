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
package org.apache.hyracks.cloud.buffercache.context;

import java.util.concurrent.atomic.LongAdder;

/**
 * Counts the pages the buffer cache reads in a cloud deployment, by whether the local disk cache held them or they had
 * to be fetched from cloud storage. Process-wide, since every read context of every index contributes to it.
 */
public final class CloudCacheStats {
    private static final LongAdder HITS = new LongAdder();
    private static final LongAdder HIT_BYTES = new LongAdder();
    private static final LongAdder MISSES = new LongAdder();
    private static final LongAdder MISS_BYTES = new LongAdder();

    private CloudCacheStats() {
    }

    /**
     * @param bytes the size of the page as stored, i.e. compressed where the page is
     */
    public static void pageRead(boolean fromCache, long bytes) {
        if (fromCache) {
            HITS.increment();
            HIT_BYTES.add(bytes);
        } else {
            MISSES.increment();
            MISS_BYTES.add(bytes);
        }
    }

    public static long getHits() {
        return HITS.sum();
    }

    public static long getHitBytes() {
        return HIT_BYTES.sum();
    }

    public static long getMisses() {
        return MISSES.sum();
    }

    public static long getMissBytes() {
        return MISS_BYTES.sum();
    }
}
