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

package org.apache.asterix.common.context;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.api.IDiskWriteRateLimiterProvider;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.buffercache.IRateLimiter;
import org.apache.hyracks.storage.common.buffercache.SleepRateLimiter;

public class DiskWriteRateLimiterProvider implements IDiskWriteRateLimiterProvider {
    // stores the write rate limiter for each NC partition
    private final Map<Integer, IRateLimiter> limiters = new HashMap<>();

    @Override
    public synchronized IRateLimiter getRateLimiter(INCServiceContext serviceCtx, IResource resource)
            throws HyracksDataException {
        int partition = StoragePathUtil.getPartitionNumFromRelativePath(resource.getPath());
        IRateLimiter limiter = limiters.get(partition);
        if (limiter == null) {
            INcApplicationContext appCtx = (INcApplicationContext) serviceCtx.getApplicationContext();
            long writeRateLimit = appCtx.getStorageProperties().getWriteRateLimit();
            limiter = SleepRateLimiter.create(writeRateLimit);
            limiters.put(partition, limiter);
        }
        return limiter;
    }

}
