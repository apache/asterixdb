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
package org.apache.asterix.cloud.clients;

import org.apache.asterix.cloud.clients.profiler.limiter.IRateLimiter;
import org.apache.asterix.cloud.clients.profiler.limiter.IRequestRateLimiter;
import org.apache.asterix.cloud.clients.profiler.limiter.NoOpRateLimiter;
import org.apache.asterix.cloud.clients.profiler.limiter.TokenBasedRateLimiter;

public abstract class AbstractCloudRequestRateLimiter implements IRequestRateLimiter {

    protected final IRateLimiter writeLimiter;
    protected final IRateLimiter readLimiter;

    public AbstractCloudRequestRateLimiter(int writeMaxRequestsPerSeconds, int readMaxRequestsPerSeconds,
            long tokenAcquireTimeout) {
        this.writeLimiter = createLimiter(writeMaxRequestsPerSeconds, tokenAcquireTimeout);
        this.readLimiter = createLimiter(readMaxRequestsPerSeconds, tokenAcquireTimeout);
    }

    @Override
    public void writeRequest() {
        writeLimiter.acquire();
    }

    @Override
    public void readRequest() {
        readLimiter.acquire();
    }

    @Override
    public void listRequest() {
        readLimiter.acquire();
    }

    @Override
    public long getReadThrottleCount() {
        return readLimiter.getThrottleCount();
    }

    @Override
    public long getWriteThrottleCount() {
        return writeLimiter.getThrottleCount();
    }

    private static IRateLimiter createLimiter(int maxRequestsPerSecond, long tokeAcquireTimeout) {
        if (maxRequestsPerSecond > 0) {
            return new TokenBasedRateLimiter(maxRequestsPerSecond, tokeAcquireTimeout);
        }
        return NoOpRateLimiter.INSTANCE;
    }
}
