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
package org.apache.hyracks.control.common.result;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultManager;
import org.apache.hyracks.api.result.IResultStateRecord;

public abstract class AbstractResultManager implements IResultManager {

    private final long nanoResultTTL;

    protected AbstractResultManager(long resultTTLMillis) {
        this.nanoResultTTL = TimeUnit.MILLISECONDS.toNanos(resultTTLMillis);
    }

    @Override
    public synchronized void sweepExpiredResultSets() {
        final List<JobId> expiredResultSets = new ArrayList<>();
        final long sweepTime = System.nanoTime();
        for (JobId jobId : getJobIds()) {
            final IResultStateRecord state = getState(jobId);
            if (state != null && hasExpired(state, sweepTime)) {
                expiredResultSets.add(jobId);
            }
        }
        for (JobId jobId : expiredResultSets) {
            sweep(jobId);
        }
    }

    private boolean hasExpired(IResultStateRecord state, long currentTime) {
        long completeTimestamp = state.getCompleteTimestamp();
        if (completeTimestamp <= 0) {
            // Not completed yet, not expired
            return false;
        }
        // Use per-request TTL if set (> 0), otherwise use system default
        long ttl = state.getResultTtlInNanos() > 0 ? state.getResultTtlInNanos() : nanoResultTTL;
        return currentTime - completeTimestamp - ttl > 0;
    }
}
