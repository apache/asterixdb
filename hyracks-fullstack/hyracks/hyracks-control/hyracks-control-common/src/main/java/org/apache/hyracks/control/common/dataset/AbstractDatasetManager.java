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
package org.apache.hyracks.control.common.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.dataset.IDatasetManager;
import org.apache.hyracks.api.dataset.IDatasetStateRecord;
import org.apache.hyracks.api.job.JobId;

public abstract class AbstractDatasetManager implements IDatasetManager {

    private final long nanoResultTTL;

    protected AbstractDatasetManager(long resultTTL) {
        this.nanoResultTTL = TimeUnit.MILLISECONDS.toNanos(resultTTL);
    }

    @Override
    public synchronized void sweepExpiredDatasets() {
        final List<JobId> expiredDatasets = new ArrayList<>();
        final long sweepTime = System.nanoTime();
        for (JobId jobId : getJobIds()) {
            final IDatasetStateRecord state = getState(jobId);
            if (state != null && hasExpired(state, sweepTime, nanoResultTTL)) {
                expiredDatasets.add(jobId);
            }
        }
        for (JobId jobId : expiredDatasets) {
            sweep(jobId);
        }
    }

    private static boolean hasExpired(IDatasetStateRecord dataset, long currentTime, long ttl) {
        return currentTime - dataset.getTimestamp() - ttl > 0;
    }
}
