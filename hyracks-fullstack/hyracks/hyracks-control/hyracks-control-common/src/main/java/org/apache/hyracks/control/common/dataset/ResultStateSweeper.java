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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataset.IDatasetManager;
import org.apache.hyracks.api.dataset.IDatasetStateRecord;
import org.apache.hyracks.api.job.JobId;

/**
 * Sweeper to clean up the stale result distribution files and result states.
 */
public class ResultStateSweeper implements Runnable {

    private final IDatasetManager datasetManager;

    private final long resultTTL;

    private final long resultSweepThreshold;

    private final Logger logger;

    private final List<JobId> toBeCollected;

    public ResultStateSweeper(IDatasetManager datasetManager, long resultTTL, long resultSweepThreshold,
            Logger logger) {
        this.datasetManager = datasetManager;
        this.resultTTL = resultTTL;
        this.resultSweepThreshold = resultSweepThreshold;
        this.logger = logger;
        toBeCollected = new ArrayList<JobId>();
    }

    @Override
    @SuppressWarnings("squid:S2142") // catch interrupted exception
    public void run() {
        while (true) {
            try {
                Thread.sleep(resultSweepThreshold);
                sweep();
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Result cleaner thread interrupted, shutting down.");
                break; // the interrupt was explicit from another thread. This thread should shut down...
            }
        }
    }

    private void sweep() {
        synchronized (datasetManager) {
            toBeCollected.clear();
            for (JobId jobId : datasetManager.getJobIds()) {
                final IDatasetStateRecord state = datasetManager.getState(jobId);
                if (state != null && System.currentTimeMillis() > state.getTimestamp() + resultTTL) {
                    toBeCollected.add(jobId);
                }
            }
            for (JobId jobId : toBeCollected) {
                datasetManager.deinitState(jobId);
            }
        }
        if (logger.isLoggable(Level.FINER)) {
            logger.finer("Result state cleanup instance successfully completed.");
        }
    }
}
