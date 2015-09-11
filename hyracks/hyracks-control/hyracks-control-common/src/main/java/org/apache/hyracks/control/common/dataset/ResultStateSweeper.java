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
import org.apache.hyracks.api.job.JobId;

/**
 * Sweeper to clean up the stale result distribution files and result states.
 */
public class ResultStateSweeper implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(ResultStateSweeper.class.getName());

    private final IDatasetManager datasetManager;

    private final long resultTTL;

    private final long resultSweepThreshold;

    private final List<JobId> toBeCollected;

    public ResultStateSweeper(IDatasetManager datasetManager, long resultTTL, long resultSweepThreshold) {
        this.datasetManager = datasetManager;
        this.resultTTL = resultTTL;
        this.resultSweepThreshold = resultSweepThreshold;
        toBeCollected = new ArrayList<JobId>();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(resultSweepThreshold);
                sweep();
            } catch (InterruptedException e) {
                LOGGER.severe("Result cleaner thread interrupted, but we continue running it.");
                // There isn't much we can do really here
                break; // the interrupt was explicit from another thread. This thread should shut down...
            }
        }

    }

    private void sweep() {
        synchronized (datasetManager) {
            toBeCollected.clear();
            for (JobId jobId : datasetManager.getJobIds()) {
                if (System.currentTimeMillis() > datasetManager.getState(jobId).getTimestamp() + resultTTL) {
                    toBeCollected.add(jobId);
                }
            }
            for (JobId jobId : toBeCollected) {
                datasetManager.deinitState(jobId);
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Result state cleanup instance successfully completed.");
        }
    }
}
