/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.control.common.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataset.IDatasetManager;
import edu.uci.ics.hyracks.api.dataset.IDatasetStateRecord;
import edu.uci.ics.hyracks.api.job.JobId;

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
            for (Map.Entry<JobId, IDatasetStateRecord> entry : datasetManager.getStateMap().entrySet()) {
                if (System.currentTimeMillis() > entry.getValue().getTimestamp() + resultTTL) {
                    toBeCollected.add(entry.getKey());
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
