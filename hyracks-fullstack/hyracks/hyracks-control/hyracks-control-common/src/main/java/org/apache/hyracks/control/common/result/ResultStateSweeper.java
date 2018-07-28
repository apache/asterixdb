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

import org.apache.hyracks.api.result.IResultManager;
import org.apache.logging.log4j.Logger;

/**
 * Sweeper to clean up the stale result distribution files and result states.
 */
public class ResultStateSweeper implements Runnable {

    private final IResultManager resultManager;
    private final long resultSweepThreshold;
    private final Logger logger;

    public ResultStateSweeper(IResultManager resultManager, long resultSweepThreshold, Logger logger) {
        this.resultManager = resultManager;
        this.resultSweepThreshold = resultSweepThreshold;
        this.logger = logger;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(resultSweepThreshold);
                resultManager.sweepExpiredResultSets();
                logger.trace("Result state cleanup instance successfully completed.");
            } catch (InterruptedException e) {
                logger.warn("Result cleaner thread interrupted, shutting down.");
                Thread.currentThread().interrupt();
            }
        }
    }
}
