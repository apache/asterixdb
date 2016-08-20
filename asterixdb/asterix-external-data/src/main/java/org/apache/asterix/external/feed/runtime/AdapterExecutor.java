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
package org.apache.asterix.external.feed.runtime;

import org.apache.asterix.external.dataset.adapter.FeedAdapter;
import org.apache.asterix.external.util.ExternalDataExceptionUtils;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.log4j.Logger;

/**
 * The class in charge of executing feed adapters.
 */
public class AdapterExecutor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(AdapterExecutor.class.getName());

    private final IFrameWriter writer; // A writer that sends frames to multiple receivers (that can
    // increase or decrease at any time)
    private final FeedAdapter adapter; // The adapter
    private final AdapterRuntimeManager adapterManager;// The runtime manager <-- two way visibility -->

    public AdapterExecutor(IFrameWriter writer, FeedAdapter adapter, AdapterRuntimeManager adapterManager) {
        this.writer = writer;
        this.adapter = adapter;
        this.adapterManager = adapterManager;
    }

    @Override
    public void run() {
        // Start by getting the partition number from the manager
        int partition = adapterManager.getPartition();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Starting ingestion for partition:" + partition);
        }
        boolean continueIngestion = true;
        boolean failedIngestion = false;
        while (continueIngestion) {
            try {
                // Start the adapter
                adapter.start(partition, writer);
                // Adapter has completed execution
                continueIngestion = false;
            } catch (Exception e) {
                LOGGER.error("Exception during feed ingestion ", e);
                // Check if the adapter wants to continue ingestion
                if (ExternalDataExceptionUtils.isResolvable(e)) {
                    continueIngestion = adapter.handleException(e);
                } else {
                    continueIngestion = false;
                }
                failedIngestion = !continueIngestion;
            }
        }
        // Done with the adapter. about to close, setting the stage based on the failed ingestion flag and notifying the
        // runtime manager
        adapterManager.setFailed(failedIngestion);
        adapterManager.setDone(true);
        synchronized (adapterManager) {
            adapterManager.notifyAll();
        }
    }

}
