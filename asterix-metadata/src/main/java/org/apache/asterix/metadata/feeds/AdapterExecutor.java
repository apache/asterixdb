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
package org.apache.asterix.metadata.feeds;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.DistributeFeedFrameWriter;
import org.apache.asterix.common.feeds.api.IAdapterRuntimeManager;
import org.apache.asterix.common.feeds.api.IAdapterRuntimeManager.State;
import org.apache.asterix.common.feeds.api.IDataSourceAdapter;

public class AdapterExecutor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(AdapterExecutor.class.getName());

    private final DistributeFeedFrameWriter writer;

    private final IDataSourceAdapter adapter;

    private final IAdapterRuntimeManager adapterManager;

    public AdapterExecutor(int partition, DistributeFeedFrameWriter writer, IDataSourceAdapter adapter,
            IAdapterRuntimeManager adapterManager) {
        this.writer = writer;
        this.adapter = adapter;
        this.adapterManager = adapterManager;
    }

    @Override
    public void run() {
        int partition = adapterManager.getPartition();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting ingestion for partition:" + partition);
        }
        boolean continueIngestion = true;
        boolean failedIngestion = false;
        while (continueIngestion) {
            try {
                adapter.start(partition, writer);
                continueIngestion = false;
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Exception during feed ingestion " + e.getMessage());
                    e.printStackTrace();
                }
                continueIngestion = adapter.handleException(e);
                failedIngestion = !continueIngestion;
            }
        }

        adapterManager.setState(failedIngestion ? State.FAILED_INGESTION : State.FINISHED_INGESTION);
        synchronized (adapterManager) {
            adapterManager.notifyAll();
        }
    }

}