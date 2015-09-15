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
package org.apache.asterix.common.feeds;

import java.util.logging.Level;

import org.apache.asterix.common.feeds.api.IAdapterRuntimeManager;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

public class IngestionRuntime extends SubscribableRuntime {

    private final IAdapterRuntimeManager adapterRuntimeManager;

    public IngestionRuntime(FeedId feedId, FeedRuntimeId runtimeId, DistributeFeedFrameWriter feedWriter,
            RecordDescriptor recordDesc, IAdapterRuntimeManager adaptorRuntimeManager) {
        super(feedId, runtimeId, null, feedWriter, recordDesc);
        this.adapterRuntimeManager = adaptorRuntimeManager;
    }

    public void subscribeFeed(FeedPolicyAccessor fpa, CollectionRuntime collectionRuntime) throws Exception {
        FeedFrameCollector reader = dWriter.subscribeFeed(fpa, collectionRuntime.getInputHandler(),
                collectionRuntime.getConnectionId());
        collectionRuntime.setFrameCollector(reader);
        
        if (dWriter.getDistributionMode().equals(FrameDistributor.DistributionMode.SINGLE)) {
            adapterRuntimeManager.start();
        }
        subscribers.add(collectionRuntime);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Subscribed feed collection [" + collectionRuntime + "] to " + this);
        }
    }

    public void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        dWriter.unsubscribeFeed(collectionRuntime.getInputHandler());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed feed collection [" + collectionRuntime + "] from " + this);
        }
        if (dWriter.getDistributionMode().equals(FrameDistributor.DistributionMode.INACTIVE)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping adapter for " + this + " as no more registered collectors");
            }
            adapterRuntimeManager.stop();
        }
        subscribers.remove(collectionRuntime);
    }

    public void endOfFeed() {
        dWriter.notifyEndOfFeed();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Notified End Of Feed  [" + this + "]");
        }
    }

    public IAdapterRuntimeManager getAdapterRuntimeManager() {
        return adapterRuntimeManager;
    }

}
