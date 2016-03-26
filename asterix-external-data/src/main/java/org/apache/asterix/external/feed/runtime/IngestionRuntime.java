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

import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.apache.asterix.external.api.IAdapterRuntimeManager;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.asterix.external.feed.dataflow.FrameDistributor;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;

public class IngestionRuntime extends SubscribableRuntime {

    private final IAdapterRuntimeManager adapterRuntimeManager;
    private final IHyracksTaskContext ctx;

    public IngestionRuntime(FeedId feedId, FeedRuntimeId runtimeId, DistributeFeedFrameWriter feedWriter,
            RecordDescriptor recordDesc, IAdapterRuntimeManager adaptorRuntimeManager, IHyracksTaskContext ctx) {
        super(feedId, runtimeId, null, feedWriter, recordDesc);
        this.adapterRuntimeManager = adaptorRuntimeManager;
        this.ctx = ctx;
    }

    @Override
    public void subscribeFeed(FeedPolicyAccessor fpa, CollectionRuntime collectionRuntime) throws Exception {
        FeedFrameCollector reader = dWriter.subscribeFeed(fpa, collectionRuntime.getInputHandler(),
                collectionRuntime.getConnectionId());
        collectionRuntime.setFrameCollector(reader);

        if (dWriter.getDistributionMode().equals(FrameDistributor.DistributionMode.SINGLE)) {
            ctx.setSharedObject(ByteBuffer.allocate(MessagingFrameTupleAppender.MAX_MESSAGE_SIZE));
            adapterRuntimeManager.start();
        }
        subscribers.add(collectionRuntime);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Subscribed feed collection [" + collectionRuntime + "] to " + this);
        }
        collectionRuntime.getCtx().setSharedObject(ctx.getSharedObject());
    }

    @Override
    public void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        if (dWriter.getDistributionMode().equals(FrameDistributor.DistributionMode.SINGLE)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping adapter for " + this + " as no more registered collectors");
            }
            adapterRuntimeManager.stop();
        } else {
            dWriter.unsubscribeFeed(collectionRuntime.getInputHandler());
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed feed collection [" + collectionRuntime + "] from " + this);
        }
        subscribers.remove(collectionRuntime);
    }

    public void endOfFeed() throws InterruptedException {
        dWriter.notifyEndOfFeed();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Notified End Of Feed  [" + this + "]");
        }
    }

    public IAdapterRuntimeManager getAdapterRuntimeManager() {
        return adapterRuntimeManager;
    }

}
