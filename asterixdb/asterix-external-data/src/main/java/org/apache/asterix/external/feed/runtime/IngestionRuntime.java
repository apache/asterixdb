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

import java.util.logging.Level;

import org.apache.asterix.external.feed.api.ISubscriberRuntime;
import org.apache.asterix.external.feed.dataflow.DistributeFeedFrameWriter;
import org.apache.asterix.external.feed.dataflow.FeedFrameCollector;
import org.apache.asterix.external.feed.management.FeedId;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class IngestionRuntime extends SubscribableRuntime {

    private final AdapterRuntimeManager adapterRuntimeManager;
    private final IHyracksTaskContext ctx;
    private int numSubscribers = 0;

    public IngestionRuntime(FeedId feedId, FeedRuntimeId runtimeId, DistributeFeedFrameWriter feedWriter,
            AdapterRuntimeManager adaptorRuntimeManager, IHyracksTaskContext ctx) {
        super(feedId, runtimeId, feedWriter);
        this.adapterRuntimeManager = adaptorRuntimeManager;
        this.ctx = ctx;
    }

    @Override
    public synchronized void subscribe(CollectionRuntime collectionRuntime) throws HyracksDataException {
        FeedFrameCollector collector = collectionRuntime.getFrameCollector();
        dWriter.subscribe(collector);
        subscribers.add(collectionRuntime);
        if (numSubscribers == 0) {
            ctx.setSharedObject(new VSizeFrame(ctx));
            collectionRuntime.getCtx().setSharedObject(ctx.getSharedObject());
            adapterRuntimeManager.start();
        }
        numSubscribers++;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Subscribed feed collection [" + collectionRuntime + "] to " + this);
        }
    }

    @Override
    public synchronized void unsubscribe(CollectionRuntime collectionRuntime) throws InterruptedException {
        numSubscribers--;
        if (numSubscribers == 0) {
            adapterRuntimeManager.stop();
        }
        subscribers.remove(collectionRuntime);
    }

    public AdapterRuntimeManager getAdapterRuntimeManager() {
        return adapterRuntimeManager;
    }

    public void terminate() {
        for (ISubscriberRuntime subscriber : subscribers) {
            try {
                unsubscribe((CollectionRuntime) subscriber);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Excpetion in unsubscribing " + subscriber + " message " + e.getMessage());
                }
            }
        }
    }

}
