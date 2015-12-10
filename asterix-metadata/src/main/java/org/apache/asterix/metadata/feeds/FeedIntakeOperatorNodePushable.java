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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.api.IActiveManager;
import org.apache.asterix.common.active.api.IAdapterRuntimeManager;
import org.apache.asterix.common.active.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.asterix.common.active.api.IAdapterRuntimeManager.State;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.feeds.CollectionRuntime;
import org.apache.asterix.common.feeds.DistributeFeedFrameWriter;
import org.apache.asterix.common.feeds.FeedPolicyAccessor;
import org.apache.asterix.common.feeds.IngestionRuntime;
import org.apache.asterix.common.feeds.SubscribableFeedRuntimeId;
import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.common.feeds.api.IFeedSubscriptionManager;
import org.apache.asterix.common.feeds.api.IIntakeProgressTracker;
import org.apache.asterix.common.feeds.api.ISubscriberRuntime;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * Provides the core functionality to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedIntakeOperatorNodePushable.class.getName());

    private final ActiveObjectId feedId;
    private final int partition;
    private final IFeedSubscriptionManager feedSubscriptionManager;
    private final IActiveManager feedManager;
    private final IHyracksTaskContext ctx;
    private final IFeedAdapterFactory adapterFactory;

    private IngestionRuntime ingestionRuntime;
    private IFeedAdapter adapter;
    private IIntakeProgressTracker tracker;
    private DistributeFeedFrameWriter feedFrameWriter;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, ActiveObjectId feedId,
            IFeedAdapterFactory adapterFactory, int partition, IngestionRuntime ingestionRuntime,
            FeedPolicyAccessor policyAccessor) {
        this.ctx = ctx;
        this.feedId = feedId;
        this.partition = partition;
        this.ingestionRuntime = ingestionRuntime;
        this.adapterFactory = adapterFactory;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedSubscriptionManager = runtimeCtx.getActiveManager().getFeedSubscriptionManager();
        this.feedManager = runtimeCtx.getActiveManager();

    }

    @Override
    public void initialize() throws HyracksDataException {
        IAdapterRuntimeManager adapterRuntimeManager = null;
        try {
            if (ingestionRuntime == null) {
                try {
                    adapter = (IFeedAdapter) adapterFactory.createAdapter(ctx, partition);
                    if (adapterFactory.isRecordTrackingEnabled()) {
                        tracker = adapterFactory.createIntakeProgressTracker();
                    }
                } catch (Exception e) {
                    LOGGER.severe("Unable to create adapter : " + adapterFactory.getName() + "[" + partition + "]"
                            + " Exception " + e);
                    throw new HyracksDataException(e);
                }
                FrameTupleAccessor fta = new FrameTupleAccessor(recordDesc);
                feedFrameWriter = new DistributeFeedFrameWriter(ctx, feedId, writer, ActiveRuntimeType.INTAKE,
                        partition, fta, feedManager);
                adapterRuntimeManager = new AdapterRuntimeManager(feedId, adapter, tracker, feedFrameWriter, partition);
                SubscribableFeedRuntimeId runtimeId = new SubscribableFeedRuntimeId(feedId, ActiveRuntimeType.INTAKE,
                        partition);
                ingestionRuntime = new IngestionRuntime(feedId, runtimeId, feedFrameWriter, recordDesc,
                        adapterRuntimeManager);
                feedSubscriptionManager.registerFeedSubscribableRuntime(ingestionRuntime);
                feedFrameWriter.open();
            } else {
                if (ingestionRuntime.getAdapterRuntimeManager().getState().equals(State.INACTIVE_INGESTION)) {
                    ingestionRuntime.getAdapterRuntimeManager().setState(State.ACTIVE_INGESTION);
                    adapter = ingestionRuntime.getAdapterRuntimeManager().getFeedAdapter();
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(" Switching to " + State.ACTIVE_INGESTION + " for ingestion runtime "
                                + ingestionRuntime);
                        LOGGER.info(" Adaptor " + adapter.getClass().getName() + "[" + partition + "]"
                                + " connected to backend for feed " + feedId);
                    }
                    feedFrameWriter = ingestionRuntime.getActiveFrameWriter();

                } else {
                    String message = "Feed Ingestion Runtime for feed " + feedId
                            + " is already registered and is active!.";
                    LOGGER.severe(message);
                    throw new IllegalStateException(message);
                }
            }

            waitTillIngestionIsOver(adapterRuntimeManager);
            feedSubscriptionManager
                    .deregisterFeedSubscribableRuntime((SubscribableFeedRuntimeId) ingestionRuntime.getRuntimeId());
            if (adapterRuntimeManager.getState().equals(IAdapterRuntimeManager.State.FAILED_INGESTION)) {
                throw new HyracksDataException("Unable to ingest data");
            }

        } catch (InterruptedException ie) {
            /*
             * An Interrupted Exception is thrown if the Intake job cannot progress further due to failure of another node involved in the Hyracks job.
             * As the Intake job involves only the intake operator, the exception is indicative of a failure at the sibling intake operator location.
             * The surviving intake partitions must continue to live and receive data from the external source.
             */
            List<ISubscriberRuntime> subscribers = ingestionRuntime.getSubscribers();
            FeedPolicyAccessor policyAccessor = new FeedPolicyAccessor(new HashMap<String, String>());
            boolean needToHandleFailure = false;
            List<ISubscriberRuntime> failingSubscribers = new ArrayList<ISubscriberRuntime>();
            for (ISubscriberRuntime subscriber : subscribers) {
                policyAccessor.reset(subscriber.getFeedPolicy());
                if (!policyAccessor.continueOnHardwareFailure()) {
                    failingSubscribers.add(subscriber);
                } else {
                    needToHandleFailure = true;
                }
            }

            for (ISubscriberRuntime failingSubscriber : failingSubscribers) {
                try {
                    ingestionRuntime.unsubscribeFeed((CollectionRuntime) failingSubscriber);
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning(
                                "Excpetion in unsubscribing " + failingSubscriber + " message " + e.getMessage());
                    }
                }
            }

            if (needToHandleFailure) {
                ingestionRuntime.getAdapterRuntimeManager().setState(State.INACTIVE_INGESTION);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Switching to " + State.INACTIVE_INGESTION + " on occurrence of failure.");
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(
                            "Interrupted Exception. None of the subscribers need to handle failures. Shutting down feed ingestion");
                }
                feedSubscriptionManager
                        .deregisterFeedSubscribableRuntime((SubscribableFeedRuntimeId) ingestionRuntime.getRuntimeId());
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        } finally {
            if (ingestionRuntime != null
                    && !ingestionRuntime.getAdapterRuntimeManager().getState().equals(State.INACTIVE_INGESTION)) {
                feedFrameWriter.close();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closed Frame Writer " + feedFrameWriter + " adapter state "
                            + ingestionRuntime.getAdapterRuntimeManager().getState());
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Ending intake operator node pushable in state " + State.INACTIVE_INGESTION
                            + " Will resume after correcting failure");
                }
            }

        }
    }

    private void waitTillIngestionIsOver(IAdapterRuntimeManager adapterRuntimeManager) throws InterruptedException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Waiting for adaptor [" + partition + "]" + "to be done with ingestion of feed " + feedId);
        }
        synchronized (adapterRuntimeManager) {
            while (!(adapterRuntimeManager.getState().equals(IAdapterRuntimeManager.State.FINISHED_INGESTION)
                    || (adapterRuntimeManager.getState().equals(IAdapterRuntimeManager.State.FAILED_INGESTION)))) {
                adapterRuntimeManager.wait();
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Adaptor " + adapter.getClass().getName() + "[" + partition + "]"
                    + " done with ingestion of feed " + feedId);
        }
    }

}
