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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeManager;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.metadata.feeds.AdapterRuntimeManager.State;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter.DataExchangeMode;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedIntakeOperatorNodePushable.class.getName());

    private final int partition;
    private final FeedConnectionId feedId;
    private final LinkedBlockingQueue<IFeedMessage> inbox;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyEnforcer policyEnforcer;
    private final String nodeId;
    private final FrameTupleAccessor fta;
    private final IFeedManager feedManager;

    private FeedRuntime ingestionRuntime;
    private IFeedAdapter adapter;
    private FeedFrameWriter feedFrameWriter;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedId, IFeedAdapter adapter,
            Map<String, String> feedPolicy, int partition, IngestionRuntime ingestionRuntime) {
        this.adapter = adapter;
        this.partition = partition;
        this.feedId = feedId;
        this.ingestionRuntime = ingestionRuntime;
        inbox = new LinkedBlockingQueue<IFeedMessage>();
        this.feedPolicy = feedPolicy;
        policyEnforcer = new FeedPolicyEnforcer(feedId, feedPolicy);
        nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {

        AdapterRuntimeManager adapterRuntimeMgr = null;
        try {
            if (ingestionRuntime == null) {
                feedFrameWriter = new FeedFrameWriter(writer, this, feedId, policyEnforcer, nodeId,
                        FeedRuntimeType.INGESTION, partition, fta, feedManager);
                adapterRuntimeMgr = new AdapterRuntimeManager(feedId, adapter, feedFrameWriter, partition, inbox,
                        feedManager);

                if (adapter.getDataExchangeMode().equals(DataExchangeMode.PULL) && adapter instanceof IPullBasedFeedAdapter) {
                    ((IPullBasedFeedAdapter) adapter).setFeedPolicyEnforcer(policyEnforcer);
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning new feed:" + feedId);
                }
                feedFrameWriter.open();
                adapterRuntimeMgr.start();
            } else {
                adapterRuntimeMgr = ((IngestionRuntime) ingestionRuntime).getAdapterRuntimeManager();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Resuming old feed:" + feedId);
                }
                adapter = adapterRuntimeMgr.getFeedAdapter();
                writer.open();
                adapterRuntimeMgr.getAdapterExecutor().setWriter(writer);
                adapterRuntimeMgr.getAdapterExecutor().getWriter().reset();
                adapterRuntimeMgr.setState(State.ACTIVE_INGESTION);
                feedFrameWriter = adapterRuntimeMgr.getAdapterExecutor().getWriter();
            }

            ingestionRuntime = adapterRuntimeMgr.getIngestionRuntime();
            synchronized (adapterRuntimeMgr) {
                while (!adapterRuntimeMgr.getState().equals(State.FINISHED_INGESTION)) {
                    adapterRuntimeMgr.wait();
                }
            }
            feedManager.deRegisterFeedRuntime(ingestionRuntime.getFeedRuntimeId());
            feedFrameWriter.close();
        } catch (InterruptedException ie) {
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Continuing on failure as per feed policy, switching to INACTIVE INGESTION temporarily");
                }
                adapterRuntimeMgr.setState(State.INACTIVE_INGESTION);
                FeedRuntimeManager runtimeMgr = feedManager.getFeedRuntimeManager(feedId);
                try {
                    runtimeMgr.close(false);
                } catch (IOException ioe) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to close Feed Runtime Manager " + ioe.getMessage());
                    }
                }
                feedFrameWriter.fail();
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Interrupted Exception, something went wrong");
                }

                feedManager.deRegisterFeedRuntime(ingestionRuntime.getFeedRuntimeId());
                feedFrameWriter.close();
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }
}
