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

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.feeds.AdapterRuntimeManager.State;
import edu.uci.ics.asterix.metadata.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedIntakeOperatorNodePushable.class.getName());

    private IFeedAdapter adapter;
    private final int partition;
    private final FeedConnectionId feedId;
    private final LinkedBlockingQueue<IFeedMessage> inbox;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyEnforcer policyEnforcer;
    private FeedRuntime ingestionRuntime;
    private final String nodeId;
    private FrameTupleAccessor fta;

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

    }

    @Override
    public void initialize() throws HyracksDataException {

        AdapterRuntimeManager adapterRuntimeMgr = null;
        try {
            if (ingestionRuntime == null) {
                FeedFrameWriter mWriter = new FeedFrameWriter(writer, this, feedId, policyEnforcer, nodeId,
                        FeedRuntimeType.INGESTION, partition, fta);
                adapterRuntimeMgr = new AdapterRuntimeManager(feedId, adapter, mWriter, partition, inbox);

                if (adapter instanceof AbstractFeedDatasourceAdapter) {
                    ((AbstractFeedDatasourceAdapter) adapter).setFeedPolicyEnforcer(policyEnforcer);
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning new feed:" + feedId);
                }
                mWriter.open();
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
            }

            synchronized (adapterRuntimeMgr) {
                while (!adapterRuntimeMgr.getState().equals(State.FINISHED_INGESTION)) {
                    adapterRuntimeMgr.wait();
                }
            }
            FeedManager.INSTANCE.deregisterFeed(feedId);
        } catch (InterruptedException ie) {
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Continuing on failure as per feed policy");
                }
                adapterRuntimeMgr.setState(State.INACTIVE_INGESTION);
                writer.fail();
                FeedManager.INSTANCE.deregisterFeed(feedId);
            } else {
                FeedManager.INSTANCE.deregisterFeed(feedId);
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            FeedManager.INSTANCE.deregisterFeed(feedId);
            e.printStackTrace();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }
}

class FeedInboxMonitor extends Thread {

    private LinkedBlockingQueue<IFeedMessage> inbox;
    private final AdapterRuntimeManager runtimeMgr;

    public FeedInboxMonitor(AdapterRuntimeManager runtimeMgr, LinkedBlockingQueue<IFeedMessage> inbox, int partition) {
        this.inbox = inbox;
        this.runtimeMgr = runtimeMgr;
    }

    @Override
    public void run() {
        while (true) {
            try {
                IFeedMessage feedMessage = inbox.take();
                switch (feedMessage.getMessageType()) {
                    case END:
                        runtimeMgr.stop();
                        break;
                    case ALTER:
                        runtimeMgr.getFeedAdapter().alter(((AlterFeedMessage) feedMessage).getAlteredConfParams());
                        break;
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
