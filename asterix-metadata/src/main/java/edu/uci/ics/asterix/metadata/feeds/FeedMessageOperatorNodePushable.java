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

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * Runtime for the @see{FeedMessageOperatorDescriptor}
 */
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageOperatorNodePushable.class.getName());

    private final FeedConnectionId feedId;
    private final IFeedMessage feedMessage;
    private final int partition;
    private final IHyracksTaskContext ctx;
    private final IFeedManager feedManager;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedId, IFeedMessage feedMessage,
            int partition, int nPartitions) {
        this.feedId = feedId;
        this.feedMessage = feedMessage;
        this.partition = partition;
        this.ctx = ctx;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();
            FeedRuntimeId runtimeId = new FeedRuntimeId(feedId, FeedRuntimeType.INGESTION, partition);
            FeedRuntime feedRuntime = feedManager.getFeedRuntime(runtimeId);
            boolean ingestionLocation = feedRuntime != null;

            switch (feedMessage.getMessageType()) {
                case END:
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Ending feed:" + feedId);
                    }

                    if (ingestionLocation) {
                        AdapterRuntimeManager adapterRuntimeMgr = ((IngestionRuntime) feedRuntime)
                                .getAdapterRuntimeManager();
                        adapterRuntimeMgr.stop();
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Terminating ingestion for :" + feedId);
                        }
                    }
                    break;

                case SUPER_FEED_MANAGER_ELECT:
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Registering Supers Feed Manager for :" + feedId);
                    }
                    FeedManagerElectMessage mesg = ((FeedManagerElectMessage) feedMessage);
                    SuperFeedManager sfm = new SuperFeedManager(mesg.getFeedId(), mesg.getHost(), mesg.getNodeId(),
                            mesg.getPort(), feedManager);
                    synchronized (feedManager) {
                        INCApplicationContext ncCtx = ctx.getJobletContext().getApplicationContext();
                        String nodeId = ncCtx.getNodeId();
                        if (sfm.getNodeId().equals(nodeId)) {
                            sfm.setLocal(true);
                        } else {
                            Thread.sleep(5000);
                        }
                        feedManager.registerSuperFeedManager(feedId, sfm);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Registered super feed mgr " + sfm + " for feed " + feedId);
                        }
                    }
                    break;
            }

        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
}
