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
package org.apache.asterix.external.operators;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveMessage;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.external.feed.api.ISubscribableRuntime;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.message.EndFeedMessage;
import org.apache.asterix.external.feed.runtime.AdapterRuntimeManager;
import org.apache.asterix.external.feed.runtime.CollectionRuntime;
import org.apache.asterix.external.feed.runtime.IngestionRuntime;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * @deprecated
 *             Runtime for the FeedMessageOpertorDescriptor. This operator is responsible for communicating
 *             a feed message to the local feed manager on the host node controller.
 *             For messages, use IMessageBroker interfaces
 * @see FeedMessageOperatorDescriptor
 *      IFeedMessage
 *      IFeedManager
 */
@Deprecated
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageOperatorNodePushable.class.getName());

    private final FeedConnectionId connectionId;
    private final IActiveMessage message;
    private final ActiveManager feedManager;
    private final int partition;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId connectionId,
            IActiveMessage feedMessage, int partition) {
        this.connectionId = connectionId;
        this.message = feedMessage;
        this.partition = partition;
        IAsterixAppRuntimeContext runtimeCtx =
                (IAsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject();
        this.feedManager = (ActiveManager) runtimeCtx.getActiveManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();
            switch (message.getMessageType()) {
                case END:
                    EndFeedMessage endFeedMessage = (EndFeedMessage) message;
                    switch (endFeedMessage.getEndMessageType()) {
                        case DISCONNECT_FEED:
                            hanldeDisconnectFeedTypeMessage(endFeedMessage);
                            break;
                        case DISCONTINUE_SOURCE:
                            handleDiscontinueFeedTypeMessage(endFeedMessage);
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    private void handleDiscontinueFeedTypeMessage(EndFeedMessage endFeedMessage) throws Exception {
        EntityId sourceFeedId = endFeedMessage.getSourceFeedId();
        ActiveRuntimeId subscribableRuntimeId =
                new ActiveRuntimeId(sourceFeedId, FeedRuntimeType.INTAKE.toString(), partition);
        ISubscribableRuntime feedRuntime = (ISubscribableRuntime) feedManager.getRuntime(subscribableRuntimeId);
        AdapterRuntimeManager adapterRuntimeManager = ((IngestionRuntime) feedRuntime).getAdapterRuntimeManager();
        adapterRuntimeManager.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopped Adapter " + adapterRuntimeManager);
        }
    }

    private void hanldeDisconnectFeedTypeMessage(EndFeedMessage endFeedMessage) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ending feed:" + endFeedMessage.getFeedConnectionId());
        }
        ActiveRuntimeId runtimeId;
        FeedRuntimeType subscribableRuntimeType = ((EndFeedMessage) message).getSourceRuntimeType();
        if (endFeedMessage.isCompleteDisconnection()) {
            // subscribableRuntimeType represents the location at which the feed connection receives
            // data
            FeedRuntimeType runtimeType;
            switch (subscribableRuntimeType) {
                case INTAKE:
                    runtimeType = FeedRuntimeType.COLLECT;
                    break;
                case COMPUTE:
                    runtimeType = FeedRuntimeType.COMPUTE_COLLECT;
                    break;
                default:
                    throw new IllegalStateException("Invalid subscribable runtime type " + subscribableRuntimeType);
            }

            runtimeId = new ActiveRuntimeId(endFeedMessage.getSourceFeedId(), runtimeType.toString(), partition);
            CollectionRuntime feedRuntime = (CollectionRuntime) feedManager.getRuntime(runtimeId);
            if (feedRuntime != null) {
                feedRuntime.getSourceRuntime().unsubscribe(feedRuntime);
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Complete Unsubscription of " + endFeedMessage.getFeedConnectionId());
            }
        } else {
            // subscribaleRuntimeType represents the location for data hand-off in presence of
            // subscribers
            switch (subscribableRuntimeType) {
                case INTAKE:
                    // illegal state as data hand-off from one feed to another does not happen at
                    // intake
                    throw new IllegalStateException("Illegal State, invalid runtime type  " + subscribableRuntimeType);
                case COMPUTE:
                    // feed could be primary or secondary, doesn't matter
                    ActiveRuntimeId feedSubscribableRuntimeId = new ActiveRuntimeId(connectionId.getFeedId(),
                            FeedRuntimeType.COMPUTE.toString(), partition);
                    ISubscribableRuntime feedRuntime =
                            (ISubscribableRuntime) feedManager.getRuntime(feedSubscribableRuntimeId);
                    runtimeId = new ActiveRuntimeId(endFeedMessage.getSourceFeedId(),
                            FeedRuntimeType.COMPUTE_COLLECT.toString(), partition);
                    CollectionRuntime feedCollectionRuntime = (CollectionRuntime) feedManager.getRuntime(runtimeId);
                    feedRuntime.unsubscribe(feedCollectionRuntime);
                    break;
                default:
                    break;
            }

        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed from feed :" + connectionId);
        }
    }
}
