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

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * Runtime for the @see{FeedMessageOperatorDescriptor}
 */
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageOperatorNodePushable.class.getName());

    private final FeedId feedId;
    private final IFeedMessage feedMessage;
    private final int partition;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, FeedId feedId, IFeedMessage feedMessage,
            int partition, int nPartitions) {
        this.feedId = feedId;
        this.feedMessage = feedMessage;
        this.partition = partition;
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();
            AdapterRuntimeManager adapterRuntimeMgr = FeedManager.INSTANCE.getFeedRuntimeManager(feedId, partition);
            if (adapterRuntimeMgr != null) {
                switch (feedMessage.getMessageType()) {
                    case END:
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Ending feed:" + feedId);
                        }
                        adapterRuntimeMgr.stop();
                        FeedManager.INSTANCE.deRegisterFeedRuntime(adapterRuntimeMgr);
                        break;
                    case ALTER:
                        adapterRuntimeMgr.getFeedAdapter().alter(
                                ((AlterFeedMessage) feedMessage).getAlteredConfParams());
                        break;
                }
            } else {
                throw new AsterixException("Unknown feed: " + feedId);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

}
