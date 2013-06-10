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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.api.AsterixThreadExecutor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IDatasourceAdapter adapter;
    private final int partition;
    private final IFeedManager feedManager;
    private final FeedId feedId;
    private final LinkedBlockingQueue<IFeedMessage> inbox;
    private FeedInboxMonitor feedInboxMonitor;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyEnforcer policyEnforcer;

    public FeedIntakeOperatorNodePushable(FeedId feedId, IDatasourceAdapter adapter, Map<String, String> feedPolicy,
            int partition) {
        this.adapter = adapter;
        this.partition = partition;
        this.feedManager = (IFeedManager) FeedManager.INSTANCE;
        this.feedId = feedId;
        inbox = new LinkedBlockingQueue<IFeedMessage>();
        this.feedPolicy = feedPolicy;
        policyEnforcer = new FeedPolicyEnforcer(feedId, feedPolicy);

    }

    @Override
    public void open() throws HyracksDataException {
        if (adapter instanceof IManagedFeedAdapter) {
            feedInboxMonitor = new FeedInboxMonitor((IManagedFeedAdapter) adapter, inbox, partition);
            AsterixThreadExecutor.INSTANCE.execute(feedInboxMonitor);
            feedManager.registerFeedMsgQueue(feedId, inbox);
        }
        writer.open();
        try {
            ((AbstractFeedDatasourceAdapter) adapter).setFeedPolicyEnforcer(policyEnforcer);
            adapter.start(partition, writer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
            if (adapter instanceof IManagedFeedAdapter) {
                try {
                    ((IManagedFeedAdapter) adapter).stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                feedManager.unregisterFeedMsgQueue(feedId, inbox);
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void close() throws HyracksDataException {

    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // do nothing
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }
}

class FeedInboxMonitor extends Thread {

    private LinkedBlockingQueue<IFeedMessage> inbox;
    private final IManagedFeedAdapter adapter;

    public FeedInboxMonitor(IManagedFeedAdapter adapter, LinkedBlockingQueue<IFeedMessage> inbox, int partition) {
        this.inbox = inbox;
        this.adapter = adapter;
    }

    @Override
    public void run() {
        while (true) {
            try {
                IFeedMessage feedMessage = inbox.take();
                switch (feedMessage.getMessageType()) {
                    case STOP:
                        adapter.stop();
                        break;
                    case ALTER:
                        adapter.alter(((AlterFeedMessage) feedMessage).getAlteredConfParams());
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
