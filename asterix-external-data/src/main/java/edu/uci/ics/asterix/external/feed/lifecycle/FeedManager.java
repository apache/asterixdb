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
package edu.uci.ics.asterix.external.feed.lifecycle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

/**
 * Handle (de-)registration of feeds for delivery of control messages.
 */
public class FeedManager implements IFeedManager {

    public static FeedManager INSTANCE = new FeedManager();

    private FeedManager() {

    }

    private Map<FeedId, Set<LinkedBlockingQueue<IFeedMessage>>> outGoingMsgQueueMap = new HashMap<FeedId, Set<LinkedBlockingQueue<IFeedMessage>>>();

    @Override
    public void deliverMessage(FeedId feedId, IFeedMessage feedMessage) throws AsterixException {
        Set<LinkedBlockingQueue<IFeedMessage>> operatorQueues = outGoingMsgQueueMap.get(feedId);
        try {
            if (operatorQueues != null) {
                for (LinkedBlockingQueue<IFeedMessage> queue : operatorQueues) {
                    queue.put(feedMessage);
                }
            } 
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    @Override
    public void registerFeedMsgQueue(FeedId feedId, LinkedBlockingQueue<IFeedMessage> queue) {
        Set<LinkedBlockingQueue<IFeedMessage>> feedQueues = outGoingMsgQueueMap.get(feedId);
        if (feedQueues == null) {
            feedQueues = new HashSet<LinkedBlockingQueue<IFeedMessage>>();
        }
        feedQueues.add(queue);
        outGoingMsgQueueMap.put(feedId, feedQueues);
    }

    @Override
    public void unregisterFeedMsgQueue(FeedId feedId, LinkedBlockingQueue<IFeedMessage> queue) {
        Set<LinkedBlockingQueue<IFeedMessage>> feedQueues = outGoingMsgQueueMap.get(feedId);
        if (feedQueues == null || !feedQueues.contains(queue)) {
            throw new IllegalArgumentException(" Unable to de-register feed message queue. Unknown feedId " + feedId);
        }
        feedQueues.remove(queue);
    }

}
