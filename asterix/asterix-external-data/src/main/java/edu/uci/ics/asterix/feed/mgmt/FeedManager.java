/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.feed.mgmt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.feed.comm.IFeedMessage;

public class FeedManager implements IFeedManager {

    private Map<FeedId, Set<LinkedBlockingQueue<IFeedMessage>>> outGoingMsgQueueMap = new HashMap<FeedId, Set<LinkedBlockingQueue<IFeedMessage>>>();
    private LinkedBlockingQueue<IFeedMessage> incomingMsgQueue = new LinkedBlockingQueue<IFeedMessage>();

    
    @Override
    public boolean deliverMessage(FeedId feedId, IFeedMessage feedMessage) throws Exception {
        Set<LinkedBlockingQueue<IFeedMessage>> operatorQueues = outGoingMsgQueueMap.get(feedId);
        if (operatorQueues == null) {
            throw new IllegalArgumentException(" unknown feed id " + feedId.getDataverse() + ":" + feedId.getDataset());
        }

        for (LinkedBlockingQueue<IFeedMessage> queue : operatorQueues) {
            queue.put(feedMessage);
        }
        return false;
    }

    @Override
    public void registerFeedOperatorMsgQueue(FeedId feedId, LinkedBlockingQueue<IFeedMessage> queue) {
        Set<LinkedBlockingQueue<IFeedMessage>> feedQueues = outGoingMsgQueueMap.get(feedId);
        if (feedQueues == null) {
            feedQueues = new HashSet<LinkedBlockingQueue<IFeedMessage>>();
        }
        feedQueues.add(queue);
        outGoingMsgQueueMap.put(feedId, feedQueues);
        
    }

    @Override
    public void unregisterFeedOperatorMsgQueue(FeedId feedId, LinkedBlockingQueue<IFeedMessage> queue) {
        Set<LinkedBlockingQueue<IFeedMessage>> feedQueues = outGoingMsgQueueMap.get(feedId);
        if (feedQueues == null || !feedQueues.contains(queue)) {
            throw new IllegalArgumentException(" unable to de-register feed message queue");
        }
        feedQueues.remove(queue);
    }

}
