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

import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

/**
 * Handle (de-)registration of feeds for delivery of control messages.
 */
public interface IFeedManager {

    /**
     * Register an input message queue for a feed specified by feedId.
     * All messages sent to a feed are directed to the registered queue(s).
     * 
     * @param feedId
     *            an identifier for the feed dataset.
     * @param queue
     *            an input message queue for receiving control messages.
     */
    public void registerFeedMsgQueue(FeedId feedId, LinkedBlockingQueue<IFeedMessage> queue);

    /**
     * Unregister an input message queue for a feed specified by feedId.
     * A feed prior to finishing should unregister all previously registered queue(s)
     * as it is no longer active and thus need not process any control messages.
     * 
     * @param feedId
     *            an identifier for the feed dataset.
     * @param queue
     *            an input message queue for receiving control messages.
     */
    public void unregisterFeedMsgQueue(FeedId feedId, LinkedBlockingQueue<IFeedMessage> queue);

    /**
     * Deliver a message to a feed with a given feedId.
     * 
     * @param feedId
     *            identifier for the feed dataset.
     * @param feedMessage
     *            control message that needs to be delivered.
     * @throws Exception
     */
    public void deliverMessage(FeedId feedId, IFeedMessage feedMessage) throws AsterixException;
}
