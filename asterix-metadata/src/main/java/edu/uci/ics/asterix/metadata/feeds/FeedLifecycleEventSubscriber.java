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

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleEventSubscriber;

public class FeedLifecycleEventSubscriber implements IFeedLifecycleEventSubscriber {

    private LinkedBlockingQueue<FeedLifecycleEvent> inbox;

    public FeedLifecycleEventSubscriber() {
        this.inbox = new LinkedBlockingQueue<FeedLifecycleEvent>();
    }

    @Override
    public void handleFeedEvent(FeedLifecycleEvent event) {
        inbox.add(event);
    }

    @Override
    public void assertEvent(FeedLifecycleEvent event) throws AsterixException, InterruptedException {
        boolean eventOccurred = false;
        FeedLifecycleEvent e = null;
        Iterator<FeedLifecycleEvent> eventsSoFar = inbox.iterator();
        while (eventsSoFar.hasNext()) {
            e = eventsSoFar.next();
            assertNoFailure(e);
            eventOccurred = e.equals(event);
        }

        while (!eventOccurred) {
            e = inbox.take();
            eventOccurred = e.equals(event);
            if (!eventOccurred) {
                assertNoFailure(e);
            }
        }
    }

    private void assertNoFailure(FeedLifecycleEvent e) throws AsterixException {
        if (e.equals(FeedLifecycleEvent.FEED_INTAKE_FAILURE) || e.equals(FeedLifecycleEvent.FEED_COLLECT_FAILURE)) {
            throw new AsterixException("Failure in feed");
        }
    }

}
