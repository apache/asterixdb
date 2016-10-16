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
package org.apache.asterix.external.feed.management;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.feed.api.IActiveLifecycleEventSubscriber;

public class ActiveLifecycleEventSubscriber implements IActiveLifecycleEventSubscriber {

    private LinkedBlockingQueue<ActiveLifecycleEvent> inbox;

    public ActiveLifecycleEventSubscriber() {
        this.inbox = new LinkedBlockingQueue<>();
    }

    @Override
    public void handleEvent(ActiveLifecycleEvent event) {
        inbox.add(event);
    }

    @Override
    public void assertEvent(ActiveLifecycleEvent event) throws AsterixException, InterruptedException {
        boolean eventOccurred = false;
        ActiveLifecycleEvent e;
        Iterator<ActiveLifecycleEvent> eventsSoFar = inbox.iterator();
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

    private void assertNoFailure(ActiveLifecycleEvent e) throws AsterixException {
        if (e.equals(ActiveLifecycleEvent.FEED_INTAKE_FAILURE) || e.equals(ActiveLifecycleEvent.FEED_COLLECT_FAILURE)
                || e.equals(ActiveLifecycleEvent.ACTIVE_JOB_FAILED)) {
            throw new AsterixException("Failure in active job.");
        }
    }

}
