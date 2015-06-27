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
package edu.uci.ics.asterix.metadata.cluster;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.common.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.common.api.IClusterManagementWork;

public abstract class AbstractClusterManagementWork implements IClusterManagementWork {

    protected final IClusterEventsSubscriber subscriber;

    protected final int workId;

    @Override
    public int getWorkId() {
        return workId;
    }

    public AbstractClusterManagementWork(IClusterEventsSubscriber subscriber) {
        this.subscriber = subscriber;
        this.workId = WorkIdGenerator.getNextWorkId();
    }

   

    private static class WorkIdGenerator {
        private static AtomicInteger workId = new AtomicInteger(0);

        public static int getNextWorkId() {
            return workId.incrementAndGet();
        }

    }

    @Override
    public IClusterEventsSubscriber getSourceSubscriber() {
        return subscriber;
    }

}
