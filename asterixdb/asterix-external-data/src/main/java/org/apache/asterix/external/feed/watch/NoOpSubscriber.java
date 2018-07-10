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
package org.apache.asterix.external.feed.watch;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.IActiveEntityEventSubscriber;
import org.apache.asterix.active.IActiveEntityEventsListener;

/**
 * An event subscriber that does not listen to any events
 */
public class NoOpSubscriber implements IActiveEntityEventSubscriber {

    public static final NoOpSubscriber INSTANCE = new NoOpSubscriber();

    private NoOpSubscriber() {
    }

    @Override
    public void notify(ActiveEvent event) {
        // no op
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public void sync() {
        // no op
    }

    @Override
    public void subscribed(IActiveEntityEventsListener eventsListener) {
        // no op
    }
}
