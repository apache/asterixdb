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

import java.util.Set;

import org.apache.asterix.active.ActiveEvent;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.IActiveEntityEventsListener;

public class WaitForStateSubscriber extends AbstractSubscriber {

    private final Set<ActivityState> targetStates;

    public WaitForStateSubscriber(IActiveEntityEventsListener listener, Set<ActivityState> targetStates) {
        super(listener);
        this.targetStates = targetStates;
        listener.subscribe(this);
    }

    @Override
    public void notify(ActiveEvent event) {
        if (targetStates.contains(listener.getState())) {
            complete(listener.getJobFailure());
        } else if (event != null && event.getEventKind() == ActiveEvent.Kind.FAILURE) {
            complete((Exception) event.getEventObject());
        }
    }

    @Override
    public void subscribed(IActiveEntityEventsListener eventsListener) {
        if (targetStates.contains(listener.getState())) {
            complete(null);
        }
    }
}
