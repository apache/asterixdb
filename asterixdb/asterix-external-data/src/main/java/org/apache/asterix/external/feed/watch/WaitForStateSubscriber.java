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
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class WaitForStateSubscriber extends AbstractSubscriber {

    private final ActivityState targetState;

    public WaitForStateSubscriber(IActiveEntityEventsListener listener, ActivityState targetState)
            throws HyracksDataException {
        super(listener);
        this.targetState = targetState;
        listener.subscribe(this);
    }

    @Override
    public void notify(ActiveEvent event) throws HyracksDataException {
        if (listener.getState() == targetState) {
            complete();
        }
    }

    @Override
    public void subscribed(IActiveEntityEventsListener eventsListener) throws HyracksDataException {
        if (eventsListener.getState() == ActivityState.FAILED) {
            throw new RuntimeDataException(ErrorCode.CANNOT_SUBSCRIBE_TO_FAILED_ACTIVE_ENTITY);
        }
        if (listener.getState() == targetState) {
            complete();
        }
    }
}
