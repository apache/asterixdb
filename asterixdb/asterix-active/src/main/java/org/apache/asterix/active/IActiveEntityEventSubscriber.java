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
package org.apache.asterix.active;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An active event subscriber that subscribes to events related to active entity
 */
public interface IActiveEntityEventSubscriber {

    /**
     * Notifies the subscriber of a new event
     *
     * @param event
     * @throws HyracksDataException
     */
    void notify(ActiveEvent event);

    /**
     * Checkcs whether the subscriber is done receiving events
     *
     * @return
     */
    boolean isDone();

    /**
     * Wait until the terminal event has been received
     *
     * @throws InterruptedException
     */
    void sync() throws InterruptedException;

    /**
     * callback upon successful subscription
     *
     * @param eventsListener
     * @throws HyracksDataException
     */
    void subscribed(IActiveEntityEventsListener eventsListener);
}
