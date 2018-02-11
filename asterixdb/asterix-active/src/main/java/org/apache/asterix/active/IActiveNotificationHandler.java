
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

import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Represents the notification handler for events of active entity jobs
 */
public interface IActiveNotificationHandler {

    /**
     * Recover all active jobs that failed
     */
    void recover();

    /**
     * Set whether handler initialization has completed or not
     *
     * @param initialized
     * @throws HyracksDataException
     */
    void setInitialized(boolean initialized) throws HyracksDataException;

    /**
     * @return true if initialization has completed, false otherwise
     */
    boolean isInitialized();

    /**
     * Register a listener for events of an active entity
     *
     * @param listener
     *            the listener to register
     * @throws HyracksDataException
     *             if the active entity already has a listener associated with it
     */
    void registerListener(IActiveEntityEventsListener listener) throws HyracksDataException;

    /**
     * Unregister a listener for events of an active entity
     *
     * @param listener
     *            the listener to unregister
     * @throws HyracksDataException
     *             if the entity is still active or if the listener was not registered
     */
    void unregisterListener(IActiveEntityEventsListener listener) throws HyracksDataException;

    /**
     * @return all the registered event listeners
     */
    IActiveEntityEventsListener[] getEventListeners();

    /**
     * Lookup an event listener using the entity id
     *
     * @param entityId
     *            the lookup key
     * @return the registered listener if found, null otherwise
     */
    IActiveEntityEventsListener getListener(EntityId entityId);

    /**
     * Recieves an active job message from an nc
     *
     * @param message
     *            the message
     */
    void receive(ActivePartitionMessage message);

}
