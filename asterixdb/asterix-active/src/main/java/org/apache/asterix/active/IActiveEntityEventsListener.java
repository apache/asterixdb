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

import org.apache.asterix.common.metadata.IDataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IActiveEntityEventsListener {

    /**
     * Notify the listener that an event related to the entity has taken place
     * Examples of such events include
     * 1. Job created
     * 2. Job completed
     * 3. Partition event
     *
     * @param event
     *            the event that took place
     */
    void notify(ActiveEvent event);

    /**
     * @return the state of the entity
     */
    ActivityState getState();

    /**
     * @return the active entity id
     */
    EntityId getEntityId();

    /**
     * dataset
     *
     * @return
     */
    boolean isEntityUsingDataset(IDataset dataset);

    /**
     * subscribe to events. subscription ends when subscriber.done() returns true
     *
     * @param subscriber
     * @throws HyracksDataException
     */
    void subscribe(IActiveEventSubscriber subscriber) throws HyracksDataException;

    /**
     * The most recent acquired stats for the active entity
     *
     * @return
     */
    String getStats();

    /**
     * @return The timestamp of the most recent acquired stats for the active entity
     */
    long getStatsTimeStamp();

    /**
     * refresh the stats
     *
     * @param timeout
     * @throws HyracksDataException
     */
    void refreshStats(long timeout) throws HyracksDataException;
}
