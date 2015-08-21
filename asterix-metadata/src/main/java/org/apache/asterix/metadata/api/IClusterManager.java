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
package edu.uci.ics.asterix.metadata.api;

import java.util.Set;

import edu.uci.ics.asterix.common.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.schema.cluster.Node;

public interface IClusterManager {

    /**
     * @param node
     * @throws AsterixException
     */
    public void addNode(Node node) throws AsterixException;

    /**
     * @param node
     * @throws AsterixException
     */
    public void removeNode(Node node) throws AsterixException;

    /**
     * @param subscriber
     */
    public void registerSubscriber(IClusterEventsSubscriber subscriber);

    /**
     * @param sunscriber
     * @return
     */
    public boolean deregisterSubscriber(IClusterEventsSubscriber sunscriber);

    /**
     * @return
     */
    public Set<IClusterEventsSubscriber> getRegisteredClusterEventSubscribers();

}
