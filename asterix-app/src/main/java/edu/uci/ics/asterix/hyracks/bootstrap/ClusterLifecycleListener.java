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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.cluster.ClusterManager;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.api.application.IClusterLifecycleListener;

public class ClusterLifecycleListener implements IClusterLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(ClusterLifecycleListener.class.getName());

    public static ClusterLifecycleListener INSTANCE = new ClusterLifecycleListener();

    private ClusterLifecycleListener() {
        Thread t = new Thread(eventHandler);
        t.start();
    }

    private static final LinkedBlockingQueue<Set<IClusterManagementWork>> outbox = new LinkedBlockingQueue<Set<IClusterManagementWork>>();
    private static ClusterEventHandler eventHandler = new ClusterEventHandler(outbox);

    @Override
    public void notifyNodeJoin(String nodeId, Map<String, String> ncConfiguration) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("NC: " + nodeId + " joined");
        }
        AsterixClusterProperties.INSTANCE.addNCConfiguration(nodeId, ncConfiguration);

    }

    public void notifyNodeFailure(Set<String> deadNodeIds) {
        for (String deadNode : deadNodeIds) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NC: " + deadNode + " left");
            }
            AsterixClusterProperties.INSTANCE.removeNCConfiguration(deadNode);
        }
        Set<IClusterEventsSubscriber> subscribers = ClusterManager.INSTANCE.getRegisteredClusterEventSubscribers();
        Set<IClusterManagementWork> work = new HashSet<IClusterManagementWork>();
        for (IClusterEventsSubscriber sub : subscribers) {
            work.addAll(sub.notifyNodeFailure(deadNodeIds));
        }

        try {
            outbox.put(work);
        } catch (InterruptedException e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Interrupted :" + e.getMessage());
            }
        }

    }
}
