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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWorkResponse;
import edu.uci.ics.asterix.metadata.cluster.ClusterManager;
import edu.uci.ics.asterix.metadata.cluster.IClusterManagementWorkResponse.Status;
import edu.uci.ics.asterix.metadata.cluster.RemoveNodeWork;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.api.application.IClusterLifecycleListener;

public class ClusterLifecycleListener implements IClusterLifecycleListener {

    public static ClusterLifecycleListener INSTANCE = new ClusterLifecycleListener();

    private ClusterLifecycleListener() {
    }

    private static final Logger LOGGER = Logger.getLogger(ClusterLifecycleListener.class.getName());

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

        int nodesToAdd = 0;
        Set<String> nodesToRemove = new HashSet<String>();
        Set<IClusterManagementWork> nodeAdditionRequests = new HashSet<IClusterManagementWork>();
        Set<IClusterManagementWork> nodeRemovalRequests = new HashSet<IClusterManagementWork>();
        for (IClusterManagementWork w : work) {
            switch (w.getClusterManagementWorkType()) {
                case ADD_NODE:
                    if (nodesToAdd < ((AddNodeWork) w).getNumberOfNodes()) {
                        nodesToAdd = ((AddNodeWork) w).getNumberOfNodes();
                    }
                    nodeAdditionRequests.add(w);
                    break;
                case REMOVE_NODE:
                    nodesToRemove.addAll(((RemoveNodeWork) w).getNodesToBeRemoved());
                    nodeRemovalRequests.add(w);
                    break;
            }
        }

        Set<Node> addedNodes = new HashSet<Node>();
        for (int i = 0; i < nodesToAdd; i++) {
            Node node = AsterixClusterProperties.INSTANCE.getAvailableSubstitutionNode();
            if (node != null) {
                try {
                    ClusterManager.INSTANCE.addNode(node);
                    addedNodes.add(node);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Added NC at:" + node.getId());
                    }
                } catch (AsterixException e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to add NC at:" + node.getId());
                    }
                    e.printStackTrace();
                }
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to add NC: no more available nodes");
                }
            }
        }

        for (IClusterManagementWork w : nodeAdditionRequests) {
            w.getSourceSubscriber().notifyRequestCompletion(
                    new AddNodeWorkResponse((AddNodeWork) w, Status.SUCCESS, addedNodes));
        }

    }
}
