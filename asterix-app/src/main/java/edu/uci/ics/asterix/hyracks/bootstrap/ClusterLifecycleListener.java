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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.common.api.IClusterManagementWork;
import edu.uci.ics.asterix.common.api.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.common.api.IClusterManagementWorkResponse.Status;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWorkResponse;
import edu.uci.ics.asterix.metadata.cluster.ClusterManager;
import edu.uci.ics.asterix.metadata.cluster.RemoveNodeWork;
import edu.uci.ics.asterix.metadata.cluster.RemoveNodeWorkResponse;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.api.application.IClusterLifecycleListener;

public class ClusterLifecycleListener implements IClusterLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(ClusterLifecycleListener.class.getName());

    private static final LinkedBlockingQueue<Set<IClusterManagementWork>> workRequestQueue = new LinkedBlockingQueue<Set<IClusterManagementWork>>();

    private static ClusterWorkExecutor eventHandler = new ClusterWorkExecutor(workRequestQueue);

    private static List<IClusterManagementWorkResponse> pendingWorkResponses = new ArrayList<IClusterManagementWorkResponse>();

    public static ClusterLifecycleListener INSTANCE = new ClusterLifecycleListener();

    private ClusterLifecycleListener() {
        Thread t = new Thread(eventHandler);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting cluster event handler");
        }
        t.start();
    }

    public enum ClusterEventType {
        NODE_JOIN,
        NODE_FAILURE
    }

    @Override
    public void notifyNodeJoin(String nodeId, Map<String, String> ncConfiguration) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("NC: " + nodeId + " joined");
        }
        AsterixClusterProperties.INSTANCE.addNCConfiguration(nodeId, ncConfiguration);
        Set<String> nodeAddition = new HashSet<String>();
        nodeAddition.add(nodeId);
        updateProgress(ClusterEventType.NODE_JOIN, nodeAddition);
        Set<IClusterEventsSubscriber> subscribers = ClusterManager.INSTANCE.getRegisteredClusterEventSubscribers();
        Set<IClusterManagementWork> work = new HashSet<IClusterManagementWork>();
        for (IClusterEventsSubscriber sub : subscribers) {
            Set<IClusterManagementWork> workRequest = sub.notifyNodeJoin(nodeId);
            if (workRequest != null && !workRequest.isEmpty()) {
                work.addAll(workRequest);
            }
        }
        if (!work.isEmpty()) {
            executeWorkSet(work);
        }

    }

    public void notifyNodeFailure(Set<String> deadNodeIds) {
        for (String deadNode : deadNodeIds) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NC: " + deadNode + " left");
            }
            AsterixClusterProperties.INSTANCE.removeNCConfiguration(deadNode);
        }
        updateProgress(ClusterEventType.NODE_FAILURE, deadNodeIds);
        Set<IClusterEventsSubscriber> subscribers = ClusterManager.INSTANCE.getRegisteredClusterEventSubscribers();
        Set<IClusterManagementWork> work = new HashSet<IClusterManagementWork>();
        for (IClusterEventsSubscriber sub : subscribers) {
            Set<IClusterManagementWork> workRequest = sub.notifyNodeFailure(deadNodeIds);
            if (workRequest != null && !workRequest.isEmpty()) {
                work.addAll(workRequest);
            }
        }
        if (!work.isEmpty()) {
            executeWorkSet(work);
        }
    }

    private void updateProgress(ClusterEventType eventType, Set<String> nodeIds) {
        List<IClusterManagementWorkResponse> completedResponses = new ArrayList<IClusterManagementWorkResponse>();
        boolean isComplete = false;
        for (IClusterManagementWorkResponse resp : pendingWorkResponses) {
            switch (eventType) {
                case NODE_FAILURE:
                    isComplete = ((RemoveNodeWorkResponse) resp).updateProgress(nodeIds);
                    if (isComplete) {
                        resp.setStatus(Status.SUCCESS);
                        resp.getWork().getSourceSubscriber().notifyRequestCompletion(resp);
                        completedResponses.add(resp);
                    }
                    break;

                case NODE_JOIN:
                    isComplete = ((AddNodeWorkResponse) resp).updateProgress(nodeIds.iterator().next());
                    if (isComplete) {
                        resp.setStatus(Status.SUCCESS);
                        resp.getWork().getSourceSubscriber().notifyRequestCompletion(resp);
                        completedResponses.add(resp);
                    }
                    break;
            }
        }
        pendingWorkResponses.removeAll(completedResponses);
    }

    private void executeWorkSet(Set<IClusterManagementWork> workSet) {
        int nodesToAdd = 0;
        Set<String> nodesToRemove = new HashSet<String>();
        Set<AddNodeWork> nodeAdditionRequests = new HashSet<AddNodeWork>();
        Set<IClusterManagementWork> nodeRemovalRequests = new HashSet<IClusterManagementWork>();
        for (IClusterManagementWork w : workSet) {
            switch (w.getClusterManagementWorkType()) {
                case ADD_NODE:
                    if (nodesToAdd < ((AddNodeWork) w).getNumberOfNodesRequested()) {
                        nodesToAdd = ((AddNodeWork) w).getNumberOfNodesRequested();
                    }
                    nodeAdditionRequests.add((AddNodeWork) w);
                    break;
                case REMOVE_NODE:
                    nodesToRemove.addAll(((RemoveNodeWork) w).getNodesToBeRemoved());
                    nodeRemovalRequests.add(w);
                    RemoveNodeWorkResponse response = new RemoveNodeWorkResponse((RemoveNodeWork) w, Status.IN_PROGRESS);
                    pendingWorkResponses.add(response);
                    break;
            }
        }

        List<String> addedNodes = new ArrayList<String>();
        String asterixInstanceName = AsterixClusterProperties.INSTANCE.getCluster().getInstanceName();
        for (int i = 0; i < nodesToAdd; i++) {
            Node node = AsterixClusterProperties.INSTANCE.getAvailableSubstitutionNode();
            if (node != null) {
                try {
                    ClusterManager.INSTANCE.addNode(node);
                    addedNodes.add(asterixInstanceName + "_" + node.getId());
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

        for (AddNodeWork w : nodeAdditionRequests) {
            int n = w.getNumberOfNodesRequested();
            List<String> nodesToBeAddedForWork = new ArrayList<String>();
            for (int i = 0; i < n && i < addedNodes.size(); i++) {
                nodesToBeAddedForWork.add(addedNodes.get(i));
            }
            if (nodesToBeAddedForWork.isEmpty()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Unable to satisfy request by " + w);
                }
                AddNodeWorkResponse response = new AddNodeWorkResponse(w, nodesToBeAddedForWork);
                response.setStatus(Status.FAILURE);
                w.getSourceSubscriber().notifyRequestCompletion(response);

            } else {
                AddNodeWorkResponse response = new AddNodeWorkResponse(w, nodesToBeAddedForWork);
                pendingWorkResponses.add(response);
            }
        }

    }
}
