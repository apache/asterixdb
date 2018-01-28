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
package org.apache.asterix.hyracks.bootstrap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IClusterManagementWorkResponse;
import org.apache.asterix.common.api.IClusterManagementWorkResponse.Status;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.cluster.AddNodeWork;
import org.apache.asterix.metadata.cluster.AddNodeWorkResponse;
import org.apache.asterix.metadata.cluster.RemoveNodeWork;
import org.apache.asterix.metadata.cluster.RemoveNodeWorkResponse;
import org.apache.hyracks.api.application.IClusterLifecycleListener;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterLifecycleListener implements IClusterLifecycleListener {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ICcApplicationContext appCtx;
    private final LinkedBlockingQueue<Set<IClusterManagementWork>> workRequestQueue = new LinkedBlockingQueue<>();
    private final ClusterWorkExecutor eventHandler;
    private final List<IClusterManagementWorkResponse> pendingWorkResponses = new ArrayList<>();

    public ClusterLifecycleListener(ICcApplicationContext appCtx) {
        this.appCtx = appCtx;
        eventHandler = new ClusterWorkExecutor(workRequestQueue);
        Thread t = new Thread(eventHandler);
        LOGGER.info("Starting cluster event handler");
        t.start();
    }

    @Override
    public void notifyNodeJoin(String nodeId, Map<IOption, Object> ncConfiguration) throws HyracksException {
        LOGGER.info("NC: {} joined", nodeId);
        IClusterStateManager csm = appCtx.getClusterStateManager();
        csm.notifyNodeJoin(nodeId, ncConfiguration);

        //if metadata node rejoining, we need to rebind the proxy connection when it is active again.
        if (!csm.isMetadataNodeActive()) {
            MetadataManager.INSTANCE.rebindMetadataNode();
        }

        Set<String> nodeAddition = new HashSet<>();
        nodeAddition.add(nodeId);
        updateProgress(ClusterEventType.NODE_JOIN, nodeAddition);
    }

    @Override
    public void notifyNodeFailure(Collection<String> deadNodeIds) throws HyracksException {
        for (String deadNode : deadNodeIds) {
            LOGGER.info("NC: {} left", deadNode);
            IClusterStateManager csm = appCtx.getClusterStateManager();
            csm.notifyNodeFailure(deadNode);

            //if metadata node failed, we need to rebind the proxy connection when it is active again
            if (!csm.isMetadataNodeActive()) {
                MetadataManager.INSTANCE.rebindMetadataNode();
            }
        }
        updateProgress(ClusterEventType.NODE_FAILURE, deadNodeIds);
        Set<IClusterManagementWork> work = new HashSet<>();
        if (!work.isEmpty()) {
            executeWorkSet(work);
        }
    }

    private void updateProgress(ClusterEventType eventType, Collection<String> nodeIds) {
        List<IClusterManagementWorkResponse> completedResponses = new ArrayList<>();
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
        Set<String> nodesToRemove = new HashSet<>();
        Set<AddNodeWork> nodeAdditionRequests = new HashSet<>();
        Set<IClusterManagementWork> nodeRemovalRequests = new HashSet<>();
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
                    RemoveNodeWorkResponse response =
                            new RemoveNodeWorkResponse((RemoveNodeWork) w, Status.IN_PROGRESS);
                    pendingWorkResponses.add(response);
                    break;
            }
        }

        List<String> addedNodes = new ArrayList<>();

        for (AddNodeWork w : nodeAdditionRequests) {
            int n = w.getNumberOfNodesRequested();
            List<String> nodesToBeAddedForWork = new ArrayList<>();
            for (int i = 0; i < n && i < addedNodes.size(); i++) {
                nodesToBeAddedForWork.add(addedNodes.get(i));
            }
            if (nodesToBeAddedForWork.isEmpty()) {
                if (LOGGER.isInfoEnabled()) {
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
