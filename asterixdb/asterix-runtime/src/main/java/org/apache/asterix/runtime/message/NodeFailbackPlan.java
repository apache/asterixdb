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
package org.apache.asterix.runtime.message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NodeFailbackPlan {

    public enum FailbackPlanState {
        /**
         * Initial state while selecting the nodes that will participate
         * in the node failback plan.
         */
        PREPARING,
        /**
         * Once a pending {@link PreparePartitionsFailbackRequestMessage} request is added,
         * the state is changed from PREPARING to PENDING_PARTICIPANT_REPONSE to indicate
         * a response is expected and need to wait for it.
         */
        PENDING_PARTICIPANT_REPONSE,
        /**
         * Upon receiving the last {@link PreparePartitionsFailbackResponseMessage} response,
         * the state changes from PENDING_PARTICIPANT_REPONSE to PENDING_COMPLETION to indicate
         * the need to send {@link CompleteFailbackRequestMessage} to the failing back node.
         */
        PENDING_COMPLETION,
        /**
         * if any of the participants fail or the failing back node itself fails during
         * and of these states (PREPARING, PENDING_PARTICIPANT_REPONSE, PENDING_COMPLETION),
         * the state is changed to FAILED.
         */
        FAILED,
        /**
         * if the state is FAILED, and all pending responses (if any) have been received,
         * the state changes from FAILED to PENDING_ROLLBACK to indicate the need to revert
         * the effects of this plan (if any).
         */
        PENDING_ROLLBACK
    }

    private static long planIdGenerator = 0;
    private long planId;
    private final String nodeId;
    private final Set<String> participants;
    private final Map<Integer, String> partition2nodeMap;
    private String nodeToReleaseMetadataManager;
    private int requestId;
    private Map<Integer, PreparePartitionsFailbackRequestMessage> pendingRequests;
    private FailbackPlanState state;

    public static NodeFailbackPlan createPlan(String nodeId) {
        return new NodeFailbackPlan(planIdGenerator++, nodeId);
    }

    private NodeFailbackPlan(long planId, String nodeId) {
        this.planId = planId;
        this.nodeId = nodeId;
        participants = new HashSet<>();
        partition2nodeMap = new HashMap<>();
        pendingRequests = new HashMap<>();
        state = FailbackPlanState.PREPARING;
    }

    public synchronized void addPartitionToFailback(int partitionId, String currentActiveNode) {
        partition2nodeMap.put(partitionId, currentActiveNode);
    }

    public synchronized void addParticipant(String nodeId) {
        participants.add(nodeId);
    }

    public synchronized void notifyNodeFailure(String failedNode) {
        if (participants.contains(failedNode)) {
            if (state == FailbackPlanState.PREPARING) {
                state = FailbackPlanState.FAILED;
            } else if (state == FailbackPlanState.PENDING_PARTICIPANT_REPONSE) {
                /**
                 * if there is any pending request from this failed node,
                 * it should be marked as completed and the plan should be marked as failed
                 */
                Set<Integer> failedRequests = new HashSet<>();
                for (PreparePartitionsFailbackRequestMessage request : pendingRequests.values()) {
                    if (request.getNodeID().equals(failedNode)) {
                        failedRequests.add(request.getRequestId());
                    }
                }

                if (!failedRequests.isEmpty()) {
                    state = FailbackPlanState.FAILED;
                    for (Integer failedRequestId : failedRequests) {
                        markRequestCompleted(failedRequestId);
                    }
                }
            }
        } else if (nodeId.equals(failedNode)) {
            //if the failing back node is the failed node itself
            state = FailbackPlanState.FAILED;
            updateState();
        }
    }

    public synchronized Set<Integer> getPartitionsToFailback() {
        return new HashSet<>(partition2nodeMap.keySet());
    }

    public synchronized void addPendingRequest(PreparePartitionsFailbackRequestMessage msg) {
        //if this is the first request
        if (pendingRequests.size() == 0) {
            state = FailbackPlanState.PENDING_PARTICIPANT_REPONSE;
        }
        pendingRequests.put(msg.getRequestId(), msg);
    }

    public synchronized void markRequestCompleted(int requestId) {
        pendingRequests.remove(requestId);
        updateState();
    }

    private void updateState() {
        if (pendingRequests.size() == 0) {
            switch (state) {
                case PREPARING:
                case FAILED:
                    state = FailbackPlanState.PENDING_ROLLBACK;
                    break;
                case PENDING_PARTICIPANT_REPONSE:
                    state = FailbackPlanState.PENDING_COMPLETION;
                    break;
                default:
                    break;
            }
        }
    }

    public synchronized Set<PreparePartitionsFailbackRequestMessage> getPlanFailbackRequests() {
        Set<PreparePartitionsFailbackRequestMessage> node2Partitions = new HashSet<>();
        /**
         * for each participant, construct a request with the partitions
         * that will be failed back or flushed.
         */
        for (String participant : participants) {
            Set<Integer> partitionToPrepareForFailback = new HashSet<>();
            for (Map.Entry<Integer, String> entry : partition2nodeMap.entrySet()) {
                if (entry.getValue().equals(participant)) {
                    partitionToPrepareForFailback.add(entry.getKey());
                }
            }
            PreparePartitionsFailbackRequestMessage msg = new PreparePartitionsFailbackRequestMessage(planId,
                    requestId++, participant, partitionToPrepareForFailback);
            if (participant.equals(nodeToReleaseMetadataManager)) {
                msg.setReleaseMetadataNode(true);
            }
            node2Partitions.add(msg);
        }
        return node2Partitions;
    }

    public synchronized CompleteFailbackRequestMessage getCompleteFailbackRequestMessage() {
        return new CompleteFailbackRequestMessage(planId, requestId++, nodeId, getPartitionsToFailback());
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getPlanId() {
        return planId;
    }

    public void setNodeToReleaseMetadataManager(String nodeToReleaseMetadataManager) {
        this.nodeToReleaseMetadataManager = nodeToReleaseMetadataManager;
    }

    public synchronized FailbackPlanState getState() {
        return state;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Plan ID: " + planId);
        sb.append(" Failing back node: " + nodeId);
        sb.append(" Participants: " + participants);
        sb.append(" Partitions to Failback: " + partition2nodeMap.keySet());
        return sb.toString();
    }
}
