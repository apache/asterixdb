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
package org.apache.hyracks.control.cc.partitions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionRequest;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PartitionMatchMaker {
    private static final Logger LOGGER = LogManager.getLogger();

    private final Map<PartitionId, List<PartitionDescriptor>> partitionDescriptors;

    private final Map<PartitionId, List<PartitionRequest>> partitionRequests;

    public PartitionMatchMaker() {
        partitionDescriptors = new HashMap<>();
        partitionRequests = new HashMap<>();
    }

    public List<Pair<PartitionDescriptor, PartitionRequest>> registerPartitionDescriptor(
            PartitionDescriptor partitionDescriptor) {
        List<Pair<PartitionDescriptor, PartitionRequest>> matches = new ArrayList<>();
        PartitionId pid = partitionDescriptor.getPartitionId();
        boolean matched = false;
        List<PartitionRequest> requests = partitionRequests.get(pid);
        if (requests != null) {
            Iterator<PartitionRequest> i = requests.iterator();
            while (i.hasNext()) {
                PartitionRequest req = i.next();
                if (partitionDescriptor.getState().isAtLeast(req.getMinimumState())) {
                    matches.add(Pair.of(partitionDescriptor, req));
                    i.remove();
                    matched = true;
                    if (!partitionDescriptor.isReusable()) {
                        break;
                    }
                }
            }
            if (requests.isEmpty()) {
                partitionRequests.remove(pid);
            }
        }

        if (!matched) {
            List<PartitionDescriptor> descriptors = partitionDescriptors.computeIfAbsent(pid, k -> new ArrayList<>());
            descriptors.add(partitionDescriptor);
        }

        return matches;
    }

    public Pair<PartitionDescriptor, PartitionRequest> matchPartitionRequest(PartitionRequest partitionRequest) {
        Pair<PartitionDescriptor, PartitionRequest> match = null;

        PartitionId pid = partitionRequest.getPartitionId();

        List<PartitionDescriptor> descriptors = partitionDescriptors.get(pid);
        if (descriptors != null) {
            Iterator<PartitionDescriptor> i = descriptors.iterator();
            while (i.hasNext()) {
                PartitionDescriptor descriptor = i.next();
                if (descriptor.getState().isAtLeast(partitionRequest.getMinimumState())) {
                    match = Pair.of(descriptor, partitionRequest);
                    if (!descriptor.isReusable()) {
                        i.remove();
                    }
                    break;
                }
            }
            if (descriptors.isEmpty()) {
                partitionDescriptors.remove(pid);
            }
        }

        if (match == null) {
            List<PartitionRequest> requests = partitionRequests.computeIfAbsent(pid, k -> new ArrayList<>());
            requests.add(partitionRequest);
        }

        return match;
    }

    public PartitionState getMaximumAvailableState(PartitionId pid) {
        List<PartitionDescriptor> descriptors = partitionDescriptors.get(pid);
        if (descriptors == null) {
            return null;
        }
        for (PartitionDescriptor descriptor : descriptors) {
            if (descriptor.getState() == PartitionState.COMMITTED) {
                return PartitionState.COMMITTED;
            }
        }
        return PartitionState.STARTED;
    }

    private interface IEntryFilter<T> {
        boolean matches(T o);
    }

    private static <T> void removeEntries(List<T> list, IEntryFilter<T> filter) {
        list.removeIf(filter::matches);
    }

    private static <T> void removeEntries(Map<PartitionId, List<T>> map, IEntryFilter<T> filter) {
        Iterator<Map.Entry<PartitionId, List<T>>> i = map.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<PartitionId, List<T>> e = i.next();
            List<T> list = e.getValue();
            removeEntries(list, filter);
            if (list.isEmpty()) {
                i.remove();
            }
        }
    }

    public void notifyNodeFailures(final Collection<String> deadNodes) {
        removeEntries(partitionDescriptors, o -> deadNodes.contains(o.getNodeId()));
        removeEntries(partitionRequests, o -> deadNodes.contains(o.getNodeId()));
    }

    public void removeUncommittedPartitions(Set<PartitionId> partitionIds, Set<TaskAttemptId> taIds, JobId jobId) {
        if (!partitionIds.isEmpty()) {
            LOGGER.debug("Removing uncommitted partitions {}: {}", jobId, partitionIds);
        }
        IEntryFilter<PartitionDescriptor> filter =
                o -> o.getState() != PartitionState.COMMITTED && taIds.contains(o.getProducingTaskAttemptId());
        for (PartitionId pid : partitionIds) {
            List<PartitionDescriptor> descriptors = partitionDescriptors.get(pid);
            if (descriptors != null) {
                removeEntries(descriptors, filter);
                if (descriptors.isEmpty()) {
                    partitionDescriptors.remove(pid);
                }
            }
        }
    }

    public void removePartitionRequests(Set<PartitionId> partitionIds, Set<TaskAttemptId> taIds, JobId jobId) {
        if (!partitionIds.isEmpty()) {
            LOGGER.debug("Removing partition requests {}: {}", jobId, partitionIds);
        }
        IEntryFilter<PartitionRequest> filter = o -> taIds.contains(o.getRequestingTaskAttemptId());
        for (PartitionId pid : partitionIds) {
            List<PartitionRequest> requests = partitionRequests.get(pid);
            if (requests != null) {
                removeEntries(requests, filter);
                if (requests.isEmpty()) {
                    partitionRequests.remove(pid);
                }
            }
        }
    }
}
