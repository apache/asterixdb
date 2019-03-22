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
package org.apache.hyracks.control.nc.partitions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IWorkspaceFileFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.partitions.IPartition;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.comm.channels.NetworkOutputChannel;
import org.apache.hyracks.control.common.job.PartitionDescriptor;
import org.apache.hyracks.control.common.job.PartitionState;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.io.WorkspaceFileFactory;
import org.apache.hyracks.control.nc.resources.DefaultDeallocatableRegistry;
import org.apache.hyracks.net.protocols.muxdemux.AbstractChannelWriteInterface;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PartitionManager {

    private final NodeControllerService ncs;

    private final Map<PartitionId, List<IPartition>> availablePartitionMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    private final Map<PartitionId, NetworkOutputChannel> partitionRequests = new HashMap<>();

    private final Cache<JobId, JobId> failedJobsCache;

    public PartitionManager(NodeControllerService ncs) {
        this.ncs = ncs;
        this.availablePartitionMap = new HashMap<>();
        this.deallocatableRegistry = new DefaultDeallocatableRegistry();
        this.fileFactory = new WorkspaceFileFactory(deallocatableRegistry, ncs.getIoManager());
        failedJobsCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
    }

    public synchronized void registerPartition(PartitionId pid, CcId ccId, TaskAttemptId taId, IPartition partition,
            PartitionState state, boolean updateToCC) throws HyracksDataException {
        try {
            /*
             * process pending requests
             */
            NetworkOutputChannel writer = partitionRequests.remove(pid);
            if (writer != null) {
                writer.setFrameSize(partition.getTaskContext().getInitialFrameSize());
                partition.writeTo(writer);
                if (!partition.isReusable()) {
                    return;
                }
            }

            /*
             * put a coming available partition into the available partition map
             */
            List<IPartition> pList = availablePartitionMap.computeIfAbsent(pid, k -> new ArrayList<>());
            pList.add(partition);

            /*
             * update to CC only when necessary
             */
            if (updateToCC) {
                updatePartitionState(ccId, pid, taId, partition, state);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public synchronized IPartition getPartition(PartitionId pid) {
        return availablePartitionMap.get(pid).get(0);
    }

    public synchronized void registerPartitionRequest(PartitionId partitionId, NetworkOutputChannel writer) {
        if (failedJobsCache.getIfPresent(partitionId.getJobId()) != null) {
            writer.abort(AbstractChannelWriteInterface.REMOTE_ERROR_CODE);
            return;
        }
        List<IPartition> pList = availablePartitionMap.get(partitionId);
        if (pList != null && !pList.isEmpty()) {
            IPartition partition = pList.get(0);
            writer.setFrameSize(partition.getTaskContext().getInitialFrameSize());
            partition.writeTo(writer);
            if (!partition.isReusable()) {
                availablePartitionMap.remove(partitionId);
            }
        } else {
            partitionRequests.put(partitionId, writer);
        }
    }

    public IWorkspaceFileFactory getFileFactory() {
        return fileFactory;
    }

    public void close() {
        deallocatableRegistry.close();
    }

    public synchronized void jobCompleted(JobId jobId, JobStatus status) {
        if (status == JobStatus.FAILURE) {
            failedJobsCache.put(jobId, jobId);
        }
        final List<IPartition> jobPartitions = unregisterPartitions(jobId);
        final List<NetworkOutputChannel> pendingRequests = removePendingRequests(jobId, status);
        if (!jobPartitions.isEmpty() || !pendingRequests.isEmpty()) {
            ncs.getExecutor().execute(() -> {
                jobPartitions.forEach(IDeallocatable::deallocate);
                pendingRequests.forEach(networkOutputChannel -> networkOutputChannel
                        .abort(AbstractChannelWriteInterface.REMOTE_ERROR_CODE));
            });
        }
    }

    public synchronized void jobsCompleted(CcId ccId) {
        failedJobsCache.asMap().keySet().removeIf(jobId -> jobId.getCcId().equals(ccId));
    }

    private void updatePartitionState(CcId ccId, PartitionId pid, TaskAttemptId taId, IPartition partition,
            PartitionState state) throws HyracksDataException {
        PartitionDescriptor desc = new PartitionDescriptor(pid, ncs.getId(), taId, partition.isReusable());
        desc.setState(state);
        try {
            ncs.getClusterController(ccId).registerPartitionProvider(desc);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private List<IPartition> unregisterPartitions(JobId jobId) {
        final List<IPartition> unregisteredPartitions = new ArrayList<>();
        for (Iterator<Map.Entry<PartitionId, List<IPartition>>> i = availablePartitionMap.entrySet().iterator(); i
                .hasNext();) {
            Map.Entry<PartitionId, List<IPartition>> entry = i.next();
            PartitionId pid = entry.getKey();
            if (jobId.equals(pid.getJobId())) {
                unregisteredPartitions.addAll(entry.getValue());
                i.remove();
            }
        }
        return unregisteredPartitions;
    }

    private List<NetworkOutputChannel> removePendingRequests(JobId jobId, JobStatus status) {
        if (status != JobStatus.FAILURE) {
            return Collections.emptyList();
        }
        final List<NetworkOutputChannel> pendingRequests = new ArrayList<>();
        final Iterator<Map.Entry<PartitionId, NetworkOutputChannel>> requestsIterator =
                partitionRequests.entrySet().iterator();
        while (requestsIterator.hasNext()) {
            final Map.Entry<PartitionId, NetworkOutputChannel> entry = requestsIterator.next();
            final PartitionId partitionId = entry.getKey();
            if (partitionId.getJobId().equals(jobId)) {
                pendingRequests.add(entry.getValue());
                requestsIterator.remove();
            }
        }
        return pendingRequests;
    }
}
