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
package edu.uci.ics.hyracks.control.nc.partitions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.comm.channels.NetworkOutputChannel;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionState;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class PartitionManager {
    private final NodeControllerService ncs;

    private final Map<PartitionId, List<IPartition>> partitionMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    public PartitionManager(NodeControllerService ncs) {
        this.ncs = ncs;
        partitionMap = new HashMap<PartitionId, List<IPartition>>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(deallocatableRegistry, (IOManager) ncs.getRootContext().getIOManager());
    }

    public void registerPartition(PartitionId pid, TaskAttemptId taId, IPartition partition, PartitionState state)
            throws HyracksDataException {
        synchronized (this) {
            List<IPartition> pList = partitionMap.get(pid);
            if (pList == null) {
                pList = new ArrayList<IPartition>();
                partitionMap.put(pid, pList);
            }
            pList.add(partition);
        }
        updatePartitionState(pid, taId, partition, state);
    }

    public void updatePartitionState(PartitionId pid, TaskAttemptId taId, IPartition partition, PartitionState state)
            throws HyracksDataException {
        PartitionDescriptor desc = new PartitionDescriptor(pid, ncs.getId(), taId, partition.isReusable());
        desc.setState(state);
        try {
            ncs.getClusterController().registerPartitionProvider(desc);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public synchronized IPartition getPartition(PartitionId pid) {
        return partitionMap.get(pid).get(0);
    }

    public synchronized void unregisterPartitions(JobId jobId, Collection<IPartition> unregisteredPartitions) {
        for (Iterator<Map.Entry<PartitionId, List<IPartition>>> i = partitionMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry<PartitionId, List<IPartition>> e = i.next();
            PartitionId pid = e.getKey();
            if (jobId.equals(pid.getJobId())) {
                for (IPartition p : e.getValue()) {
                    unregisteredPartitions.add(p);
                }
                i.remove();
            }
        }
    }

    public synchronized void registerPartitionRequest(PartitionId partitionId, NetworkOutputChannel writer)
            throws HyracksException {
        List<IPartition> pList = partitionMap.get(partitionId);
        if (pList != null && !pList.isEmpty()) {
            IPartition partition = pList.get(0);
            writer.setFrameSize(partition.getTaskContext().getFrameSize());
            partition.writeTo(writer);
            if (!partition.isReusable()) {
                partitionMap.remove(partitionId);
            }
        } else {
            throw new HyracksException("Request for unknown partition " + partitionId);
        }
    }

    public IWorkspaceFileFactory getFileFactory() {
        return fileFactory;
    }

    public void close() {
        deallocatableRegistry.close();
    }
}