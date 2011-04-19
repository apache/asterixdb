/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IWorkspaceFileFactory;
import edu.uci.ics.hyracks.api.partitions.IPartition;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.io.WorkspaceFileFactory;
import edu.uci.ics.hyracks.control.nc.resources.DefaultDeallocatableRegistry;

public class PartitionManager implements IPartitionRequestListener {
    private final NetworkAddress dataPort;

    private final NodeControllerService ncs;

    private final Map<PartitionId, IPartition> partitionMap;

    private final DefaultDeallocatableRegistry deallocatableRegistry;

    private final IWorkspaceFileFactory fileFactory;

    public PartitionManager(NodeControllerService ncs, NetworkAddress dataPort) {
        this.dataPort = dataPort;
        this.ncs = ncs;
        partitionMap = new HashMap<PartitionId, IPartition>();
        deallocatableRegistry = new DefaultDeallocatableRegistry();
        fileFactory = new WorkspaceFileFactory(deallocatableRegistry, (IOManager) ncs.getRootContext().getIOManager());
    }

    public void registerPartition(PartitionId pid, IPartition partition) throws HyracksDataException {
        synchronized (this) {
            partitionMap.put(pid, partition);
        }
        try {
            ncs.getClusterController().registerPartitionProvider(pid, dataPort);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public synchronized IPartition getPartition(PartitionId pid) {
        return partitionMap.get(pid);
    }

    public synchronized void unregisterPartitions(UUID jobId) {
        for (Iterator<Map.Entry<PartitionId, IPartition>> i = partitionMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry<PartitionId, IPartition> e = i.next();
            PartitionId pid = e.getKey();
            if (jobId.equals(pid.getJobId())) {
                e.getValue().deallocate();
                i.remove();
            }
        }
    }

    @Override
    public synchronized void registerPartitionRequest(PartitionId partitionId, IFrameWriter writer)
            throws HyracksException {
        IPartition partition = partitionMap.get(partitionId);
        if (partition != null) {
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