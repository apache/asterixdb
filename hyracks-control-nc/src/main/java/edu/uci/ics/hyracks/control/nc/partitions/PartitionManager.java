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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
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

    public void registerPartition(PartitionId pid, IPartition partition) throws HyracksDataException {
        synchronized (this) {
            List<IPartition> pList = partitionMap.get(pid);
            if (pList == null) {
                pList = new ArrayList<IPartition>();
                partitionMap.put(pid, pList);
            }
            pList.add(partition);
        }
        try {
            ncs.getClusterController().registerPartitionProvider(pid, ncs.getId());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public synchronized IPartition getPartition(PartitionId pid) {
        return partitionMap.get(pid).get(0);
    }

    public synchronized void unregisterPartitions(UUID jobId) {
        for (Iterator<Map.Entry<PartitionId, List<IPartition>>> i = partitionMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry<PartitionId, List<IPartition>> e = i.next();
            PartitionId pid = e.getKey();
            if (jobId.equals(pid.getJobId())) {
                for (IPartition p : e.getValue()) {
                    p.deallocate();
                }
                i.remove();
            }
        }
    }

    @Override
    public synchronized void registerPartitionRequest(PartitionId partitionId, IFrameWriter writer)
            throws HyracksException {
        List<IPartition> pList = partitionMap.get(partitionId);
        if (pList != null && !pList.isEmpty()) {
            IPartition partition = pList.get(0);
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