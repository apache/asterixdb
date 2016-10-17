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

import java.rmi.RemoteException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;

public class PreparePartitionsFailbackRequestMessage extends AbstractFailbackPlanMessage {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PreparePartitionsFailbackRequestMessage.class.getName());
    private final Set<Integer> partitions;
    private boolean releaseMetadataNode = false;
    private final String nodeID;

    public PreparePartitionsFailbackRequestMessage(long planId, int requestId, String nodeId, Set<Integer> partitions) {
        super(planId, requestId);
        this.nodeID = nodeId;
        this.partitions = partitions;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public boolean isReleaseMetadataNode() {
        return releaseMetadataNode;
    }

    public void setReleaseMetadataNode(boolean releaseMetadataNode) {
        this.releaseMetadataNode = releaseMetadataNode;
    }

    public String getNodeID() {
        return nodeID;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(PreparePartitionsFailbackRequestMessage.class.getSimpleName());
        sb.append(" Plan ID: " + planId);
        sb.append(" Partitions: " + partitions);
        sb.append(" releaseMetadataNode: " + releaseMetadataNode);
        return sb.toString();
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException, InterruptedException {
        NodeControllerService ncs = (NodeControllerService) cs;
        IAsterixAppRuntimeContext appContext =
                (IAsterixAppRuntimeContext) ncs.getApplicationContext().getApplicationObject();
        INCMessageBroker broker = (INCMessageBroker) ncs.getApplicationContext().getMessageBroker();
        /**
         * if the metadata partition will be failed back
         * we need to flush and close all datasets including metadata datasets
         * otherwise we need to close all non-metadata datasets and flush metadata datasets
         * so that their memory components will be copied to the failing back node
         */
        if (releaseMetadataNode) {
            appContext.getDatasetLifecycleManager().closeAllDatasets();
            //remove the metadata node stub from RMI registry
            try {
                appContext.unexportMetadataNodeStub();
            } catch (RemoteException e) {
                LOGGER.log(Level.SEVERE, "Failed unexporting metadata stub", e);
                throw ExceptionUtils.convertToHyracksDataException(e);
            }
        } else {
            //close all non-metadata datasets
            appContext.getDatasetLifecycleManager().closeUserDatasets();
            //flush the remaining metadata datasets that were not closed
            appContext.getDatasetLifecycleManager().flushAllDatasets();
        }

        //mark the partitions to be closed as inactive
        PersistentLocalResourceRepository localResourceRepo = (PersistentLocalResourceRepository) appContext
                .getLocalResourceRepository();
        for (Integer partitionId : partitions) {
            localResourceRepo.addInactivePartition(partitionId);
        }

        //send response after partitions prepared for failback
        PreparePartitionsFailbackResponseMessage reponse = new PreparePartitionsFailbackResponseMessage(planId,
                requestId, partitions);
        try {
            broker.sendMessageToCC(reponse);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed sending message to cc", e);
            throw ExceptionUtils.convertToHyracksDataException(e);
        }
    }
}
