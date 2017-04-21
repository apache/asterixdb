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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.NodeControllerService;

public class ReportMaxResourceIdMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ReportMaxResourceIdMessage.class.getName());
    private final long maxResourceId;
    private final String src;

    public ReportMaxResourceIdMessage(String src, long maxResourceId) {
        this.src = src;
        this.maxResourceId = maxResourceId;
    }

    public long getMaxResourceId() {
        return maxResourceId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        IResourceIdManager resourceIdManager = appCtx.getResourceIdManager();
        resourceIdManager.report(src, maxResourceId);
    }

    public static void send(NodeControllerService cs) throws HyracksDataException {
        NodeControllerService ncs = cs;
        INcApplicationContext appContext = (INcApplicationContext) ncs.getApplicationContext();
        long maxResourceId = Math.max(appContext.getLocalResourceRepository().maxId(),
                MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID);
        ReportMaxResourceIdMessage maxResourceIdMsg = new ReportMaxResourceIdMessage(ncs.getId(), maxResourceId);
        try {
            ((INCMessageBroker) ncs.getContext().getMessageBroker()).sendMessageToCC(maxResourceIdMsg);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unable to report max local resource id", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        return ReportMaxResourceIdMessage.class.getSimpleName();
    }
}
