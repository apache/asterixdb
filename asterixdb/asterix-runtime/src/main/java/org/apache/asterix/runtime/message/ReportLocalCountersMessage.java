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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReportLocalCountersMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final long maxResourceId;
    private final long maxTxnId;
    private final long maxJobId;
    private final String src;

    public ReportLocalCountersMessage(String src, long maxResourceId, long maxTxnId, long maxJobId) {
        this.src = src;
        this.maxResourceId = maxResourceId;
        this.maxTxnId = maxTxnId;
        this.maxJobId = maxJobId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        IResourceIdManager resourceIdManager = appCtx.getResourceIdManager();
        try {
            appCtx.getTxnIdFactory().ensureMinimumId(maxTxnId);
        } catch (AlgebricksException e) {
            throw HyracksDataException.create(e);
        }
        resourceIdManager.report(src, maxResourceId);
        ((ClusterControllerService) appCtx.getServiceContext().getControllerService()).getJobIdFactory()
                .setMaxJobId(maxJobId);
    }

    public static void send(CcId ccId, NodeControllerService ncs) throws HyracksDataException {
        INcApplicationContext appContext = (INcApplicationContext) ncs.getApplicationContext();
        long maxResourceId = Math.max(appContext.getLocalResourceRepository().maxId(),
                MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID);
        long maxTxnId = appContext.getMaxTxnId();
        long maxJobId = ncs.getMaxJobId(ccId);
        ReportLocalCountersMessage countersMessage =
                new ReportLocalCountersMessage(ncs.getId(), maxResourceId, maxTxnId, maxJobId);
        try {
            ((INCMessageBroker) ncs.getContext().getMessageBroker()).sendMessageToCC(ccId, countersMessage);
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Unable to report local counters", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        return ReportLocalCountersMessage.class.getSimpleName();
    }
}
