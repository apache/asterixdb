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

import java.util.Set;

import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.transactions.IAsterixResourceIdManager;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;

public class ResourceIdRequestMessage implements IApplicationMessage {
    private static final long serialVersionUID = 1L;
    private final String src;

    public ResourceIdRequestMessage(String src) {
        this.src = src;
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException, InterruptedException {
        try {
            ICCMessageBroker broker =
                    (ICCMessageBroker) AsterixAppContextInfo.INSTANCE.getCCApplicationContext().getMessageBroker();
            ResourceIdRequestResponseMessage reponse = new ResourceIdRequestResponseMessage();
            if (!ClusterStateManager.INSTANCE.isClusterActive()) {
                reponse.setResourceId(-1);
                reponse.setException(new Exception("Cannot generate global resource id when cluster is not active."));
            } else {
                IAsterixResourceIdManager resourceIdManager =
                        AsterixAppContextInfo.INSTANCE.getResourceIdManager();
                reponse.setResourceId(resourceIdManager.createResourceId());
                if (reponse.getResourceId() < 0) {
                    reponse.setException(new Exception("One or more nodes has not reported max resource id."));
                }
                requestMaxResourceID(resourceIdManager, broker);
            }
            broker.sendApplicationMessageToNC(reponse, src);
        } catch (Exception e) {
            throw ExceptionUtils.convertToHyracksDataException(e);
        }
    }

    private void requestMaxResourceID(IAsterixResourceIdManager resourceIdManager, ICCMessageBroker broker)
            throws Exception {
        Set<String> getParticipantNodes = ClusterStateManager.INSTANCE.getParticipantNodes();
        ReportMaxResourceIdRequestMessage msg = new ReportMaxResourceIdRequestMessage();
        for (String nodeId : getParticipantNodes) {
            if (!resourceIdManager.reported(nodeId)) {
                broker.sendApplicationMessageToNC(msg, nodeId);
            }
        }
    }

    @Override
    public String toString() {
        return ReportMaxResourceIdRequestMessage.class.getSimpleName();
    }
}
