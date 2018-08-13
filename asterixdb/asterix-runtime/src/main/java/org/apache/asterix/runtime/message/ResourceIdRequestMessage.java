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

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ResourceIdRequestMessage implements ICcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private final String src;

    public ResourceIdRequestMessage(String src) {
        this.src = src;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        try {
            ICCMessageBroker broker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
            ResourceIdRequestResponseMessage response = new ResourceIdRequestResponseMessage();
            IClusterStateManager clusterStateManager = appCtx.getClusterStateManager();
            IResourceIdManager resourceIdManager = appCtx.getResourceIdManager();
            response.setResourceId(resourceIdManager.createResourceId());
            if (response.getResourceId() < 0) {
                if (!(clusterStateManager.isClusterActive())) {
                    response.setException(
                            new Exception("Cannot generate global resource id when cluster is not active."));
                } else {
                    response.setException(new Exception("One or more nodes has not reported max resource id."));
                }
            }
            broker.sendApplicationMessageToNC(response, src);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        return ResourceIdRequestMessage.class.getSimpleName();
    }
}
