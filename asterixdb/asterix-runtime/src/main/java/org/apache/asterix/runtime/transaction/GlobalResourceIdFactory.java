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
package org.apache.asterix.runtime.transaction;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.runtime.message.ResourceIdRequestMessage;
import org.apache.asterix.runtime.message.ResourceIdRequestResponseMessage;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

/**
 * A resource id factory that generates unique resource ids across all NCs by requesting
 * unique ids from the cluster controller.
 */
public class GlobalResourceIdFactory implements IResourceIdFactory {

    private final INCServiceContext serviceCtx;
    private final LinkedBlockingQueue<ResourceIdRequestResponseMessage> resourceIdResponseQ;
    private final String nodeId;

    public GlobalResourceIdFactory(INCServiceContext serviceCtx) {
        this.serviceCtx = serviceCtx;
        this.resourceIdResponseQ = new LinkedBlockingQueue<>();
        this.nodeId = serviceCtx.getNodeId();
    }

    public void addNewIds(ResourceIdRequestResponseMessage resourceIdResponse) throws InterruptedException {
        resourceIdResponseQ.put(resourceIdResponse);
    }

    @Override
    public long createId() throws HyracksDataException {
        try {
            ResourceIdRequestResponseMessage reponse = null;
            //if there already exists a response, use it
            if (!resourceIdResponseQ.isEmpty()) {
                synchronized (resourceIdResponseQ) {
                    if (!resourceIdResponseQ.isEmpty()) {
                        reponse = resourceIdResponseQ.take();
                    }
                }
            }
            //if no response available or it has an exception, request a new one
            if (reponse == null || reponse.getException() != null) {
                ResourceIdRequestMessage msg = new ResourceIdRequestMessage(nodeId);
                ((INCMessageBroker) serviceCtx.getMessageBroker()).sendMessageToPrimaryCC(msg);
                reponse = resourceIdResponseQ.take();
                if (reponse.getException() != null) {
                    throw HyracksDataException.create(reponse.getException());
                }
            }
            return reponse.getResourceId();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
