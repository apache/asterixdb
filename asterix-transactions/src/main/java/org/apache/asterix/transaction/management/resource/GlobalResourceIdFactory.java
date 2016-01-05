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
package org.apache.asterix.transaction.management.resource;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.messaging.ResourceIdRequestMessage;
import org.apache.asterix.common.messaging.ResourceIdRequestResponseMessage;
import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.common.messaging.api.IApplicationMessageCallback;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.hyracks.api.application.IApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

/**
 * A resource id factory that generates unique resource ids across all NCs by requesting unique ids from the cluster controller.
 */
public class GlobalResourceIdFactory implements IResourceIdFactory, IApplicationMessageCallback {

    private final IApplicationContext appCtx;
    private final LinkedBlockingQueue<IApplicationMessage> resourceIdResponseQ;

    public GlobalResourceIdFactory(IApplicationContext appCtx) {
        this.appCtx = appCtx;
        this.resourceIdResponseQ = new LinkedBlockingQueue<>();
    }

    @Override
    public long createId() throws HyracksDataException {
        try {
            ResourceIdRequestResponseMessage reponse = null;
            //if there already exists a response, use it
            if (resourceIdResponseQ.size() > 0) {
                synchronized (resourceIdResponseQ) {
                    if (resourceIdResponseQ.size() > 0) {
                        reponse = (ResourceIdRequestResponseMessage) resourceIdResponseQ.take();
                    }
                }
            }
            //if no response available or it has an exception, request a new one
            if (reponse == null || reponse.getException() != null) {
                ResourceIdRequestMessage msg = new ResourceIdRequestMessage();
                ((INCMessageBroker) appCtx.getMessageBroker()).sendMessage(msg, this);
                reponse = (ResourceIdRequestResponseMessage) resourceIdResponseQ.take();
                if (reponse.getException() != null) {
                    throw new HyracksDataException(reponse.getException().getMessage());
                }
            }
            return reponse.getResourceId();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void deliverMessageResponse(IApplicationMessage message) {
        resourceIdResponseQ.offer(message);
    }
}