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

import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.runtime.message.ResourceIdRequestMessage;
import org.apache.asterix.runtime.message.ResourceIdRequestResponseMessage;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueues;

/**
 * A resource id factory that generates unique resource ids across all NCs by requesting
 * unique ids from the cluster controller.
 */
public class GlobalResourceIdFactory implements IResourceIdFactory {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int RESOURCE_ID_BLOCK_SIZE = 25;
    private final INCServiceContext serviceCtx;
    private final LongPriorityQueue resourceIds =
            LongPriorityQueues.synchronize(new LongArrayFIFOQueue(RESOURCE_ID_BLOCK_SIZE));
    private final LinkedBlockingQueue<ResourceIdRequestResponseMessage> resourceIdResponseQ;
    private final String nodeId;

    public GlobalResourceIdFactory(INCServiceContext serviceCtx) {
        this.serviceCtx = serviceCtx;
        this.resourceIdResponseQ = new LinkedBlockingQueue<>();
        this.nodeId = serviceCtx.getNodeId();
    }

    public void addNewIds(ResourceIdRequestResponseMessage resourceIdResponse) throws InterruptedException {
        LOGGER.debug("rec'd block of ids: {}", resourceIdResponse);
        resourceIdResponseQ.put(resourceIdResponse);
    }

    @Override
    public long createId() throws HyracksDataException {
        try {
            final long resourceId = resourceIds.dequeueLong();
            if (resourceIds.isEmpty()) {
                serviceCtx.getControllerService().getExecutor().submit(() -> {
                    try {
                        requestNewBlock();
                    } catch (Exception e) {
                        LOGGER.warn("failed on preemptive block request", e);
                    }
                });
            }
            return resourceId;
        } catch (NoSuchElementException e) {
            // fallthrough
        }
        try {
            // if there already exists a response, use it
            ResourceIdRequestResponseMessage response = resourceIdResponseQ.poll();
            if (response == null) {
                requestNewBlock();
                response = resourceIdResponseQ.take();
            }
            if (response.getException() != null) {
                throw HyracksDataException.create(response.getException());
            }
            // take the first id, queue the rest
            final long startingId = response.getResourceId();
            for (int i = 1; i < response.getBlockSize(); i++) {
                resourceIds.enqueue(startingId + i);
            }
            return startingId;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void requestNewBlock() throws Exception {
        // queue is empty; request a new block
        ResourceIdRequestMessage msg = new ResourceIdRequestMessage(nodeId, RESOURCE_ID_BLOCK_SIZE);
        ((INCMessageBroker) serviceCtx.getMessageBroker()).sendMessageToPrimaryCC(msg);
    }
}
