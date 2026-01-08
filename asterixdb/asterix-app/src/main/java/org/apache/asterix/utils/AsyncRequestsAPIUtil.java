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
package org.apache.asterix.utils;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.asterix.app.message.ClientInfoRequestMessage;
import org.apache.asterix.app.message.ClientInfoResponseMessage;
import org.apache.asterix.app.message.DiscardResultPartitionRequestMessage;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultJobRecord;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.result.IResultDirectoryService;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class AsyncRequestsAPIUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long RESULT_PARTITIONS_FETCH_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(1);
    public static final long NC_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    /**
     * Discards result partitions across all nodes and cleans up tracking information.
     *
     * @param appCtx Application context
     * @param jobId Job identifier
     * @param resultSetId Result set identifier
     * @param requestId Optional request identifier for cleanup
     * @throws HyracksDataException if discard operation fails
     */
    public static void discardResultPartitions(ICcApplicationContext appCtx, JobId jobId, ResultSetId resultSetId,
            String requestId) throws HyracksDataException {
        IResultDirectoryService resultDirectoryService =
                ((ClusterControllerService) appCtx.getServiceContext().getControllerService())
                        .getResultDirectoryService();;
        // Check if result is in a valid state for discarding
        ResultJobRecord.Status status = resultDirectoryService.getResultStatus(jobId, resultSetId);
        if (status.getState() != ResultJobRecord.State.SUCCESS) {
            LOGGER.log(Level.WARN, "Cannot discard result for job {}, result set {}, request {} - status is {}", jobId,
                    resultSetId, requestId, status);
            return;
        }

        // Send discard result messages to all nodes containing result partitions
        Set<String> nodeIds = fetchResultNodeIds(resultDirectoryService, jobId, resultSetId);
        ICCMessageBroker messageBroker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        for (String nodeId : nodeIds) {
            DiscardResultPartitionRequestMessage message = new DiscardResultPartitionRequestMessage(jobId);
            try {
                messageBroker.sendRealTimeApplicationMessageToNC(message, nodeId);
            } catch (Exception e) {
                throw new ACIDException("Failed to send discard message to node " + nodeId, e);
            }
        }

        // Clean up result directory and request tracking
        resultDirectoryService.sweep(jobId);
        if (requestId != null) {
            appCtx.getRequestTracker().removeAsyncOrDeferredRequest(requestId);
        }
    }

    private static Set<String> fetchResultNodeIds(IResultDirectoryService rds, JobId jobId, ResultSetId resultSetId)
            throws HyracksDataException {
        CompletableFuture<ResultDirectoryRecord[]> future = new CompletableFuture<>();
        rds.getResultPartitionLocations(jobId, resultSetId, null, new IResultCallback<>() {
            @Override
            public void setValue(ResultDirectoryRecord[] result) {
                future.complete(result);
            }

            @Override
            public void setException(Exception e) {
                LOGGER.log(org.apache.logging.log4j.Level.WARN, "Failed to get result partition locations", e);
                future.completeExceptionally(e);
            }
        });
        ResultDirectoryRecord[] records;
        try {
            records = future.get(RESULT_PARTITIONS_FETCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw HyracksDataException.create(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
        return Arrays.stream(records).filter(Objects::nonNull).map(ResultDirectoryRecord::getNodeId)
                .filter(Objects::nonNull).collect(LinkedHashSet::new, Set::add, Set::addAll);
    }

    public static boolean isValidRequest(IApplicationContext appCtx, String requestId, JobId jobId,
            IServletResponse response) throws HyracksDataException {
        INCServiceContext serviceCtx = (INCServiceContext) appCtx.getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();
        ClientInfoRequestMessage clientInfoRequestMessage =
                new ClientInfoRequestMessage(serviceCtx.getNodeId(), futureId, jobId, requestId, false);
        try {
            messageBroker.sendMessageToPrimaryCC(clientInfoRequestMessage);
            ClientInfoResponseMessage responseMessage =
                    (ClientInfoResponseMessage) messageFuture.get(NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (responseMessage == null || !responseMessage.isValidRequestId()) {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return false;
            }
            return true;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
