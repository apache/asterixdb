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
package org.apache.asterix.runtime.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RequestTracker implements IRequestTracker {

    private final Map<String, IClientRequest> runningRequests = new ConcurrentHashMap<>();
    private final Map<String, IClientRequest> clientIdRequests = new ConcurrentHashMap<>();
    private final CircularFifoQueue<IClientRequest> completedRequests;
    private final ICcApplicationContext ccAppCtx;

    public RequestTracker(ICcApplicationContext ccAppCtx) {
        this.ccAppCtx = ccAppCtx;
        completedRequests = new CircularFifoQueue<>(ccAppCtx.getExternalProperties().getRequestsArchiveSize());
    }

    @Override
    public IClientRequest get(String requestId) {
        return runningRequests.get(requestId);
    }

    @Override
    public IClientRequest getByClientContextId(String clientContextId) {
        Objects.requireNonNull(clientContextId, "clientContextId must not be null");
        return clientIdRequests.get(clientContextId);
    }

    @Override
    public void track(IClientRequest request) {
        runningRequests.put(request.getId(), request);
        if (request.getClientContextId() != null) {
            clientIdRequests.put(request.getClientContextId(), request);
        }
    }

    @Override
    public void cancel(String requestId) throws HyracksDataException {
        final IClientRequest request = runningRequests.get(requestId);
        if (request == null) {
            return;
        }
        if (!request.isCancellable()) {
            throw new IllegalStateException("Request " + request.getId() + " cannot be cancelled");
        }
        cancel(request);
    }

    @Override
    public void complete(String requestId) {
        final IClientRequest request = runningRequests.get(requestId);
        if (request != null) {
            request.complete();
            untrack(request);
        }
    }

    @Override
    public synchronized Collection<IClientRequest> getRunningRequests() {
        return Collections.unmodifiableCollection(runningRequests.values());
    }

    @Override
    public synchronized Collection<IClientRequest> getCompletedRequests() {
        return Collections.unmodifiableCollection(new ArrayList<>(completedRequests));
    }

    private void cancel(IClientRequest request) throws HyracksDataException {
        request.cancel(ccAppCtx);
        untrack(request);
    }

    private void untrack(IClientRequest request) {
        final IClientRequest completedRequest = runningRequests.remove(request.getId());
        if (completedRequest != null) {
            final String clientContextId = completedRequest.getClientContextId();
            if (clientContextId != null) {
                clientIdRequests.remove(completedRequest.getClientContextId());
            }
            archive(completedRequest);
        }
    }

    private synchronized void archive(IClientRequest request) {
        completedRequests.add(request);
    }
}
