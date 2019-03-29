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
package org.apache.asterix.common.api;

import java.util.Collection;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IRequestTracker {

    /**
     * Starts tracking {@code request}
     *
     * @param request
     */
    void track(IClientRequest request);

    /**
     * Gets a client request by {@code requestId}
     *
     * @param requestId
     * @return the client request if found. Otherwise null.
     */
    IClientRequest get(String requestId);

    /**
     * Gets a client request by {@code clientContextId}
     *
     * @param clientContextId
     * @return the client request if found. Otherwise null.
     */
    IClientRequest getByClientContextId(String clientContextId);

    /**
     * Cancels the client request with id {@code requestId} if found.
     *
     * @param requestId
     * @throws HyracksDataException
     */
    void cancel(String requestId) throws HyracksDataException;

    /**
     * Completes the request with id {@code requestId}
     *
     * @param requestId
     */
    void complete(String requestId);

    /**
     * Gets the currently running requests
     *
     * @return the currently running requests
     */
    Collection<IClientRequest> getRunningRequests();

    /**
     * Gets the recently completed requests
     *
     * @return the recently completed requests
     */
    Collection<IClientRequest> getCompletedRequests();
}
