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

import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.api.IServletRequest;

public interface IReceptionist {

    /**
     * Generates a request reference based on {@code request}
     *
     * @param request
     * @return a request reference representing the request
     */
    IRequestReference welcome(IServletRequest request);

    /**
     * Generates a {@link IClientRequest} based on the requests parameters
     *
     * @param requestParameters the request parameters
     * @return the client request
     * @throws HyracksDataException HyracksDataException
     */
    IClientRequest requestReceived(ICommonRequestParameters requestParameters) throws HyracksDataException;

    /**
     * Ensures a client's request can be executed before its job is started
     *
     * @param schedulableRequest
     * @throws HyracksDataException
     */
    void ensureSchedulable(ISchedulableClientRequest schedulableRequest) throws HyracksDataException;

    /**
     * Ensures a client's request is authorized
     *
     * @param requestParameters
     * @param metadataProvider
     * @throws HyracksDataException
     */
    void ensureAuthorized(ICommonRequestParameters requestParameters, IMetadataProvider<?, ?> metadataProvider)
            throws HyracksDataException;
}
