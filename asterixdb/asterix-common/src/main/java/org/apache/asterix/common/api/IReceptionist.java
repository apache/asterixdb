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

import java.util.Map;

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
     * @param requestRef
     * @param clientContextId
     * @param statement
     * @param getOptionalParameters
     * @return A client request
     * @throws HyracksDataException
     */
    IClientRequest requestReceived(IRequestReference requestRef, String clientContextId, String statement,
            Map<String, String> getOptionalParameters) throws HyracksDataException;
}
