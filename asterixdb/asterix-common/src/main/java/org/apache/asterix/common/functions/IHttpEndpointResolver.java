/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.asterix.common.functions;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Resolves the endpoint of an external HTTP function on a node controller. The default resolver supports only a
 * function that names its URL inline; an extension that stores endpoints elsewhere replaces it.
 */
@FunctionalInterface
public interface IHttpEndpointResolver {

    /**
     * Resolves the endpoint of a function. It is called once per evaluator, before its first request, so a changed
     * endpoint is seen by the next query.
     *
     * @param externalIdentifier the function's external identifier: {@link HttpFunctionConstants#TARGET_URL} and the
     *        URL, or {@link HttpFunctionConstants#TARGET_LINK} and the link's database, dataverse and name
     * @param resources the function's options, keyed by the {@code OPTION_*} names of {@link HttpFunctionConstants}
     * @return the endpoint. For a function that names a link, the URL is the link's, and the caller appends the
     *         function's {@link HttpFunctionConstants#OPTION_PATH path} to it.
     * @throws HyracksDataException if the endpoint cannot be resolved
     */
    HttpEndpoint resolve(List<String> externalIdentifier, Map<String, String> resources) throws HyracksDataException;
}
