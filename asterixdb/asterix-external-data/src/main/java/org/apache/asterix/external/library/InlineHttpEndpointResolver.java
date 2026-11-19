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
package org.apache.asterix.external.library;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpEndpoint;
import org.apache.asterix.common.functions.HttpFunctionConstants;
import org.apache.asterix.common.functions.IHttpEndpointResolver;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Resolves the endpoint of a function that names its URL inline. It refuses a function that names a link, since
 * only an extension knows where links are stored.
 */
public final class InlineHttpEndpointResolver implements IHttpEndpointResolver {

    public static final InlineHttpEndpointResolver INSTANCE = new InlineHttpEndpointResolver();

    private static final int TARGET_INDEX = 0;
    private static final int URL_INDEX = 1;

    private InlineHttpEndpointResolver() {
    }

    @Override
    public HttpEndpoint resolve(List<String> externalIdentifier, Map<String, String> resources)
            throws HyracksDataException {
        if (HttpFunctionConstants.TARGET_URL.equals(externalIdentifier.get(TARGET_INDEX))) {
            return new HttpEndpoint(externalIdentifier.get(URL_INDEX), Map.of());
        }
        // the link's name is the last element of its identifier
        throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_LINK_UNSUPPORTED,
                externalIdentifier.get(externalIdentifier.size() - 1));
    }
}
