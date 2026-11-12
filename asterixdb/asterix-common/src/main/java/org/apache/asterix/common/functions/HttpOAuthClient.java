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

import java.io.Serializable;
import java.util.Objects;

/**
 * An OAuth 2.0 client that an external HTTP function authenticates as, getting its access tokens with the client
 * credentials grant.
 *
 * @param tokenUri the URI of the token endpoint the tokens are requested from
 * @param clientId the client's identifier
 * @param clientSecret the client's secret
 * @param scopes the scope requested, as the space-delimited list that a token endpoint takes, or {@code null} to
 *        request the endpoint's default
 */
public record HttpOAuthClient(String tokenUri, String clientId, String clientSecret, String scopes)
        implements Serializable {

    public HttpOAuthClient {
        Objects.requireNonNull(tokenUri);
        Objects.requireNonNull(clientId);
        Objects.requireNonNull(clientSecret);
    }

    // the generated one would show the secret wherever a client is logged or printed
    @Override
    public String toString() {
        return "HttpOAuthClient[tokenUri=" + tokenUri + ", clientId=" + clientId + ", scopes=" + scopes + "]";
    }
}
