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

import java.util.Map;
import java.util.Objects;

/**
 * Where an external HTTP function sends its requests, as resolved by an {@link IHttpEndpointResolver}.
 */
public final class HttpEndpoint {

    private final String url;
    private final Map<String, String> headers;
    private final String certificate;

    /**
     * @param url the URL requests are sent to. It may contain the {@code {param}} placeholders that the function's
     *        arguments fill.
     * @param headers headers sent with every request. A function's own headers cannot replace them, so they are
     *        where credentials go.
     * @param certificate the PEM-encoded certificates of the authorities that an HTTPS endpoint's certificate must be
     *        issued by, in place of the JVM's, or {@code null} to use the JVM's
     */
    public HttpEndpoint(String url, Map<String, String> headers, String certificate) {
        this.url = Objects.requireNonNull(url);
        this.headers = Map.copyOf(headers);
        this.certificate = certificate;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getCertificate() {
        return certificate;
    }
}
