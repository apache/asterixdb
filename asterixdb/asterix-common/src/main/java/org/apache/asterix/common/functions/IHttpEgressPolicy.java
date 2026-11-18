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

import java.net.InetAddress;
import java.net.URI;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Decides where external HTTP functions may send requests. Each request is given to
 * {@link #checkRequest(String, URI)} before it is sent. A request sent directly then has the addresses its host name
 * resolves to given to {@link #checkAddresses(String, InetAddress[])} right before a connection to them is opened, so
 * a host is judged by where it points now rather than when the function was created. A request sent through the
 * proxy of {@link #getProxy()} is resolved by the proxy, so only its host name is judged here; the proxy itself is
 * not judged.
 */
@FunctionalInterface
public interface IHttpEgressPolicy {

    IHttpEgressPolicy ALLOW_ALL = (host, addresses) -> {
    };

    /**
     * @param host the host name of the request
     * @param addresses the addresses the host name resolved to
     * @throws HyracksDataException if no connection may be opened to these addresses. It always fails the query.
     */
    void checkAddresses(String host, InetAddress[] addresses) throws HyracksDataException;

    /**
     * @return the proxy to send each request through, as {@code http://<host>:<port>} or
     *         {@code https://<host>:<port>}, or {@code null} to send requests directly. It is asked for each request,
     *         so a change applies from the next one.
     * @throws HyracksDataException if no request may be sent. It always fails the query.
     */
    default URI getProxy() throws HyracksDataException {
        return null;
    }

    /**
     * @param host the host name of the request
     * @param proxy the proxy {@link #getProxy()} returned for the request, or {@code null} if it is sent directly
     * @throws HyracksDataException if the request may not be sent. It always fails the query.
     */
    default void checkRequest(String host, URI proxy) throws HyracksDataException {
    }
}
