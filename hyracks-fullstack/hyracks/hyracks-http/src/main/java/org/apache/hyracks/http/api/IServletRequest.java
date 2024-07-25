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
package org.apache.hyracks.http.api;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpScheme;

/**
 * An Http Request instance
 */
public interface IServletRequest {
    /**
     * @return the full http request
     */
    FullHttpRequest getHttpRequest();

    /**
     * Get a request parameter. If there are multiple values, it returns the first one.
     *
     * @param name parameter name
     * @return the parameter or null if not found
     */
    String getParameter(CharSequence name);

    /**
     * Get all values of a request parameter
     *
     * @param name parameter name
     * @return the parameter values or empty list if not found
     */
    List<String> getParameterValues(CharSequence name);

    /**
     * Get the names of all request parameters
     *
     * @return the list of parameter names
     */
    Set<String> getParameterNames();

    /**
     * Get all the request parameters. If there are multiple values for a parameter, it contains the first one.
     *
     * @return the parameters
     */
    Map<String, String> getParameters();

    /**
     * Get all the values of all the request parameters.
     *
     * @return the parameters
     */
    Map<String, List<String>> getParametersValues();

    /**
     * Get a request header
     *
     * @param name header name
     * @return the header or null if not found
     */
    default String getHeader(CharSequence name) {
        return getHttpRequest().headers().get(name);
    }

    /**
     * Get a request header if found, return the default value, otherwise
     *
     * @param name header name
     * @param defaultValue default value
     * @return the header or defaultValue if not found
     */
    default String getHeader(CharSequence name, String defaultValue) {
        String value = getHeader(name);
        return value == null ? defaultValue : value;
    }

    /**
     * Gets the remote address of this request if its channel is connected. Otherwise null.
     *
     * @return the remote address
     */
    InetSocketAddress getRemoteAddress();

    /**
     * Indicates which scheme the client used making this request
     */
    HttpScheme getScheme();

    /**
     * Gets the local address of this request
     *
     * @return the remote address
     */
    InetSocketAddress getLocalAddress();

    Channel getChannel();
}
