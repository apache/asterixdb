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
package org.apache.hyracks.http.server;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.QueryStringDecoder;

public class BaseRequest implements IServletRequest {
    protected final FullHttpRequest request;
    protected final Map<String, List<String>> parameters;
    protected final InetSocketAddress remoteAddress;
    protected final HttpScheme scheme;

    public static IServletRequest create(ChannelHandlerContext ctx, FullHttpRequest request, HttpScheme scheme) {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> param = decoder.parameters();
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        return new BaseRequest(request, remoteAddress, param, scheme);
    }

    protected BaseRequest(FullHttpRequest request, InetSocketAddress remoteAddress,
            Map<String, List<String>> parameters, HttpScheme scheme) {
        this.request = request;
        this.remoteAddress = remoteAddress;
        this.parameters = parameters;
        this.scheme = scheme;
    }

    @Override
    public FullHttpRequest getHttpRequest() {
        return request;
    }

    @Override
    public String getParameter(CharSequence name) {
        return HttpUtil.getParameter(parameters, name);
    }

    @Override
    public Set<String> getParameterNames() {
        return Collections.unmodifiableSet(parameters.keySet());
    }

    @Override
    public Map<String, String> getParameters() {
        HashMap<String, String> paramMap = new HashMap<>();
        for (String name : parameters.keySet()) {
            paramMap.put(name, HttpUtil.getParameter(parameters, name));

        }
        return Collections.unmodifiableMap(paramMap);
    }

    @Override
    public String getHeader(CharSequence name) {
        return request.headers().get(name);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public HttpScheme getScheme() {
        return scheme;
    }
}
