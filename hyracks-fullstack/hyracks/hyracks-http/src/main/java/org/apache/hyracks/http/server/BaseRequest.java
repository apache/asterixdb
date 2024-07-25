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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.http.api.IServletRequest;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.QueryStringDecoder;

public class BaseRequest implements IServletRequest {

    private static final List<String> NO_PARAM = Collections.singletonList(null);
    private final Channel channel;
    protected final FullHttpRequest request;
    protected final Map<? extends CharSequence, List<String>> parameters;
    protected final HttpScheme scheme;

    public static IServletRequest create(ChannelHandlerContext ctx, FullHttpRequest request, HttpScheme scheme,
            boolean ignoreQueryParameters) {
        Map<? extends CharSequence, List<String>> param =
                ignoreQueryParameters ? Collections.emptyMap() : new QueryStringDecoder(request.uri()).parameters();
        return new BaseRequest(ctx.channel(), request, param, scheme);
    }

    protected BaseRequest(Channel channel, FullHttpRequest request,
            Map<? extends CharSequence, List<String>> parameters, HttpScheme scheme) {
        this.channel = channel;
        this.request = request;
        this.parameters = parameters;
        this.scheme = scheme;
    }

    @Override
    public FullHttpRequest getHttpRequest() {
        return request;
    }

    @Override
    public String getParameter(CharSequence name) {
        return parameters.getOrDefault(name, NO_PARAM).get(0);
    }

    @Override
    public List<String> getParameterValues(CharSequence name) {
        return Collections.unmodifiableList(parameters.getOrDefault(name, Collections.emptyList()));
    }

    @Override
    public Set<String> getParameterNames() {
        Set<String> names = new HashSet<>();
        parameters.keySet().forEach(name -> names.add(name.toString()));
        return names;
    }

    @Override
    public Map<String, String> getParameters() {
        HashMap<String, String> paramMap = new HashMap<>();
        parameters.forEach((name, values) -> paramMap.put(name.toString(), values.get(0)));
        return paramMap;
    }

    @Override
    public Map<String, List<String>> getParametersValues() {
        Map<String, List<String>> params = new HashMap<>();
        parameters.forEach((name, values) -> params.put(name.toString(), new ArrayList<>(values)));
        return params;
    }

    @Override
    public String getHeader(CharSequence name) {
        return request.headers().get(name);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public HttpScheme getScheme() {
        return scheme;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public Channel getChannel() {
        return channel;
    }
}
