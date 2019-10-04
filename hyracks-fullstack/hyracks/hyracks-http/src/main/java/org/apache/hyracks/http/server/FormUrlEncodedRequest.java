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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.QueryStringDecoder;

public class FormUrlEncodedRequest extends BaseRequest implements IServletRequest {

    public static IServletRequest create(ChannelHandlerContext ctx, FullHttpRequest request, HttpScheme scheme) {
        Charset charset = HttpUtil.getRequestCharset(request);
        Map<String, List<String>> parameters = new LinkedHashMap<>();
        URLEncodedUtils.parse(request.content().toString(charset), charset).forEach(
                pair -> parameters.computeIfAbsent(pair.getName(), a -> new ArrayList<>()).add(pair.getValue()));
        new QueryStringDecoder(request.uri()).parameters()
                .forEach((name, value) -> parameters.computeIfAbsent(name, a -> new ArrayList<>()).addAll(value));
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        return new FormUrlEncodedRequest(request, remoteAddress, parameters, scheme);
    }

    private FormUrlEncodedRequest(FullHttpRequest request, InetSocketAddress remoteAddress,
            Map<String, List<String>> parameters, HttpScheme scheme) {
        super(request, remoteAddress, parameters, scheme);
    }
}
