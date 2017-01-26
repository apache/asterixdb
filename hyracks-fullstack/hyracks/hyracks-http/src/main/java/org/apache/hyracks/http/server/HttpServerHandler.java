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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = Logger.getLogger(HttpServerHandler.class.getName());
    protected final HttpServer server;

    public HttpServerHandler(HttpServer server) {
        this.server = server;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        try {
            FullHttpRequest http = (FullHttpRequest) msg;
            IServlet servlet = server.getServlet(http);
            if (servlet == null) {
                DefaultHttpResponse response = new DefaultHttpResponse(http.protocolVersion(),
                        HttpResponseStatus.NOT_FOUND);
                ctx.write(response).addListener(ChannelFutureListener.CLOSE);
            } else {
                if (http.method() != HttpMethod.GET && http.method() != HttpMethod.POST) {
                    DefaultHttpResponse response = new DefaultHttpResponse(http.protocolVersion(),
                            HttpResponseStatus.METHOD_NOT_ALLOWED);
                    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                    return;
                }
                IServletRequest request = http.method() == HttpMethod.GET ? get(http) : post(http);
                IServletResponse response = new FullResponse(ctx, http);
                try {
                    servlet.handle(request, response);
                } catch (Throwable th) { // NOSONAR
                    LOGGER.log(Level.WARNING, "Failure during handling of an IServLetRequest", th);
                    response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                } finally {
                    response.close();
                }
                ChannelFuture lastContentFuture = response.future();
                if (!HttpUtil.isKeepAlive(http)) {
                    lastContentFuture.addListener(ChannelFutureListener.CLOSE);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure handling HTTP Request", e);
            ctx.close();
        }
    }

    public static IServletRequest post(FullHttpRequest request) throws IOException {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        HttpPostRequestDecoder decoder = null;
        try {
            decoder = new HttpPostRequestDecoder(request);
        } catch (Exception e) {
            //ignore. this means that the body of the POST request does not have key value pairs
            LOGGER.log(Level.WARNING, "Failed to decode a post message. Fix the API not to have queries as POST body",
                    e);
        }
        if (decoder != null) {
            try {
                List<InterfaceHttpData> bodyHttpDatas = decoder.getBodyHttpDatas();
                for (InterfaceHttpData data : bodyHttpDatas) {
                    if (data.getHttpDataType().equals(HttpDataType.Attribute)) {
                        Attribute attr = (MixedAttribute) data;
                        names.add(data.getName());
                        values.add(attr.getValue());
                    }
                }
            } finally {
                decoder.destroy();
            }
        }
        return new PostRequest(request, new QueryStringDecoder(request.uri()).parameters(), names, values);
    }

    public static IServletRequest get(FullHttpRequest request) throws IOException {
        return new GetRequest(request, new QueryStringDecoder(request.uri()).parameters());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.log(Level.SEVERE, "Failure handling HTTP Request", cause);
        ctx.close();
    }
}
