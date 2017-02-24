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
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = Logger.getLogger(HttpServerHandler.class.getName());
    protected final HttpServer server;
    protected final int chunkSize;
    protected HttpRequestHandler handler;

    public HttpServerHandler(HttpServer server, int chunkSize) {
        this.server = server;
        this.chunkSize = chunkSize;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            handler.notifyChannelWritable();
        }
        super.channelWritabilityChanged(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        try {
            FullHttpRequest request = (FullHttpRequest) msg;
            IServlet servlet = server.getServlet(request);
            if (servlet == null) {
                DefaultHttpResponse notFound =
                        new DefaultHttpResponse(request.protocolVersion(), HttpResponseStatus.NOT_FOUND);
                ctx.write(notFound).addListener(ChannelFutureListener.CLOSE);
            } else {
                handler = new HttpRequestHandler(ctx, servlet, HttpUtil.toServletRequest(request), chunkSize);
                submit();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failure handling HTTP Request", e);
            ctx.close();
        }
    }

    private void submit() throws IOException {
        try {
            server.getExecutor().submit(handler);
        } catch (RejectedExecutionException e) { // NOSONAR
            LOGGER.log(Level.WARNING, "Request rejected by server executor service. " + e.getMessage());
            handler.reject();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.log(Level.SEVERE, "Failure handling HTTP Request", cause);
        ctx.close();
    }
}
