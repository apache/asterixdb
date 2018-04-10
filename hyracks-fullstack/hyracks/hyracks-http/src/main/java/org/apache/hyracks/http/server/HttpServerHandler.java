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
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hyracks.http.api.IChannelClosedHandler;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class HttpServerHandler<T extends HttpServer> extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final T server;
    protected final int chunkSize;
    protected HttpRequestHandler handler;
    protected IChannelClosedHandler closeHandler;
    protected Future<Void> task;
    protected IServlet servlet;

    public HttpServerHandler(T server, int chunkSize) {
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
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (handler != null) {
            handler.notifyChannelInactive();
        }
        if (closeHandler != null) {
            closeHandler.channelClosed(server, servlet, task);
        }
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        FullHttpRequest request = (FullHttpRequest) msg;
        handler = null;
        task = null;
        closeHandler = null;
        try {
            servlet = server.getServlet(request);
            if (servlet == null) {
                handleServletNotFound(ctx, request);
            } else {
                submit(ctx, servlet, request);
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Failure Submitting HTTP Request", e);
            respond(ctx, request.protocolVersion(), new HttpResponseStatus(500, e.getMessage()));
        }
    }

    protected void respond(ChannelHandlerContext ctx, HttpVersion httpVersion, HttpResponseStatus status) {
        DefaultHttpResponse response = new DefaultHttpResponse(httpVersion, status);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private void submit(ChannelHandlerContext ctx, IServlet servlet, FullHttpRequest request) throws IOException {
        IServletRequest servletRequest;
        try {
            servletRequest = HttpUtil.toServletRequest(request);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARN, "Failure Decoding Request", e);
            respond(ctx, request.protocolVersion(), HttpResponseStatus.BAD_REQUEST);
            return;
        }
        handler = new HttpRequestHandler(ctx, servlet, servletRequest, chunkSize);
        submit(servlet);
    }

    private void submit(IServlet servlet) throws IOException {
        try {
            task = server.getExecutor(handler).submit(handler);
            closeHandler = servlet.getChannelClosedHandler(server);
        } catch (RejectedExecutionException e) { // NOSONAR
            LOGGER.log(Level.WARN, "Request rejected by server executor service. " + e.getMessage());
            handler.reject();
        }
    }

    protected void handleServletNotFound(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("No servlet for " + request.uri());
        }
        respond(ctx, request.protocolVersion(), HttpResponseStatus.NOT_FOUND);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.log(Level.ERROR, "Failure handling HTTP Request", cause);
        ctx.close();
    }
}
