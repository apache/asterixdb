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

import static org.apache.hyracks.http.server.utils.HttpUtil.X_FORWARDED_PROTO;

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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

public class HttpServerHandler<T extends HttpServer> extends SimpleChannelInboundHandler<Object>
        implements ChannelFutureListener {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String PIPELINED_REQUEST_ERROR_MSG = "Server doesn't support pipelined requests";
    protected final T server;
    protected volatile HttpRequestHandler handler;
    protected volatile Future<Void> task;
    protected volatile IServlet servlet;
    private volatile IChannelClosedHandler closeHandler;
    private volatile boolean pipelinedRequest = false;
    private final int chunkSize;

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
        final HttpRequestHandler currentHandler = handler;
        if (currentHandler != null && ctx.channel().isWritable()) {
            currentHandler.notifyChannelWritable();
        }
        super.channelWritabilityChanged(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final HttpRequestHandler currentHandler = handler;
        if (currentHandler != null) {
            currentHandler.notifyChannelInactive();
        }
        final IChannelClosedHandler currentCloseHandler = closeHandler;
        final IServlet currentServlet = servlet;
        final Future<Void> currentTask = task;
        if (currentCloseHandler != null && currentServlet != null && currentTask != null) {
            currentCloseHandler.channelClosed(server, currentServlet, currentTask);
        }
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        FullHttpRequest request = (FullHttpRequest) msg;
        if (isPipelinedRequest()) {
            pipelinedRequest = true;
            rejectPipelinedRequestAndClose(ctx, request);
            return;
        }
        if (request.decoderResult().isFailure()) {
            respond(ctx, request, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        try {
            servlet = server.getServlet(request);
            if (servlet == null) {
                handleServletNotFound(ctx, request);
            } else {
                submit(ctx, servlet, request);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failure handling HTTP request", e);
            respond(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    protected void respond(ChannelHandlerContext ctx, HttpRequest request, HttpResponseStatus status) {
        final DefaultHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), status);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        HttpUtil.setConnectionHeader(request, response);
        final ChannelPromise responseCompletionPromise = ctx.newPromise();
        responseCompletionPromise.addListener(this);
        final ChannelFuture clientChannel = ctx.writeAndFlush(response, responseCompletionPromise);
        if (!io.netty.handler.codec.http.HttpUtil.isKeepAlive(request)) {
            clientChannel.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void submit(ChannelHandlerContext ctx, IServlet servlet, FullHttpRequest request) throws IOException {
        IServletRequest servletRequest;
        try {
            HttpScheme scheme =
                    server.getScheme() == HttpScheme.HTTPS || "https".equals(request.headers().get(X_FORWARDED_PROTO))
                            ? HttpScheme.HTTPS : HttpScheme.HTTP;
            servletRequest = HttpUtil.toServletRequest(ctx, request, scheme);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.WARN, "Failure Decoding Request", e);
            respond(ctx, request, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        handler = new HttpRequestHandler(this, ctx, servlet, servletRequest, chunkSize);
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("No servlet for " + request.uri());
        }
        respond(ctx, request, HttpResponseStatus.NOT_FOUND);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.log(Level.WARN, "Failure handling HTTP Request", cause);
        ctx.close();
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (!pipelinedRequest) {
            requestHandled();
        }
    }

    private boolean isPipelinedRequest() {
        return handler != null || servlet != null || closeHandler != null || task != null;
    }

    private void rejectPipelinedRequestAndClose(ChannelHandlerContext ctx, FullHttpRequest request) {
        LOGGER.warn(PIPELINED_REQUEST_ERROR_MSG);
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        respond(ctx, request,
                new HttpResponseStatus(HttpResponseStatus.BAD_REQUEST.code(), PIPELINED_REQUEST_ERROR_MSG));
    }

    private void requestHandled() {
        handler = null;
        servlet = null;
        task = null;
        closeHandler = null;
    }
}
