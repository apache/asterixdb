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
import java.util.concurrent.Callable;

import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

public class HttpRequestHandler implements Callable<Void> {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ChannelHandlerContext ctx;
    private final IServlet servlet;
    private final IServletRequest request;
    private final IServletResponse response;
    private final HttpServerHandler<?> handler;
    private boolean started = false;
    private boolean cancelled = false;

    public HttpRequestHandler(HttpServerHandler<?> handler, ChannelHandlerContext ctx, IServlet servlet,
            IServletRequest request, int chunkSize) {
        this.handler = handler;
        this.ctx = ctx;
        this.servlet = servlet;
        this.request = request;
        response = chunkSize == 0 ? new FullResponse(handler, ctx, request.getHttpRequest())
                : new ChunkedResponse(handler, ctx, request.getHttpRequest(), chunkSize);
        request.getHttpRequest().retain();
    }

    @Override
    public Void call() throws Exception {
        synchronized (this) {
            if (cancelled) {
                LOGGER.warn("Request cancelled before it is started");
                return null;
            }
            started = true;
        }
        try {
            ChannelFuture lastContentFuture = handle();
            if (!HttpUtil.isKeepAlive(request.getHttpRequest())) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable th) { //NOSONAR
            LOGGER.log(Level.ERROR, "Failure handling HTTP Request", th);
            ctx.close();
        } finally {
            request.getHttpRequest().release();
        }
        return null;
    }

    private ChannelFuture handle() throws IOException {
        try {
            servlet.handle(request, response);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failure during handling of an IServletRequest", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            response.close();
        }
        return response.lastContentFuture();
    }

    public void notifyChannelWritable() {
        response.notifyChannelWritable();
    }

    public void notifyChannelInactive() {
        synchronized (this) {
            if (!started) {
                cancelled = true;
            }
        }
        if (cancelled) {
            // release request and response
            response.cancel();
            request.getHttpRequest().release();
        } else {
            response.notifyChannelInactive();
        }
    }

    public void reject() throws IOException {
        try {
            response.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
            response.close();
        } finally {
            request.getHttpRequest().release();
        }
    }

    public IServlet getServlet() {
        return servlet;
    }

    public IServletRequest getRequest() {
        return request;
    }
}
