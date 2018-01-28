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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PromiseNotificationUtil;

/**
 * A handler to do input control... as a single pipeline can create many requests
 * The remaining capacity of the server executor queue is incremented only when a request has been fully read
 * Therefore, there is a window where requests can be read and get rejected later when they are
 * submitted to the server executor
 */
public class HttpRequestCapacityController extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LogManager.getLogger();
    private final HttpServer server;
    private boolean overloaded = false;

    public HttpRequestCapacityController(HttpServer server) {
        this.server = server;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (overloaded) {
            ReferenceCountUtil.release(msg);
            return;
        }
        if (overloaded()) {
            ReferenceCountUtil.release(msg);
            reject(ctx);
            return;
        } else {
            super.channelRead(ctx, msg);
        }
    }

    public static void reject(ChannelHandlerContext ctx) {
        HttpResponseEncoder encoder = new HttpResponseEncoder();
        ChannelPromise promise = ctx.newPromise();
        promise.addListener(ChannelFutureListener.CLOSE);
        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        try {
            encoder.write(ctx, response, ctx.voidPromise());
            ctx.writeAndFlush(ctx.alloc().buffer(0), promise);
        } catch (Throwable th) {//NOSONAR
            try {
                LOGGER.log(Level.ERROR, "Failure during request rejection", th);
            } catch (Throwable loggingFailure) {//NOSONAR
            }
            PromiseNotificationUtil.tryFailure(promise, th, null);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (overloaded()) {
            reject(ctx);
            return;
        }
        // We disable auto read to avoid reading at all if we can't handle any more requests
        ctx.read();
        super.channelActive(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.read();
        super.channelReadComplete(ctx);
    }

    private boolean overloaded() {
        if (!overloaded) {
            overloaded = server.getExecutor(null).getQueue().remainingCapacity() == 0;
        }
        return overloaded;
    }
}
