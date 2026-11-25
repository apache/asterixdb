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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

/**
 * A handler to do input control... as a single pipeline can create many requests
 * The remaining capacity of the server executor queue is incremented only when a request has been fully read
 * Therefore, there is a window where requests can be read and get rejected later when they are
 * submitted to the server executor
 * <p>
 * It sits after the request decoder and the access logger, so that a rejected request is logged like any other.
 * When the queue is full at the start of a request, the request body is read and discarded (up to a cap) before
 * the 503 is sent, and the channel is closed only once the response is written. Closing while the client is
 * still sending the body would make it see a broken pipe instead of the 503.
 */
public class HttpRequestCapacityController extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * The most request body bytes read and discarded before a rejected request is answered
     */
    static final long MAX_DISCARD_BYTES = 64L * 1024 * 1024;
    private static final long LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final AtomicLong lastLogNanos = new AtomicLong(System.nanoTime() - LOG_INTERVAL_NANOS);
    private static final AtomicLong suppressedLogs = new AtomicLong();

    private enum State {
        ACCEPTING,
        DISCARDING,
        REJECTED
    }

    private final HttpServer server;
    private final long maxDiscardBytes;
    private State state = State.ACCEPTING;
    private long discardedBytes;

    public HttpRequestCapacityController(HttpServer server) {
        this(server, Integer.MAX_VALUE);
    }

    public HttpRequestCapacityController(HttpServer server, int maxRequestSize) {
        this.server = server;
        this.maxDiscardBytes = Math.min(maxRequestSize, MAX_DISCARD_BYTES);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (state == State.ACCEPTING && msg instanceof HttpRequest && overloaded()) {
            startRejecting(ctx, (HttpRequest) msg);
        }
        if (state == State.ACCEPTING) {
            super.channelRead(ctx, msg);
            return;
        }
        try {
            if (state == State.DISCARDING && msg instanceof HttpContent) {
                discardedBytes += ((HttpContent) msg).content().readableBytes();
                if (msg instanceof LastHttpContent || discardedBytes > maxDiscardBytes) {
                    reject(ctx);
                }
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void startRejecting(ChannelHandlerContext ctx, HttpRequest request) {
        state = State.DISCARDING;
        discardedBytes = 0;
        server.requestRejected();
        logRejection(ctx, request);
        // answer at once if the body will not follow (100-continue) or is too large to be worth reading
        if (request.decoderResult().isFailure() || HttpUtil.is100ContinueExpected(request)
                || HttpUtil.getContentLength(request, 0L) > maxDiscardBytes) {
            reject(ctx);
        }
    }

    private void reject(ChannelHandlerContext ctx) {
        state = State.REJECTED;
        FullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static void logRejection(ChannelHandlerContext ctx, HttpRequest request) {
        long now = System.nanoTime();
        long last = lastLogNanos.get();
        if (now - last < LOG_INTERVAL_NANOS || !lastLogNanos.compareAndSet(last, now)) {
            suppressedLogs.incrementAndGet();
            return;
        }
        String uri = request.uri();
        int query = uri.indexOf('?');
        LOGGER.log(Level.WARN,
                "Rejecting {} {} from {}: server request queue is full ({} other rejections not logged in the last {}s)",
                request.method(), query < 0 ? uri : uri.substring(0, query), ctx.channel().remoteAddress(),
                suppressedLogs.getAndSet(0), TimeUnit.NANOSECONDS.toSeconds(now - last));
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // We disable auto read to avoid reading at all if we can't handle any more requests
        ctx.read();
        super.channelActive(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (state != State.REJECTED) {
            ctx.read();
        }
        super.channelReadComplete(ctx);
    }

    private boolean overloaded() {
        return server.getExecutor(null).getQueue().remainingCapacity() == 0;
    }
}
