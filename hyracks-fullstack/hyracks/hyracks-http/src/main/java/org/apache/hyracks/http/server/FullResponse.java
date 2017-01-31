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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.hyracks.http.api.IServletResponse;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

public class FullResponse implements IServletResponse {
    private final ChannelHandlerContext ctx;
    private final ByteArrayOutputStream baos;
    private final PrintWriter writer;
    private final FullHttpResponse response;
    private final boolean keepAlive;
    private ChannelFuture future;

    public FullResponse(ChannelHandlerContext ctx, FullHttpRequest request) {
        this.ctx = ctx;
        baos = new ByteArrayOutputStream();
        writer = new PrintWriter(baos);
        response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
        FullHttpResponse fullResponse = response.replace(Unpooled.copiedBuffer(baos.toByteArray()));
        if (keepAlive && response.status() == HttpResponseStatus.OK) {
            fullResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, fullResponse.content().readableBytes());
        }
        future = ctx.writeAndFlush(fullResponse);
        if (response.status() != HttpResponseStatus.OK) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public IServletResponse setHeader(CharSequence name, Object value) throws IOException {
        response.headers().set(name, value);
        return this;
    }

    @Override
    public PrintWriter writer() {
        return writer;
    }

    @Override
    public ChannelFuture lastContentFuture() throws IOException {
        return future;
    }

    @Override
    public OutputStream outputStream() {
        return baos;
    }

    @Override
    public void setStatus(HttpResponseStatus status) {
        response.setStatus(status);
    }

    @Override
    public void notifyChannelWritable() {
        // Do nothing.
        // This response is sent as a single piece
    }
}
