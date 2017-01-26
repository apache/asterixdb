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

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;

public class ChunkedResponse implements IServletResponse {
    private final ChannelHandlerContext ctx;
    private final ChunkedNettyOutputStream outputStream;
    private final PrintWriter writer;
    private HttpResponse response;
    private boolean headerSent;
    private ByteBuf error;
    private ChannelFuture future;

    public ChunkedResponse(ChannelHandlerContext ctx, FullHttpRequest request) {
        this.ctx = ctx;
        outputStream = new ChunkedNettyOutputStream(ctx, 4096, this);
        writer = new PrintWriter(outputStream);
        response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
    }

    @Override
    public IServletResponse setHeader(CharSequence name, Object value) throws IOException {
        if (headerSent) {
            throw new IOException("Can't add more headers since the initial response was sent");
        }
        response.headers().set(name, value);
        return this;
    }

    @Override
    public ChannelFuture future() {
        return future;
    }

    @Override
    public PrintWriter writer() {
        return writer;
    }

    @Override
    public void close() throws IOException {
        if (error == null) {
            writer.close();
            future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        }
    }

    public HttpResponseStatus status() {
        return response.status();
    }

    public void flush() {
        if (!headerSent && response.status() == HttpResponseStatus.OK) {
            ctx.writeAndFlush(response);
            headerSent = true;
        }
    }

    public void error(ByteBuf error) {
        this.error = error;
    }

    @Override
    public OutputStream outputStream() {
        return outputStream;
    }

    @Override
    public void setStatus(HttpResponseStatus status) {
        // update the response
        // close the stream
        // write the response
    }
}
