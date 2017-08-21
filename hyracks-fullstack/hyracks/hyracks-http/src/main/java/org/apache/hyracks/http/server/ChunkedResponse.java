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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServletResponse;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

/**
 * A chunked http response. Here is how it is expected to work:
 * If the response is a success aka 200 and is less than chunkSize, then it is sent as a single http response message
 * If the response is larger than the chunkSize and the response status is 200, then it is sent as chunks of chunkSize.
 * If the response status is non 200, then it is always sent as a single http response message.
 * If the response status is non 200, then output buffered before setting the response status is discarded.
 * If flush() is called on the writer and even if it is less than chunkSize, then the initial response will be sent
 * with headers, followed by the buffered bytes as the first chunk.
 * When chunking, an output buffer is allocated only when the previous buffer has been sent
 * If an error occurs after sending the first chunk, the connection will close abruptly.
 *
 * Here is a breakdown of the possible cases.
 * 1. smaller than chunkSize, no error -> full response
 * 2. smaller than chunkSize, error -> full response
 * 3. larger than chunkSize, error after header-> close connection. release buffer and release error
 * 4. larger than chunkSize, no error. -> header, data, empty response
 */
public class ChunkedResponse implements IServletResponse {

    private static final Logger LOGGER = Logger.getLogger(ChunkedResponse.class.getName());
    private final ChannelHandlerContext ctx;
    private final ChunkedNettyOutputStream outputStream;
    private final PrintWriter writer;
    private HttpResponse response;
    private boolean headerSent;
    private ByteBuf error;
    private ChannelFuture future;
    private boolean done;
    private final boolean keepAlive;

    public ChunkedResponse(ChannelHandlerContext ctx, FullHttpRequest request, int chunkSize) {
        this.ctx = ctx;
        outputStream = new ChunkedNettyOutputStream(ctx, chunkSize, this);
        writer = new PrintWriter(outputStream);
        response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive) {
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
    public ChannelFuture lastContentFuture() {
        return future;
    }

    @Override
    public PrintWriter writer() {
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        if (error == null && response.status() == HttpResponseStatus.OK) {
            if (!done) {
                future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            }
        } else {
            // There was an error
            if (headerSent) {
                LOGGER.log(Level.WARNING, "Error after header write of chunked response");
                if (error != null) {
                    error.release();
                }
                future = ctx.channel().close();
            } else {
                if (keepAlive && response.status() != HttpResponseStatus.UNAUTHORIZED) {
                    response.headers().remove(HttpHeaderNames.CONNECTION);
                }
                // we didn't send anything to the user, we need to send an unchunked error response
                fullResponse(response.protocolVersion(), response.status(),
                        error == null ? ctx.alloc().buffer(0, 0) : error, response.headers());
            }
            if (response.status() != HttpResponseStatus.UNAUTHORIZED) {
                // since the request failed, we need to close the channel on complete
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }
        done = true;
    }

    public HttpResponseStatus status() {
        return response.status();
    }

    public void beforeFlush() {
        if (!headerSent && response.status() == HttpResponseStatus.OK) {
            ctx.write(response, ctx.channel().voidPromise());
            headerSent = true;
        }
    }

    public void error(ByteBuf error) {
        if (this.error == null) {
            this.error = error;
        } else {
            this.error.capacity(this.error.capacity() + error.capacity());
            this.error.writeBytes(error);
        }
    }

    @Override
    public OutputStream outputStream() {
        return outputStream;
    }

    @Override
    public void setStatus(HttpResponseStatus status) {
        response.setStatus(status);
    }

    public boolean isHeaderSent() {
        return headerSent;
    }

    public void fullReponse(ByteBuf buffer) {
        fullResponse(response.protocolVersion(), response.status(), buffer, response.headers());
    }

    private void fullResponse(HttpVersion version, HttpResponseStatus status, ByteBuf buffer, HttpHeaders headers) {
        DefaultFullHttpResponse fullResponse = new DefaultFullHttpResponse(version, status, buffer);
        fullResponse.headers().set(headers);
        // for a full response remove chunked transfer-encoding and set the content length instead
        fullResponse.headers().remove(HttpHeaderNames.TRANSFER_ENCODING);
        fullResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
        future = ctx.writeAndFlush(fullResponse);
        headerSent = true;
        done = true;
    }

    @Override
    public void notifyChannelWritable() {
        outputStream.resume();
    }
}
