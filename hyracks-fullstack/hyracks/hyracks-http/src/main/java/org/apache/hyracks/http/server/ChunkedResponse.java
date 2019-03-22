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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;
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
 * Here is a breakdown of the possible cases.
 * 1. smaller than chunkSize, no error -> full response
 * 2. smaller than chunkSize, error -> full response
 * 3. larger than chunkSize, error after header-> close connection. release buffer and release error
 * 4. larger than chunkSize, no error. -> header, data, empty response
 */
public class ChunkedResponse implements IServletResponse {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ChannelHandlerContext ctx;
    private final ChunkedNettyOutputStream outputStream;
    private final HttpServerHandler<?> handler;
    private PrintWriter writer;
    private DefaultHttpResponse response;
    private boolean headerSent;
    private ByteBuf error;
    private ChannelFuture future;
    private boolean done;

    public ChunkedResponse(HttpServerHandler<?> handler, ChannelHandlerContext ctx, FullHttpRequest request,
            int chunkSize) {
        this.handler = handler;
        this.ctx = ctx;
        outputStream = new ChunkedNettyOutputStream(ctx, chunkSize, this);
        response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        HttpUtil.setConnectionHeader(request, response);
    }

    @Override
    public IServletResponse setHeader(CharSequence name, Object value) throws IOException {
        if (headerSent) {
            throw new IOException("Can't add more headers since the initial response was sent");
        }
        String nameString = String.valueOf(name);
        if (writer != null && nameString.equals(HttpHeaderNames.CONTENT_TYPE.toString())) {
            throw new IOException("Can't set " + HttpHeaderNames.CONTENT_TYPE + " after writer has been accessed");
        }
        response.headers().set(nameString, value);
        return this;
    }

    @Override
    public ChannelFuture lastContentFuture() {
        return future;
    }

    @Override
    public synchronized PrintWriter writer() {
        if (writer == null) {
            Charset charset = io.netty.handler.codec.http.HttpUtil.getCharset(response, StandardCharsets.UTF_8);
            writer = new PrintWriter(new OutputStreamWriter(outputStream, charset));
        }
        return writer;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        } else {
            outputStream.close();
        }
        if (error == null && response.status() == HttpResponseStatus.OK) {
            if (!done) {
                respond(LastHttpContent.EMPTY_LAST_CONTENT);
            }
        } else {
            // There was an error
            if (headerSent) {
                LOGGER.log(Level.WARN, "Error after header write of chunked response");
                if (error != null) {
                    error.release();
                }
                future = ctx.channel().close().addListener(handler);
            } else {
                // we didn't send anything to the user, we need to send an non-chunked error response
                fullResponse(response.protocolVersion(), response.status(),
                        error == null ? ctx.alloc().buffer(0, 0) : error, response.headers());
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

    public void fullResponse(ByteBuf buffer) {
        fullResponse(response.protocolVersion(), response.status(), buffer, response.headers());
    }

    private void fullResponse(HttpVersion version, HttpResponseStatus status, ByteBuf buffer, HttpHeaders headers) {
        DefaultFullHttpResponse fullResponse = new DefaultFullHttpResponse(version, status, buffer);
        fullResponse.headers().set(headers);
        // for a full response remove chunked transfer-encoding and set the content length instead
        fullResponse.headers().remove(HttpHeaderNames.TRANSFER_ENCODING);
        fullResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
        respond(fullResponse);
        headerSent = true;
        done = true;
    }

    @Override
    public void notifyChannelWritable() {
        outputStream.channelWritabilityChanged();
    }

    @Override
    public void notifyChannelInactive() {
        outputStream.channelWritabilityChanged();
    }

    @Override
    public void cancel() {
        outputStream.cancel();
    }

    private void respond(HttpObject response) {
        final ChannelPromise responseCompletionPromise = ctx.newPromise();
        responseCompletionPromise.addListener(handler);
        future = ctx.writeAndFlush(response, responseCompletionPromise);
    }
}
