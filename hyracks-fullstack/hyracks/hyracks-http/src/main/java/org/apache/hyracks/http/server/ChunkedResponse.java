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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.InvokeUtil;
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
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

/**
 * A chunked http response. Here is how it is expected to work:
 * If the response is a success aka 200 and is less than chunkSize, then it is sent as a single http response message
 * If the response is larger than the chunkSize and the response status is 200, then it is sent as chunks of chunkSize.
 * If the response status is non 200, then it is always sent as a single http response message.
 * If the response status is non 200, then output buffered before setting the response status is discarded.
 * If flush() is called on the writer and even if it is less than chunkSize, then the initial response will be sent
 * with headers, followed by the buffered bytes as the first chunk.
 * When chunking, an output buffer is allocated only when the previous buffer has been sent
 * If an error occurs after sending the first chunk, the error is sent as the last chunk and the connection is then
 * closed; if the error cannot be sent (no error content, or the channel is no longer writable), the connection is
 * closed without terminating the response.
 * Here is a breakdown of the possible cases.
 * 1. smaller than chunkSize, no error -> full response
 * 2. smaller than chunkSize, error -> full response
 * 3. larger than chunkSize, error after header -> error as last chunk, then close connection
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
    private ByteBuf errorBuf;
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
        try {
            InvokeUtil.tryIoWithCleanups(() -> {
                if (writer != null) {
                    writer.close();
                } else {
                    outputStream.close();
                }
                if (errorBuf == null && response.status() == HttpResponseStatus.OK) {
                    if (!done) {
                        respond(LastHttpContent.EMPTY_LAST_CONTENT);
                    }
                } else {
                    // There was an error
                    if (headerSent) {
                        //Send the error content without re-sending the header
                        if (errorBuf != null && errorBuf.refCnt() > 0 && ctx.channel().isWritable()) {
                            sendErrorContentWithoutHeader();
                        } else {
                            // the error cannot be written, so close rather than leave the client waiting
                            LOGGER.log(Level.WARN,
                                    "Error after header write of chunked response; cannot send the error content "
                                            + "(errorBuf={}, writable={})",
                                    errorBuf, ctx.channel().isWritable());
                            future = ctx.channel().close().addListener(handler);
                        }
                    } else {
                        // we didn't send anything to the user, we need to send an non-chunked error response
                        fullResponse(response.protocolVersion(), response.status(),
                                errorBuf == null ? ctx.alloc().buffer(0, 0) : errorBuf, response.headers());
                        // The responsibility of releasing the error buffer is now with the netty pipeline since it is
                        // forwarded within the http content. We must nullify buffer to avoid releasing the buffer twice.
                        errorBuf = null;
                    }
                }
            }, outputStream::close, () -> {
                ReferenceCountUtil.release(errorBuf);
                // We must nullify buffer to avoid releasing the buffer twice in case of duplicate close()
                errorBuf = null;
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        done = true;
    }

    public HttpResponseStatus status() {
        return response.status();
    }

    public void beforeFlush() {
        if (!headerSent && response.status() == HttpResponseStatus.OK) {
            // a snapshot, not the shared response: the write is queued, and a servlet that fails meanwhile would
            // set an error status on it. Once a chunk is in flight the error goes out as the last chunk, see close()
            DefaultHttpResponse sentHeader = new DefaultHttpResponse(response.protocolVersion(), response.status());
            sentHeader.headers().set(response.headers());
            ctx.write(sentHeader, ctx.channel().voidPromise());
            headerSent = true;
        }
    }

    public void error(ByteBuf error) {
        if (errorBuf == null) {
            errorBuf = ctx.alloc().buffer(error.readableBytes());
        }
        if (errorBuf.capacity() < this.errorBuf.capacity() + error.capacity()) {
            errorBuf.capacity(this.errorBuf.capacity() + error.capacity());
        }
        errorBuf.writeBytes(error);
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

    private void sendErrorContentWithoutHeader() {
        // the header is already on the wire, so the error goes out as the last chunk, which also ends the response
        future = ctx.writeAndFlush(new DefaultLastHttpContent(errorBuf)).addListener(handler);
        // The responsibility of releasing the error buffer is now with the netty pipeline since it is
        // forwarded within the http content. We must nullify buffer to avoid releasing the buffer twice.
        errorBuf = null;
        // the response carries an error in a body that was already partially sent; don't reuse the channel
        future.addListener(f -> ctx.channel().close());
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
