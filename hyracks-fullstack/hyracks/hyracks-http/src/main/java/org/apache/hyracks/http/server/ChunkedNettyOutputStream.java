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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.OutOfDirectMemoryError;

public class ChunkedNettyOutputStream extends OutputStream {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ChannelHandlerContext ctx;
    private final ChunkedResponse response;
    private ByteBuf buffer;
    private boolean closed;

    public ChunkedNettyOutputStream(ChannelHandlerContext ctx, int chunkSize, ChunkedResponse response) {
        this.response = response;
        this.ctx = ctx;
        buffer = ctx.alloc().buffer(chunkSize);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)) {
                throw new IndexOutOfBoundsException();
            }
            while (len > 0) {
                int space = buffer.writableBytes();
                if (space >= len) {
                    buffer.writeBytes(b, off, len);
                    len = 0; // NOSONAR
                } else {
                    buffer.writeBytes(b, off, space);
                    len -= space; // NOSONAR
                    off += space; // NOSONAR
                    flush();
                }
            }
        } catch (OutOfDirectMemoryError error) {// NOSONAR
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            throw error;
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (!buffer.isWritable()) {
            flush();
        }
        buffer.writeByte(b);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            if (response.isHeaderSent() || response.status() != HttpResponseStatus.OK) {
                try {
                    flush();
                } finally {
                    if (buffer != null) {
                        buffer.release();
                    }
                }
            } else {
                response.fullResponse(buffer);
            }
            super.close();
        }
        closed = true;
    }

    @Override
    public void flush() throws IOException {
        ensureWritable();
        if (buffer != null && buffer.readableBytes() > 0) {
            if (response.status() == HttpResponseStatus.OK) {
                int size = buffer.capacity();
                response.beforeFlush();
                DefaultHttpContent content = new DefaultHttpContent(buffer);
                ctx.writeAndFlush(content, ctx.channel().voidPromise());
                // The responsibility of releasing the buffer is now with the netty pipeline since it is forwarded
                // within the http content. We must nullify buffer before we allocate the next one to avoid
                // releasing the buffer twice in case the allocation call fails.
                buffer = null;
                buffer = ctx.alloc().buffer(size);
            } else {
                ByteBuf aBuffer = ctx.alloc().buffer(buffer.readableBytes());
                aBuffer.writeBytes(buffer);
                response.error(aBuffer);
                buffer.clear();
            }
        }
    }

    private synchronized void ensureWritable() throws IOException {
        while (!ctx.channel().isWritable()) {
            try {
                if (!ctx.channel().isActive()) {
                    throw new IOException("Inactive channel");
                }
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARN, "Interrupted while waiting for channel to be writable", e);
                throw new IOException(e);
            }
        }
    }

    public synchronized void channelWritabilityChanged() {
        notifyAll();
    }

    public void cancel() {
        ReferenceCountUtil.release(buffer);
    }
}
