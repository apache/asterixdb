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
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ChunkedNettyOutputStream extends OutputStream {

    private static final Logger LOGGER = Logger.getLogger(ChunkedNettyOutputStream.class.getName());
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
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        if (len > buffer.capacity()) {
            flush();
            flush(b, off, len);
        } else {
            int space = buffer.writableBytes();
            if (space >= len) {
                buffer.writeBytes(b, off, len);
            } else {
                buffer.writeBytes(b, off, space);
                flush();
                buffer.writeBytes(b, off + space, len - space);
            }
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
                flush();
                buffer.release();
            } else {
                response.fullReponse(buffer);
            }
            super.close();
        }
        closed = true;
    }

    @Override
    public void flush() throws IOException {
        ensureWritable();
        if (buffer.readableBytes() > 0) {
            if (response.status() == HttpResponseStatus.OK) {
                int size = buffer.capacity();
                response.beforeFlush();
                DefaultHttpContent content = new DefaultHttpContent(buffer);
                ctx.write(content, ctx.channel().voidPromise());
                buffer = ctx.alloc().buffer(size);
            } else {
                ByteBuf aBuffer = ctx.alloc().buffer(buffer.readableBytes());
                aBuffer.writeBytes(buffer);
                response.error(aBuffer);
                buffer.clear();
            }
        }
    }

    private void flush(byte[] buf, int offset, int len) throws IOException {
        ensureWritable();
        ByteBuf aBuffer = ctx.alloc().buffer(len);
        aBuffer.writeBytes(buf, offset, len);
        if (response.status() == HttpResponseStatus.OK) {
            response.beforeFlush();
            ctx.write(new DefaultHttpContent(aBuffer), ctx.channel().voidPromise());
        } else {
            response.error(aBuffer);
        }
    }

    private synchronized void ensureWritable() throws IOException {
        while (!ctx.channel().isWritable()) {
            try {
                ctx.flush();
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interupted while waiting for channel to be writable", e);
                throw new IOException(e);
            }
        }
    }

    public synchronized void resume() {
        notifyAll();
    }
}