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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ChunkedNettyOutputStream extends OutputStream {

    private final ChannelHandlerContext ctx;
    private final ChunkedResponse response;
    private ByteBuf buffer;

    public ChunkedNettyOutputStream(ChannelHandlerContext ctx, int chunkSize,
            ChunkedResponse response) {
        this.response = response;
        this.ctx = ctx;
        buffer = ctx.alloc().buffer(chunkSize);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length)) {
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
    public synchronized void write(int b) {
        if (buffer.writableBytes() > 0) {
            buffer.writeByte(b);
        } else {
            flush();
            buffer.writeByte(b);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        flush();
        buffer.release();
        super.close();
    }

    @Override
    public synchronized void flush() {
        if (buffer.readableBytes() > 0) {
            int size = buffer.capacity();
            if (response.status() == HttpResponseStatus.OK) {
                response.flush();
                DefaultHttpContent content = new DefaultHttpContent(buffer);
                ctx.write(content);
            } else {
                response.error(buffer);
            }
            buffer = ctx.alloc().buffer(size);
        }
    }

    private synchronized void flush(byte[] buf, int offset, int len) {
        ByteBuf aBuffer = ctx.alloc().buffer(len);
        aBuffer.writeBytes(buf, offset, len);
        if (response.status() == HttpResponseStatus.OK) {
            response.flush();
            ctx.write(new DefaultHttpContent(aBuffer));
        } else {
            response.error(aBuffer);
        }
    }
}