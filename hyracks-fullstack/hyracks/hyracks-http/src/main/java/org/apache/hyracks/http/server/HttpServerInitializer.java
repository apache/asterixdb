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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private final HttpServer server;
    private final int maxRequestSize;
    private final int maxRequestInitialLineLength;
    private final int maxRequestHeaderSize;
    private final int maxRequestChunkSize;
    private final int maxResponseChunkSize;

    public HttpServerInitializer(HttpServer server) {
        this.server = server;
        final HttpServerConfig config = server.getConfig();
        maxRequestSize = config.getMaxRequestSize();
        maxRequestInitialLineLength = config.getMaxRequestInitialLineLength();
        maxRequestHeaderSize = config.getMaxRequestHeaderSize();
        maxRequestChunkSize = config.getMaxRequestChunkSize();
        maxResponseChunkSize = config.getMaxResponseChunkSize();
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpRequestCapacityController(server));
        p.addLast(new HttpRequestDecoder(maxRequestInitialLineLength, maxRequestHeaderSize, maxRequestChunkSize));
        p.addLast(new HttpResponseEncoder());
        p.addLast(new CLFLogger());
        p.addLast(new HttpRequestAggregator(maxRequestSize));
        p.addLast(server.createHttpHandler(maxResponseChunkSize));
    }
}
