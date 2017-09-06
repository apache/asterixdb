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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    public static final int MAX_REQUEST_CHUNK_SIZE = 262144;
    public static final int MAX_REQUEST_HEADER_SIZE = 262144;
    public static final int MAX_REQUEST_INITIAL_LINE_LENGTH = 131072;
    public static final int RESPONSE_CHUNK_SIZE = 4096;
    private final HttpServer server;

    public HttpServerInitializer(HttpServer server) {
        this.server = server;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpRequestCapacityController(server));
        p.addLast(new HttpRequestDecoder(MAX_REQUEST_INITIAL_LINE_LENGTH, MAX_REQUEST_HEADER_SIZE,
                MAX_REQUEST_CHUNK_SIZE));
        p.addLast(new HttpResponseEncoder());
        p.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
        p.addLast(server.createHttpHandler(RESPONSE_CHUNK_SIZE));
    }
}
