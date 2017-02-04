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

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class WebManager {
    private final List<HttpServer> servers;
    private final EventLoopGroup bosses;
    private final EventLoopGroup workers;

    public WebManager() {
        servers = new ArrayList<>();
        bosses = new NioEventLoopGroup(1);
        workers = new NioEventLoopGroup();
    }

    public List<HttpServer> getServers() {
        return servers;
    }

    public EventLoopGroup getBosses() {
        return bosses;
    }

    public EventLoopGroup getWorkers() {
        return workers;
    }

    public void start() throws Exception {
        for (HttpServer server : servers) {
            server.start();
        }
    }

    public void stop() throws Exception {
        for (HttpServer server : servers) {
            server.stop();
        }
        workers.shutdownGracefully().sync();
        bosses.shutdownGracefully().sync();
    }

    public void add(HttpServer server) {
        servers.add(server);
    }
}
