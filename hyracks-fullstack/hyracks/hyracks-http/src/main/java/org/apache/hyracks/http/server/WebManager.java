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
import java.util.Collections;
import java.util.List;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

public class WebManager {
    private final List<HttpServer> servers;
    private final EventLoopGroup bosses;
    private final EventLoopGroup workers;

    static {
        // bootstrap netty with log4j2 logging
        io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    /**
     * Create a web manager with number of bosses = 1
     * and number of workers = MultithreadEventLoopGroup.DEFAULT_EVENT_LOOP_THREADS
     * The default can be set using -Dio.netty.eventLoopThreads
     * Otherwise, it is set to Runtime.getRuntime().availableProcessors() * 2
     */
    public WebManager() {
        this(1, 0);
    }

    /**
     * Create a web manager with number of bosses = 1 and number of workers = numWorkers
     *
     * @param numWorkers
     *            number of worker threads
     */
    public WebManager(int numWorkers) {
        this(1, numWorkers);
    }

    /**
     * Create a web manager with number of bosses = numBosses and number of workers = numWorkers
     *
     * @param numBosses
     *            number of boss threads
     * @param numWorkers
     *            number of worker threads
     */
    public WebManager(int numBosses, int numWorkers) {
        servers = new ArrayList<>();
        bosses = new NioEventLoopGroup(numBosses);
        workers = new NioEventLoopGroup(numWorkers);
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
        List<Exception> stopExceptions = Collections.synchronizedList(new ArrayList<>());
        servers.parallelStream().forEach(server -> {
            try {
                server.stop();
            } catch (Exception e) {
                stopExceptions.add(e);
            }
        });
        workers.shutdownGracefully().sync();
        bosses.shutdownGracefully().sync();
        if (!stopExceptions.isEmpty()) {
            Exception ex = null;
            for (Exception stopException : stopExceptions) {
                if (ex == null) {
                    ex = stopException;
                } else {
                    ex.addSuppressed(stopException);
                }
            }
            throw ex;
        }
    }

    public void add(HttpServer server) {
        servers.add(server);
    }
}
