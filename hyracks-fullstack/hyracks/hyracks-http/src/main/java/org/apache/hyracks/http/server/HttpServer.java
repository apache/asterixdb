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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServlet;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class HttpServer {
    // Constants
    private static final int LOW_WRITE_BUFFER_WATER_MARK = 8 * 1024;
    private static final int HIGH_WRITE_BUFFER_WATER_MARK = 32 * 1024;
    private static final Logger LOGGER = Logger.getLogger(HttpServer.class.getName());
    private static final int FAILED = -1;
    private static final int STOPPED = 0;
    private static final int STARTING = 1;
    private static final int STARTED = 2;
    private static final int STOPPING = 3;
    // Final members
    private final Object lock = new Object();
    private final AtomicInteger threadId = new AtomicInteger();
    private final ConcurrentMap<String, Object> ctx;
    private final List<IServlet> servlets;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final int port;
    private final ExecutorService executor;
    // Mutable members
    private volatile int state = STOPPED;
    private Channel channel;
    private Throwable cause;

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port) {
        this(bossGroup, workerGroup, port, 16, 256);
    }

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port, int numExecutorThreads,
            int requestQueueSize) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.port = port;
        ctx = new ConcurrentHashMap<>();
        servlets = new ArrayList<>();
        executor = new ThreadPoolExecutor(numExecutorThreads, numExecutorThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(requestQueueSize),
                runnable -> new Thread(runnable, "HttpExecutor(port:" + port + ")-" + threadId.getAndIncrement()));
    }

    public final void start() throws Exception { // NOSONAR
        synchronized (lock) {
            try {
                if (state == STARTED || state == STARTING) {
                    return;
                }
                setStarting();
                doStart();
                setStarted();
            } catch (Throwable e) { // NOSONAR
                LOGGER.log(Level.SEVERE, "Failure starting an Http Server", e);
                setFailed(e);
                throw e;
            }
        }
    }

    public final void stop() throws Exception { // NOSONAR
        synchronized (lock) {
            try {
                if (state == STOPPING || state == STOPPED) {
                    return;
                }
                setStopping();
                doStop();
                setStopped();
            } catch (Throwable e) { // NOSONAR
                LOGGER.log(Level.SEVERE, "Failure stopping an Http Server", e);
                setFailed(e);
                throw e;
            }
        }
    }

    /**
     * @return String representation of the State for this server
     */
    public String getState() {
        switch (state) {
            case FAILED:
                return "FAILED";
            case STARTING:
                return "STARTING";
            case STARTED:
                return "STARTED";
            case STOPPING:
                return "STOPPING";
            case STOPPED:
                return "STOPPED";
            default:
                return "UNKNOWN";
        }
    }

    private void setStarting() {
        state = STARTING;
    }

    private void setStarted() {
        state = STARTED;
    }

    private void setStopping() {
        state = STOPPING;
    }

    private void setStopped() {
        state = STOPPED;
    }

    private void setFailed(Throwable th) {
        state = FAILED;
        cause = th;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setAttribute(String name, Object value) {
        ctx.put(name, value);
    }

    public Object getAttribute(String name) {
        return ctx.get(name);
    }

    public ConcurrentMap<String, Object> ctx() {
        return ctx;
    }

    public void addServlet(IServlet let) {
        servlets.add(let);
    }

    protected void doStart() throws InterruptedException {
        /*
         * This is a hacky way to ensure that IServlets with more specific paths are checked first.
         * For example:
         * "/path/to/resource/"
         * is checked before
         * "/path/to/"
         * which in turn is checked before
         * "/path/"
         * Note that it doesn't work for the case where multiple paths map to a single IServlet
         */
        Collections.sort(servlets, (l1, l2) -> l2.getPaths()[0].length() - l1.getPaths()[0].length());
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(LOW_WRITE_BUFFER_WATER_MARK, HIGH_WRITE_BUFFER_WATER_MARK))
                .handler(new LoggingHandler(LogLevel.INFO)).childHandler(new HttpServerInitializer(this));
        channel = b.bind(port).sync().channel();
    }

    protected void doStop() throws InterruptedException {
        channel.close();
        channel.closeFuture().sync();
    }

    public IServlet getServlet(FullHttpRequest request) {
        String uri = request.uri();
        int i = uri.indexOf('?');
        if (i >= 0) {
            uri = uri.substring(0, i);
        }
        for (IServlet let : servlets) {
            for (String path : let.getPaths()) {
                if (match(path, uri)) {
                    return let;
                }
            }
        }
        return null;
    }

    private static boolean match(String pathSpec, String path) {
        char c = pathSpec.charAt(0);
        if (c == '/') {
            if (pathSpec.length() == 1 || pathSpec.equals(path)) {
                return true;
            }

            if (isPathWildcardMatch(pathSpec, path)) {
                return true;
            }
        } else if (c == '*') {
            return path.regionMatches(path.length() - pathSpec.length() + 1, pathSpec, 1, pathSpec.length() - 1);
        }
        return false;
    }

    private static boolean isPathWildcardMatch(String pathSpec, String path) {
        int cpl = pathSpec.length() - 2;
        return (pathSpec.endsWith("/*") && path.regionMatches(0, pathSpec, 0, cpl))
                && (path.length() == cpl || '/' == path.charAt(cpl));
    }

    public ExecutorService getExecutor() {
        return executor;
    }
}
