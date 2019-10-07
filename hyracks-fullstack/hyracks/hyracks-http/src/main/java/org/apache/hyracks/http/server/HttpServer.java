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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.http.api.IChannelClosedHandler;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.util.MXHelper;
import org.apache.hyracks.util.ThreadDumpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class HttpServer {
    // Constants
    private static final int LOW_WRITE_BUFFER_WATER_MARK = 8 * 1024;
    private static final int HIGH_WRITE_BUFFER_WATER_MARK = 32 * 1024;
    protected static final WriteBufferWaterMark WRITE_BUFFER_WATER_MARK =
            new WriteBufferWaterMark(LOW_WRITE_BUFFER_WATER_MARK, HIGH_WRITE_BUFFER_WATER_MARK);
    protected static final int RECEIVE_BUFFER_SIZE = 4096;
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int FAILED = -1;
    private static final int STOPPED = 0;
    private static final int STARTING = 1;
    private static final int STARTED = 2;
    private static final int STOPPING = 3;
    // Final members
    private final IChannelClosedHandler closedHandler;
    private final Object lock = new Object();
    private final AtomicInteger threadId = new AtomicInteger();
    private final ConcurrentMap<String, Object> ctx;
    private final LinkedBlockingQueue<Runnable> workQueue;
    private final List<IServlet> servlets;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final InetSocketAddress address;
    private final ThreadPoolExecutor executor;
    // Mutable members
    private volatile int state = STOPPED;
    private volatile Thread recoveryThread;
    private volatile Channel channel;
    private Throwable cause;
    private HttpServerConfig config;

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port, HttpServerConfig config) {
        this(bossGroup, workerGroup, new InetSocketAddress(port), config, null);
    }

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, InetSocketAddress address,
            HttpServerConfig config) {
        this(bossGroup, workerGroup, address, config, null);
    }

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, InetSocketAddress address,
            HttpServerConfig config, IChannelClosedHandler closeHandler) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.address = address;
        this.closedHandler = closeHandler;
        this.config = config;
        ctx = new ConcurrentHashMap<>();
        servlets = new ArrayList<>();
        workQueue = new LinkedBlockingQueue<>(config.getRequestQueueSize());
        int numExecutorThreads = config.getThreadCount();
        executor = new ThreadPoolExecutor(numExecutorThreads, numExecutorThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
                runnable -> new Thread(runnable,
                        "HttpExecutor(port:" + address.getPort() + ")-" + threadId.getAndIncrement()));
        long directMemoryBudget = numExecutorThreads * (long) HIGH_WRITE_BUFFER_WATER_MARK
                + numExecutorThreads * config.getMaxResponseChunkSize();
        LOGGER.log(Level.DEBUG,
                "The output direct memory budget for this server " + "is " + directMemoryBudget + " bytes");
        long inputBudgetEstimate =
                (long) config.getMaxRequestInitialLineLength() * (config.getRequestQueueSize() + numExecutorThreads);
        inputBudgetEstimate = inputBudgetEstimate * 2;
        LOGGER.log(Level.DEBUG,
                "The \"estimated\" input direct memory budget for this server is " + inputBudgetEstimate + " bytes");
        // Having multiple arenas, memory fragments, and local thread cached buffers
        // can cause the input memory usage to exceed estimate and custom buffer allocator must be used to avoid this
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
                LOGGER.error("Failure starting an Http Server at: {}", address, e);
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
                LOGGER.log(Level.ERROR, "Failure stopping an Http Server", e);
                setFailed(e);
                throw e;
            }
        }
        // Should wait for the recovery thread outside synchronized block
        Thread rt = recoveryThread;
        if (rt != null) {
            rt.join(TimeUnit.SECONDS.toMillis(5));
            if (recoveryThread != null) {
                LOGGER.log(Level.ERROR, "Failure stopping recovery thread of {}", this);
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
        channel = bind();
    }

    private Channel bind() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(RECEIVE_BUFFER_SIZE))
                .childOption(ChannelOption.AUTO_READ, Boolean.FALSE)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, WRITE_BUFFER_WATER_MARK)
                .handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(getChannelInitializer());
        Channel newChannel = b.bind(address).sync().channel();
        newChannel.closeFuture().addListener(f -> {
            // This listener is invoked from within a netty IO thread. Hence, we can never block it
            // For simplicity, we will submit the recovery task to a different thread
            synchronized (lock) {
                if (state != STARTED) {
                    return;
                }
                LOGGER.log(Level.WARN, "{} has stopped unexpectedly. Starting server recovery", this);
                MXHelper.logFileDescriptors();
                triggerRecovery();
            }
        });
        return newChannel;
    }

    private void triggerRecovery() {
        Thread rt = recoveryThread;
        if (rt != null) {
            try {
                rt.join();
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARN, this + " recovery was interrupted", e);
                Thread.currentThread().interrupt();
                return;
            }
        }
        // try to revive the channel
        recoveryThread = new Thread(this::recover);
        recoveryThread.start();
    }

    public void recover() {
        try {
            synchronized (lock) {
                while (state == STARTED) {
                    try {
                        channel = bind();
                        break;
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARN, this + " was interrupted while attempting to revive server channel", e);
                        setFailed(e);
                        Thread.currentThread().interrupt();
                    } catch (Throwable th) {
                        // sleep for 5s
                        LOGGER.log(Level.WARN, this + " failed server recovery attempt. "
                                + "Sleeping for 5s before starting the next attempt", th);
                        try {
                            // Wait on lock to allow stop request to be executed
                            lock.wait(TimeUnit.SECONDS.toMillis(5));
                        } catch (InterruptedException e) {
                            LOGGER.log(Level.WARN, this + " interrupted while attempting to revive server channel", e);
                            setFailed(e);
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        } finally {
            recoveryThread = null;
        }
    }

    protected void doStop() throws InterruptedException {
        // stop recovery if it was ongoing
        Thread rt = recoveryThread;
        if (rt != null) {
            rt.interrupt();
        }
        // stop taking new requests
        executor.shutdown();
        try {
            // wait 5s before interrupting existing requests
            executor.awaitTermination(5, TimeUnit.SECONDS);
            // interrupt
            executor.shutdownNow();
            // wait 30s for interrupted requests to unwind
            executor.awaitTermination(30, TimeUnit.SECONDS);
            if (!executor.isTerminated()) {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.log(Level.ERROR,
                            "Failed to shutdown http server executor; thread dump: " + ThreadDumpUtil.takeDumpString());
                } else {
                    LOGGER.log(Level.ERROR, "Failed to shutdown http server executor");
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Error while shutting down http server executor", e);
        }
        if (channel != null) {
            channel.close();
            channel.closeFuture().sync();
        }
    }

    public IServlet getServlet(FullHttpRequest request) {
        String uri = request.uri();
        int i = uri.indexOf('?');
        if (i >= 0) {
            uri = uri.substring(0, i);
        }
        for (IServlet servlet : servlets) {
            for (String path : servlet.getPaths()) {
                if (match(path, uri)) {
                    return servlet;
                }
            }
        }
        return null;
    }

    static boolean match(String pathSpec, String path) {
        char c = pathSpec.charAt(0);
        if (c == '/') {
            if (pathSpec.equals(path) || (pathSpec.length() == 1 && path.isEmpty())) {
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

    static boolean isPathWildcardMatch(String pathSpec, String path) {
        final int length = pathSpec.length();
        if (length < 2) {
            return false;
        }
        final int cpl = length - 2;
        final boolean b = pathSpec.endsWith("/*") && path.regionMatches(0, pathSpec, 0, cpl);
        return b && (path.length() == cpl || '/' == path.charAt(cpl));
    }

    protected HttpServerHandler<? extends HttpServer> createHttpHandler(int chunkSize) {
        return new HttpServerHandler<>(this, chunkSize);
    }

    protected ChannelInitializer<SocketChannel> getChannelInitializer() {
        return new HttpServerInitializer(this);
    }

    public ThreadPoolExecutor getExecutor(HttpRequestHandler handler) {
        return executor;
    }

    protected EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    public int getWorkQueueSize() {
        return workQueue.size();
    }

    public IChannelClosedHandler getChannelClosedHandler() {
        return closedHandler;
    }

    public HttpScheme getScheme() {
        return HttpScheme.HTTP;
    }

    @Override
    public String toString() {
        return "{\"class\":\"" + getClass().getSimpleName() + "\",\"address\":" + address + ",\"state\":\"" + getState()
                + "\"}";
    }

    public HttpServerConfig getConfig() {
        return config;
    }
}
