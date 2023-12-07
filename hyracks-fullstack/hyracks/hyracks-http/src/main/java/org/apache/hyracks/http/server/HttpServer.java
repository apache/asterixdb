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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.util.ExceptionUtils;
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
import io.netty.channel.ChannelFuture;
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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

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
    private static final int RECOVERING = 4;
    // Final members
    private final IChannelClosedHandler closedHandler;
    private final Object lock = new Object();
    private final AtomicInteger threadId = new AtomicInteger();
    private final ConcurrentMap<String, Object> ctx;
    private final LinkedBlockingQueue<Runnable> workQueue;
    private final ServletRegistry servlets;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Set<InetSocketAddress> addresses;
    private final ThreadPoolExecutor executor;
    // Mutable members
    private volatile int state = STOPPED;
    private volatile Thread recoveryThread;
    private final List<Channel> channels;
    private Throwable cause;
    private HttpServerConfig config;

    private final GenericFutureListener<Future<Void>> channelCloseListener = f -> {
        // This listener is invoked from within a netty IO thread. Hence, we can never block it
        // For simplicity, we will submit the recovery task to a different thread. We will also
        // close all channels on this server and attempt to rebind them.
        synchronized (lock) {
            if (state != STARTED) {
                return;
            }
            LOGGER.log(Level.WARN, "{} has stopped unexpectedly. Starting server recovery", this);
            MXHelper.logFileDescriptors();
            state = RECOVERING;
            triggerRecovery();
        }
    };

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port, HttpServerConfig config) {
        this(bossGroup, workerGroup, Collections.singletonList(new InetSocketAddress(port)), config, null);
    }

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, InetSocketAddress address,
            HttpServerConfig config, IChannelClosedHandler closeHandler) {
        this(bossGroup, workerGroup, Collections.singletonList(address), config, closeHandler);
    }

    public HttpServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, Collection<InetSocketAddress> addresses,
            HttpServerConfig config, IChannelClosedHandler closeHandler) {
        if (addresses.isEmpty()) {
            throw new IllegalArgumentException("no addresses specified");
        }
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.addresses = new LinkedHashSet<>(addresses);
        this.closedHandler = closeHandler;
        this.config = config;
        channels = new ArrayList<>();
        ctx = new ConcurrentHashMap<>();
        servlets = new ServletRegistry();
        workQueue = new LinkedBlockingQueue<>(config.getRequestQueueSize());
        int numExecutorThreads = config.getThreadCount();
        int[] ports = this.addresses.stream().mapToInt(InetSocketAddress::getPort).distinct().toArray();
        String desc;
        if (ports.length > 1) {
            desc = this.addresses.stream().map(a -> a.getAddress().getHostAddress() + ":" + a.getPort())
                    .collect(Collectors.joining(",", "[", "]"));
        } else {
            desc = "port:" + ports[0];
        }
        executor = new ThreadPoolExecutor(numExecutorThreads, numExecutorThreads, 0L, TimeUnit.MILLISECONDS, workQueue,
                runnable -> new Thread(runnable, "HttpExecutor(" + desc + ")-" + threadId.getAndIncrement()));
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
            case RECOVERING:
                return "RECOVERING";
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
        servlets.register(let);
    }

    public Set<IServlet> getServlets() {
        return servlets.getServlets();
    }

    protected void doStart() throws Exception {
        for (IServlet servlet : servlets.getServlets()) {
            try {
                servlet.init();
            } catch (IOException e) {
                LOGGER.error("Failure initializing servlet {} on http server {}", servlet, addresses, e);
                throw e;
            }
        }
        bind();
    }

    private void bind() throws Exception {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(RECEIVE_BUFFER_SIZE))
                .childOption(ChannelOption.AUTO_READ, Boolean.FALSE)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, WRITE_BUFFER_WATER_MARK)
                .handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(getChannelInitializer());
        List<Pair<InetSocketAddress, ChannelFuture>> channelFutures = new ArrayList<>();
        for (InetSocketAddress address : addresses) {
            channelFutures.add(org.apache.commons.lang3.tuple.Pair.of(address, b.bind(address)));
        }
        Exception failure = null;
        for (Pair<InetSocketAddress, ChannelFuture> addressFuture : channelFutures) {
            try {
                Channel channel = addressFuture.getRight().sync().channel();
                channel.closeFuture().addListener(channelCloseListener);
                synchronized (lock) {
                    channels.add(channel);
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.error("Bind failure starting http server at {}", addressFuture.getLeft(), e);
                failure = ExceptionUtils.suppress(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void triggerRecovery() throws InterruptedException {
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
        // try to revive the channels
        recoveryThread = new Thread(this::recover);
        recoveryThread.start();
    }

    public void recover() {
        try {
            synchronized (lock) {
                while (state == RECOVERING) {
                    try {
                        closeChannels();
                        bind();
                        setStarted();
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
        closeChannels();
    }

    public IServlet getServlet(FullHttpRequest request) {
        return servlets.getServlet(request.uri());
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
        return "{\"class\":\"" + getClass().getSimpleName() + "\",\"addresses\":" + addresses + ",\"state\":\""
                + getState() + "\"}";
    }

    public HttpServerConfig getConfig() {
        return config;
    }

    @Deprecated // this returns an arbitrary (the first supplied if collection is ordered) address
    public InetSocketAddress getAddress() {
        return addresses.iterator().next();
    }

    private void closeChannels() throws InterruptedException {
        synchronized (lock) {
            for (Channel channel : channels) {
                channel.closeFuture().removeListener(channelCloseListener);
                channel.close();
                channel.closeFuture().sync();
            }
            channels.clear();
        }
    }
}
