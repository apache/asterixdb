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
package org.apache.hyracks.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpProcessorBuilder;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.HttpServerConfig;
import org.apache.hyracks.http.server.HttpServerConfigBuilder;
import org.apache.hyracks.http.server.InterruptOnCloseHandler;
import org.apache.hyracks.http.server.WebManager;
import org.apache.hyracks.test.http.servlet.SleepyServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class PipelinedRequestsTest {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int PORT = 9898;
    private static final String PATH = "/";

    @Test
    public void pipelinedRequests() throws Exception {
        setupServer();
        final HttpHost target = new HttpHost("localhost", PORT);
        final List<BasicAsyncRequestProducer> requestProducers =
                Arrays.asList(new BasicAsyncRequestProducer(target, new BasicHttpRequest("GET", PATH)),
                        new BasicAsyncRequestProducer(target, new BasicHttpRequest("GET", PATH)));
        final List<BasicAsyncResponseConsumer> responseConsumers =
                Arrays.asList(new BasicAsyncResponseConsumer(), new BasicAsyncResponseConsumer());
        final List<HttpResponse> httpResponses = executePipelined(target, requestProducers, responseConsumers);
        for (HttpResponse response : httpResponses) {
            Assert.assertNotEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        }
    }

    private void setupServer() throws Exception {
        final WebManager webMgr = new WebManager();
        final HttpServerConfig config =
                HttpServerConfigBuilder.custom().setThreadCount(16).setRequestQueueSize(16).build();
        final HttpServer server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), new InetSocketAddress(PORT),
                config, InterruptOnCloseHandler.INSTANCE);
        final SleepyServlet servlet = new SleepyServlet(server.ctx(), new String[] { PATH });
        server.addServlet(servlet);
        webMgr.add(server);
        webMgr.start();
    }

    private List<HttpResponse> executePipelined(HttpHost host, List<BasicAsyncRequestProducer> requestProducers,
            List<BasicAsyncResponseConsumer> responseConsumers) throws Exception {
        final List<HttpResponse> results = new ArrayList<>();
        final HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();
        final IOEventDispatch ioEventDispatch =
                new DefaultHttpClientIODispatch(protocolHandler, ConnectionConfig.DEFAULT);
        final ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
        final BasicNIOConnPool pool = new BasicNIOConnPool(ioReactor, ConnectionConfig.DEFAULT);
        pool.setDefaultMaxPerRoute(1);
        pool.setMaxTotal(1);
        final Thread reactorThread = new Thread(() -> {
            try {
                ioReactor.execute(ioEventDispatch);
            } catch (final IOException e) {
                LOGGER.error(e);
            }
        });
        reactorThread.start();
        final HttpCoreContext context = HttpCoreContext.create();
        final CountDownLatch latch = new CountDownLatch(1);
        final HttpProcessor httpProc =
                HttpProcessorBuilder.create().add(new RequestContent()).add(new RequestTargetHost())
                        .add(new RequestConnControl()).add(new RequestExpectContinue(true)).build();
        final HttpAsyncRequester requester = new HttpAsyncRequester(httpProc);
        requester.executePipelined(host, requestProducers, responseConsumers, pool, context,
                new FutureCallback<List<HttpResponse>>() {
                    @Override
                    public void completed(final List<HttpResponse> result) {
                        results.addAll(result);
                        latch.countDown();
                    }

                    @Override
                    public void failed(final Exception ex) {
                        latch.countDown();
                    }

                    @Override
                    public void cancelled() {
                        latch.countDown();
                    }
                });
        latch.await(5, TimeUnit.SECONDS);
        ioReactor.shutdown();
        return results;
    }
}
