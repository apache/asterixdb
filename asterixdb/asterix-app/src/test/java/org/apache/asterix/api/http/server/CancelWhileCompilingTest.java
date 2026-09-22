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
package org.apache.asterix.api.http.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.cc.CCExtensionManager;
import org.apache.asterix.common.api.IConfigValidatorFactory;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.api.IReceptionistFactory;
import org.apache.asterix.common.api.ISchedulableClientRequest;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.external.IAdapterFactoryService;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.replication.INcLifecycleCoordinator;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.translator.Receptionist;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.io.IOManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A request with no job running - compiling a statement, or between two of them - is stopped by interrupting the
 * thread executing it, which is the only thing that would tell it that it was cancelled. That window must stay
 * cancellable: leaving the cancellable set is for a statement that is only sending its result, and narrowing it
 * any further would let a cancelled request run the statements that remain.
 */
public class CancelWhileCompilingTest {

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil() {
        @Override
        protected ICCApplication createCCApplication() {
            return new PausingCCApplication();
        }
    };
    private static final String QUERY_SERVICE = "http://localhost:19002/query/service";
    private static final String RUNNING_REQUESTS = "http://localhost:19002/admin/requests/running";

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
    }

    @Test
    public void cancelWhileAStatementCompilesStopsTheStatementsThatRemain() throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        PausingReceptionist.armFor(clientContextId);
        try {
            Thread request = new Thread(() -> post(clientContextId));
            request.start();

            Assert.assertTrue("the second statement never reached the window before its job is submitted",
                    PausingReceptionist.COMPILING.await(60, TimeUnit.SECONDS));
            // the request has no job running, so it is this window the interrupt is for: the cancel must be taken
            Assert.assertEquals("a request compiling a statement must accept a cancel", 200, cancel(clientContextId));
            PausingReceptionist.RESUMED.countDown();
            request.join(TimeUnit.MINUTES.toMillis(1));

            String body = PausingReceptionist.RESPONSE.get();
            Assert.assertTrue("expected the cancellation to be reported in: " + body, body.contains("ASX0041"));
            Assert.assertTrue("the first statement should have run: " + body, body.contains("{\"$1\":1}"));
            Assert.assertFalse("the statements after the cancelled one must not run: " + body,
                    body.contains("{\"$1\":3}"));
        } finally {
            PausingReceptionist.disarm();
        }
    }

    private static void post(String clientContextId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(QUERY_SERVICE);
            httpPost.setEntity(
                    new StringEntity("{\"statement\": \"select 1; select 2; select 3;\", \"multi-statement\": true,"
                            + " \"client_context_id\": \"" + clientContextId + "\"}", StandardCharsets.UTF_8));
            httpPost.setHeader("Content-Type", "application/json");
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                PausingReceptionist.RESPONSE
                        .set(IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            PausingReceptionist.RESPONSE.set("the request failed outright: " + e);
        }
    }

    private static int cancel(String clientContextId) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpDelete delete = new HttpDelete(RUNNING_REQUESTS + "?client_context_id=" + clientContextId);
            try (CloseableHttpResponse response = httpClient.execute(delete)) {
                return response.getStatusLine().getStatusCode();
            }
        }
    }

    /**
     * Holds a request's second statement in the window between it marking itself cancellable and its job being
     * submitted - where a real statement sits while it takes its locks and compiles.
     */
    public static class PausingReceptionist extends Receptionist {

        static final CountDownLatch COMPILING = new CountDownLatch(1);
        static final CountDownLatch RESUMED = new CountDownLatch(1);
        static final AtomicReference<String> RESPONSE = new AtomicReference<>("no response was read");
        private static final Map<String, AtomicInteger> SCHEDULED = new ConcurrentHashMap<>();
        private static volatile String armedClientContextId;

        static void armFor(String clientContextId) {
            armedClientContextId = clientContextId;
        }

        static void disarm() {
            armedClientContextId = null;
            SCHEDULED.clear();
        }

        @Override
        public void ensureSchedulable(ISchedulableClientRequest schedulableRequest) throws HyracksDataException {
            String armed = armedClientContextId;
            if (armed == null || !armed.equals(schedulableRequest.getRequestParameters().getClientContextId())) {
                return;
            }
            // the first statement runs; the second is held here, cancellable and with no job of its own yet
            if (SCHEDULED.computeIfAbsent(schedulableRequest.getClientRequest().getId(), id -> new AtomicInteger())
                    .incrementAndGet() != 2) {
                return;
            }
            COMPILING.countDown();
            try {
                // the cancel interrupts this wait, as it would a compile; the latch only bounds a cancel that misses
                RESUMED.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Serves the cluster with a receptionist that can hold a statement in that window. */
    private static class PausingCCApplication extends CCApplication {
        @Override
        protected ICcApplicationContext createApplicationContext(ILibraryManager libraryManager,
                IGlobalRecoveryManager globalRecoveryManager, INcLifecycleCoordinator lifecycleCoordinator,
                IReceptionistFactory receptionistFactory, IConfigValidatorFactory configValidatorFactory,
                CCExtensionManager ccExtensionManager, IAdapterFactoryService adapterFactoryService,
                IGlobalTxManager globalTxManager, IOManager ioManager, CloudProperties cloudProperties,
                INamespaceResolver namespaceResolver, INamespacePathResolver namespacePathResolver)
                throws AlgebricksException, IOException {
            return super.createApplicationContext(libraryManager, globalRecoveryManager, lifecycleCoordinator,
                    PausingReceptionist::new, configValidatorFactory, ccExtensionManager, adapterFactoryService,
                    globalTxManager, ioManager, cloudProperties, namespaceResolver, namespacePathResolver);
        }
    }
}
