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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.AnalyzeStatement;
import org.apache.asterix.lang.common.statement.CreateIndexStatement;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.EntityDetails;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.translator.ClientRequest;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * CREATE INDEX and ANALYZE run their jobs without attaching them to the request, so its state is the only thing
 * that tells a build in progress apart from one still queued for its locks: it must read "running" once the build
 * has started and "received" until then.
 */
public class DdlRequestStateTest {

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil() {
        @Override
        protected ICCApplication createCCApplication() {
            return new PausingDdlCCApplication();
        }
    };
    private static final String QUERY_SERVICE = "http://localhost:19002/query/service";
    private static final String RUNNING_REQUESTS = "http://localhost:19002/admin/requests/running";
    private static final ObjectMapper OM = new ObjectMapper();

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
        String response = post(
                "drop dataverse test if exists; create dataverse test; use test;"
                        + " create dataset ds primary key (id: int); insert into ds ([{\"id\": 1, \"x\": 1}, {\"id\": 2}]);",
                null);
        Assert.assertTrue("setup failed: " + response, response.contains("\"success\""));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
    }

    @Test
    public void indexBeingBuiltIsRunning() throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        PausingQueryTranslator.armFor(clientContextId);
        try {
            AtomicReference<String> response = new AtomicReference<>("the create index never answered");
            Thread createIndex = new Thread(
                    () -> response.set(post("use test; create index idx_x on ds(x: int);", clientContextId)));
            createIndex.start();

            Assert.assertTrue("the create index never started building",
                    PausingQueryTranslator.building.await(60, TimeUnit.SECONDS));
            JsonNode request = runningRequest(clientContextId);
            Assert.assertEquals("an index being built must be reported as running: " + request, "running",
                    request.get("state").asText());
            Assert.assertFalse("a create index is not cancellable: " + request, request.get("cancellable").asBoolean());
            PausingQueryTranslator.resumed.countDown();
            createIndex.join(TimeUnit.MINUTES.toMillis(1));

            Assert.assertTrue("the create index did not succeed: " + response.get(),
                    response.get().contains("\"success\""));
        } finally {
            PausingQueryTranslator.disarm();
        }
    }

    @Test
    public void indexQueuedForItsLocksIsReceived() throws Exception {
        assertReceivedWhileQueued("use test; create index idx_y on ds(y: int);");
    }

    @Test
    public void existingIndexIsNeverRunning() throws Exception {
        String created = post("use test; create index idx_z on ds(z: int);", null);
        Assert.assertTrue("the index was not created: " + created, created.contains("\"success\""));
        Assert.assertEquals("an index that already exists builds nothing, so it must not be reported as running",
                "received", stateOnceExecuted("use test; create index idx_z if not exists on ds(z: int);", true));
    }

    @Test
    public void analyzedDatasetIsRunning() throws Exception {
        Assert.assertEquals("a dataset being analyzed must be reported as running", "running",
                stateOnceExecuted("use test; analyze dataset ds;", true));
    }

    @Test
    public void analyzeQueuedForItsLocksIsReceived() throws Exception {
        assertReceivedWhileQueued("use test; analyze dataset ds;");
    }

    @Test
    public void analyzeOfMissingDatasetIsNeverRunning() throws Exception {
        Assert.assertEquals(
                "an analyze that fails its validation builds nothing, so it must not be reported as running",
                "received", stateOnceExecuted("use test; analyze dataset nosuchds;", false));
    }

    /** Asserts that the statement reads "received" while it waits for the dataset's lock, then lets it run. */
    private static void assertReceivedWhileQueued(String statement) throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        ICcApplicationContext appCtx = (ICcApplicationContext) INTEGRATION_UTIL.cc.getApplicationContext();
        LockList locks = new LockList();
        AtomicReference<String> response = new AtomicReference<>("the statement never answered");
        Thread request = new Thread(() -> response.set(post(statement, clientContextId)));
        appCtx.getMetadataLockManager().acquireDatasetWriteLock(locks, MetadataConstants.DEFAULT_DATABASE,
                DataverseName.createSinglePartName("test"), "ds");
        try {
            PausingQueryTranslator.awaitEntryFor(clientContextId);
            request.start();

            Assert.assertTrue("the statement never reached its locks",
                    PausingQueryTranslator.entered.await(60, TimeUnit.SECONDS));
            awaitBlocked(clientContextId, appCtx);
            JsonNode tracked = runningRequest(clientContextId);
            Assert.assertEquals("a statement waiting for its locks must not be reported as running: " + tracked,
                    "received", tracked.get("state").asText());
        } finally {
            locks.unlock();
        }
        request.join(TimeUnit.MINUTES.toMillis(1));
        Assert.assertTrue("the statement did not succeed: " + response.get(), response.get().contains("\"success\""));
    }

    /**
     * Executes the statement, holding the request once the statement is done but before it is reported to the
     * client, and returns the state the request had at that point.
     */
    private static String stateOnceExecuted(String statement, boolean expectSuccess) throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        PausingQueryTranslator.holdAfterFor(clientContextId);
        try {
            AtomicReference<String> response = new AtomicReference<>("the statement never answered");
            Thread request = new Thread(() -> response.set(post(statement, clientContextId)));
            request.start();

            Assert.assertTrue("the statement never finished",
                    PausingQueryTranslator.finished.await(60, TimeUnit.SECONDS));
            String state = runningRequest(clientContextId).get("state").asText();
            PausingQueryTranslator.released.countDown();
            request.join(TimeUnit.MINUTES.toMillis(1));

            Assert.assertEquals("unexpected outcome: " + response.get(), expectSuccess,
                    response.get().contains("\"success\""));
            return state;
        } finally {
            PausingQueryTranslator.disarm();
        }
    }

    /** Waits for the thread executing the request to block, which with the dataset locked is on that lock. */
    private static void awaitBlocked(String clientContextId, ICcApplicationContext appCtx) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            IClientRequest request = appCtx.getRequestTracker().getByClientContextId(clientContextId);
            Thread executor = request instanceof ClientRequest ? ((ClientRequest) request).getExecutor() : null;
            if (executor != null && executor.getState() == Thread.State.WAITING) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        Assert.fail("the statement never blocked on the dataset lock");
    }

    private static JsonNode runningRequest(String clientContextId) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse httpResponse = httpClient.execute(new HttpGet(RUNNING_REQUESTS))) {
            JsonNode requests = OM.readTree(httpResponse.getEntity().getContent());
            for (JsonNode request : requests) {
                if (clientContextId.equals(request.path("clientContextID").asText())) {
                    return request;
                }
            }
            throw new AssertionError("no running request for " + clientContextId + " in " + requests);
        }
    }

    private static String post(String statement, String clientContextId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(QUERY_SERVICE);
            StringBuilder body =
                    new StringBuilder("{\"statement\": \"").append(statement.replace("\"", "\\\"")).append("\"");
            if (clientContextId != null) {
                body.append(", \"client_context_id\": \"").append(clientContextId).append('"');
            }
            httpPost.setEntity(new StringEntity(body.append('}').toString(), StandardCharsets.UTF_8));
            httpPost.setHeader("Content-Type", "application/json");
            try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
                return IOUtils.toString(httpResponse.getEntity().getContent(), StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            return "the request failed outright: " + e;
        }
    }

    /**
     * Signals a CREATE INDEX or ANALYZE of the armed request before it takes its locks, and can hold one once it
     * has finished but has not yet been reported to the client, or a CREATE INDEX once its build has started, where
     * a real statement sits while its jobs build the index.
     */
    public static class PausingQueryTranslator extends QueryTranslator {

        // armed afresh by each case: a latch another case's disarm released would hold nothing
        static volatile CountDownLatch entered = new CountDownLatch(1);
        static volatile CountDownLatch building = new CountDownLatch(1);
        static volatile CountDownLatch resumed = new CountDownLatch(1);
        static volatile CountDownLatch finished = new CountDownLatch(1);
        static volatile CountDownLatch released = new CountDownLatch(1);
        private static volatile String enteringClientContextId;
        private static volatile String pausingClientContextId;
        private static volatile String holdingClientContextId;
        private boolean pauseInBuild;

        public PausingQueryTranslator(ICcApplicationContext appCtx, List<Statement> statements, SessionOutput output,
                ILangCompilationProvider compilationProvider, ExecutorService executorService,
                IResponsePrinter responsePrinter) {
            super(appCtx, statements, output, compilationProvider, executorService, responsePrinter);
        }

        static void awaitEntryFor(String clientContextId) {
            entered = new CountDownLatch(1);
            enteringClientContextId = clientContextId;
        }

        static void armFor(String clientContextId) {
            building = new CountDownLatch(1);
            resumed = new CountDownLatch(1);
            pausingClientContextId = clientContextId;
        }

        static void holdAfterFor(String clientContextId) {
            finished = new CountDownLatch(1);
            released = new CountDownLatch(1);
            holdingClientContextId = clientContextId;
        }

        static void disarm() {
            pausingClientContextId = null;
            holdingClientContextId = null;
            resumed.countDown();
            released.countDown();
        }

        private static boolean isFor(String clientContextId, IRequestParameters requestParameters) {
            return clientContextId != null && clientContextId.equals(requestParameters.getClientContextId());
        }

        @Override
        public void handleCreateIndexStatement(MetadataProvider metadataProvider, Statement stmt,
                IHyracksClientConnection hcc, IRequestParameters requestParameters, Creator creator) throws Exception {
            if (isFor(enteringClientContextId, requestParameters)) {
                entered.countDown();
            }
            super.handleCreateIndexStatement(metadataProvider, stmt, hcc, requestParameters, creator);
        }

        @Override
        protected void doCreateIndex(MetadataProvider metadataProvider, CreateIndexStatement stmtCreateIndex,
                String databaseName, DataverseName dataverseName, String datasetName, IHyracksClientConnection hcc,
                IRequestParameters requestParameters, Creator creator) throws Exception {
            pauseInBuild = isFor(pausingClientContextId, requestParameters);
            super.doCreateIndex(metadataProvider, stmtCreateIndex, databaseName, dataverseName, datasetName, hcc,
                    requestParameters, creator);
            holdIfArmed(requestParameters);
        }

        @Override
        protected void handleAnalyzeStatement(MetadataProvider metadataProvider, Statement stmt,
                IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception {
            if (isFor(enteringClientContextId, requestParameters)) {
                entered.countDown();
            }
            super.handleAnalyzeStatement(metadataProvider, stmt, hcc, requestParameters);
        }

        @Override
        protected void doAnalyzeDataset(MetadataProvider metadataProvider, AnalyzeStatement stmtAnalyze,
                String databaseName, DataverseName dataverseName, String datasetName, IHyracksClientConnection hcc,
                IRequestParameters requestParameters) throws Exception {
            try {
                super.doAnalyzeDataset(metadataProvider, stmtAnalyze, databaseName, dataverseName, datasetName, hcc,
                        requestParameters);
            } finally {
                holdIfArmed(requestParameters);
            }
        }

        private static void holdIfArmed(IRequestParameters requestParameters) throws InterruptedException {
            if (isFor(holdingClientContextId, requestParameters)) {
                finished.countDown();
                released.await(60, TimeUnit.SECONDS);
            }
        }

        @Override
        protected void beforeTxnCommit(MetadataProvider metadataProvider, Creator creator, EntityDetails entityDetails)
                throws AlgebricksException {
            if (pauseInBuild) {
                pauseInBuild = false;
                building.countDown();
                try {
                    resumed.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            super.beforeTxnCommit(metadataProvider, creator, entityDetails);
        }
    }

    /** Serves the cluster with a statement executor that can hold a CREATE INDEX or an ANALYZE. */
    private static class PausingDdlCCApplication extends CCApplication {
        @Override
        public IStatementExecutorFactory getStatementExecutorFactory() {
            return new DefaultStatementExecutorFactory(ccServiceCtx.getControllerService().getExecutor()) {
                @Override
                public IStatementExecutor create(ICcApplicationContext appCtx, List<Statement> statements,
                        SessionOutput output, ILangCompilationProvider compilationProvider,
                        IStorageComponentProvider storageComponentProvider, IResponsePrinter responsePrinter) {
                    return new PausingQueryTranslator(appCtx, statements, output, compilationProvider, executorService,
                            responsePrinter);
                }
            };
        }
    }
}
