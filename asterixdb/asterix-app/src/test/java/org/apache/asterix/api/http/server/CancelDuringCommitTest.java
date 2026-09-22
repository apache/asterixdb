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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.app.cc.GlobalTxManager;
import org.apache.asterix.common.cluster.IGlobalTxManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.nc.io.IOManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Committing the job of an atomic statement is a two-phase commit that cannot be undone by cancelling: its second
 * wait sits after the commit messages have gone to the nodes, and an interrupt there throws, whereupon the caller
 * aborts a transaction the nodes may already have committed. A request in that window must therefore refuse a
 * cancel rather than have its thread interrupted.
 */
public class CancelDuringCommitTest {

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil() {
        @Override
        protected ICCApplication createCCApplication() {
            return new PausingCommitCCApplication();
        }
    };
    private static final String QUERY_SERVICE = "http://localhost:19002/query/service";
    private static final String RUNNING_REQUESTS = "http://localhost:19002/admin/requests/running";

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
        // a dataset declared without a type is atomic, so inserting into it commits through the global manager
        post("drop dataverse test if exists; create dataverse test;"
                + " use test; create dataset reviews primary key (id: int);", null);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
    }

    @Test
    public void cancelIsRefusedWhileTheTransactionCommits() throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        PausingGlobalTxManager.armFor();
        try {
            AtomicReference<String> response = new AtomicReference<>("the insert never answered");
            Thread insert = new Thread(() -> response
                    .set(post("use test; insert into reviews ([{\"id\": 1}, {\"id\": 2}]);", clientContextId)));
            insert.start();

            Assert.assertTrue("the insert never reached its commit",
                    PausingGlobalTxManager.COMMITTING.await(60, TimeUnit.SECONDS));
            Assert.assertEquals("a request committing its transaction must refuse a cancel", 403,
                    cancel(clientContextId));
            PausingGlobalTxManager.RESUMED.countDown();
            insert.join(TimeUnit.MINUTES.toMillis(1));

            Assert.assertFalse("the commit was interrupted by the cancel", PausingGlobalTxManager.interrupted());
            Assert.assertTrue("the insert did not succeed: " + response.get(), response.get().contains("\"success\""));
        } finally {
            PausingGlobalTxManager.disarm();
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

    private static int cancel(String clientContextId) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpDelete delete = new HttpDelete(RUNNING_REQUESTS + "?client_context_id=" + clientContextId);
            try (CloseableHttpResponse httpResponse = httpClient.execute(delete)) {
                return httpResponse.getStatusLine().getStatusCode();
            }
        }
    }

    /** Holds a request in the window in which its transaction is being committed. */
    public static class PausingGlobalTxManager extends GlobalTxManager {

        static final CountDownLatch COMMITTING = new CountDownLatch(1);
        static final CountDownLatch RESUMED = new CountDownLatch(1);
        private static volatile boolean armed;
        private static volatile boolean interrupted;

        public PausingGlobalTxManager(ICCServiceContext serviceContext, IOManager ioManager) {
            super(serviceContext, ioManager);
        }

        static void armFor() {
            armed = true;
        }

        static void disarm() {
            armed = false;
        }

        static boolean interrupted() {
            return interrupted;
        }

        @Override
        public void commitTransaction(JobId jobId) throws ACIDException {
            if (armed) {
                armed = false;
                COMMITTING.countDown();
                try {
                    // stands in for the commit's own waits, where an interrupt would tear the two-phase commit
                    RESUMED.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    Thread.currentThread().interrupt();
                }
            }
            super.commitTransaction(jobId);
        }
    }

    /** Serves the cluster with a transaction manager that can hold a commit open. */
    private static class PausingCommitCCApplication extends CCApplication {
        @Override
        protected IGlobalTxManager createGlobalTxManager(IOManager ioManager) {
            return new PausingGlobalTxManager(ccServiceCtx, ioManager);
        }
    }
}
