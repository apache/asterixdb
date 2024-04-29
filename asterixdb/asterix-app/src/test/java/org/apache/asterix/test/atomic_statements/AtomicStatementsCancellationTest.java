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
package org.apache.asterix.test.atomic_statements;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.asterix.api.http.server.QueryServiceRequestParameters.Parameter.CLIENT_ID;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.common.LocalCloudUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AtomicStatementsCancellationTest {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private static final String TEST_CONFIG_FILE_NAME = "cc-cloud-storage.conf";
    private static final TestExecutor TEST_EXECUTOR = new TestExecutor();

    private static final String TEST_CONFIG_PATH =
            joinPath(System.getProperty("user.dir"), "src", "test", "resources");;
    private static final String TEST_CONFIG_FILE_PATH = TEST_CONFIG_PATH + File.separator + TEST_CONFIG_FILE_NAME;
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    private static final String DATASET_NAME = "ds_0";
    private static final int BATCH_SIZE = 20000;
    private static final int NUM_UPSERTS = 100;

    @Before
    public void setUp() throws Exception {
        boolean cleanStart = true;
        LocalCloudUtil.startS3CloudEnvironment(cleanStart);
        integrationUtil.setGracefulShutdown(true);
        integrationUtil.init(true, TEST_CONFIG_FILE_PATH);
        createDatasets();
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    private void createDatasets() throws Exception {
        TestDataUtil.createDatasetWithoutType(DATASET_NAME, Map.of("id", "uuid"), true);
        TestDataUtil.createSecondaryBTreeIndex(DATASET_NAME, DATASET_NAME + "_sidx", "name:string");
    }

    public String generateInsertStatement(String dataset, long count) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            stringBuilder.append("{\"name\": \"name_" + i + "\"},");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return "INSERT INTO " + dataset + "([" + stringBuilder + "]);";
    }

    private int cancelQuery(URI uri, List<TestCase.CompilationUnit.Parameter> params) throws Exception {
        HttpResponse response = TEST_EXECUTOR.executeHttpRequest(TestExecutor.constructDeleteMethod(uri, params));
        return response.getStatusLine().getStatusCode();
    }

    @Test
    public void testAtomicityWithCancellation() throws Exception {
        Random rnd = new Random();
        for (int j = 0; j < NUM_UPSERTS; j++) {
            String clientContextId = UUID.randomUUID().toString();
            final List<TestCase.CompilationUnit.Parameter> params = new ArrayList<>();
            TestCase.CompilationUnit.Parameter newParam = new TestCase.CompilationUnit.Parameter();
            newParam.setName(CLIENT_ID.str());
            newParam.setType(ParameterTypeEnum.STRING);
            newParam.setValue(clientContextId);
            params.add(newParam);
            String statement = generateInsertStatement(DATASET_NAME, BATCH_SIZE);
            Callable<InputStream> upsert = () -> {
                try {
                    return TEST_EXECUTOR.executeQueryService(statement, TestCaseContext.OutputFormat.CLEAN_JSON,
                            TEST_EXECUTOR.getEndpoint(Servlets.QUERY_SERVICE), params, false, UTF_8);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            };
            Future<InputStream> future = executor.submit(upsert);
            if (!future.isDone()) {
                Thread.sleep(rnd.nextInt(900) + 800);
                // Cancels the query request while the query is executing.
                int rc = cancelQuery(TEST_EXECUTOR.getEndpoint(Servlets.RUNNING_REQUESTS), params);
                Assert.assertTrue(rc == 200 || rc == 404 || rc == 403);
            }
            while (!future.isDone()) {
                Thread.sleep(100);
            }
        }
    }
}
