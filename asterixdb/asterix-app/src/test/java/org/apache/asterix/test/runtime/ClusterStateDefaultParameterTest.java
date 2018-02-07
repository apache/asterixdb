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

package org.apache.asterix.test.runtime;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Runs the cluster state runtime tests with the storage parallelism.
 */
@RunWith(Parameterized.class)
public class ClusterStateDefaultParameterTest {
    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc4.conf";

    @BeforeClass
    public static void setUp() throws Exception {
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, new TestExecutor());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "ClusterStateExecutionTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_cluster_state.xml", "cluster_state.xml");
    }

    protected TestCaseContext tcCtx;

    public ClusterStateDefaultParameterTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL("http://localhost:19002/admin/cluster/node/asterix_nc1/config");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        long maxHeap = Runtime.getRuntime().maxMemory();
        int matchCount = 0;
        String[] rows = result.toString().split(",");
        for (String row : rows) {
            if (row.contains("storage.buffercache.size")) {
                Assert.assertTrue(getValue(row) == maxHeap / 4);
                matchCount++;
            }
            if (row.contains("storage.memorycomponent.globalbudget")) {
                Assert.assertTrue(getValue(row) == maxHeap / 4);
                matchCount++;
            }
            if (row.contains("storage.max.active.writable.datasets")) {
                Assert.assertTrue(getValue(row) == 8);
                matchCount++;
            }
        }
        Assert.assertTrue(matchCount == 3);
    }

    // Parses a long value parameter.
    private long getValue(String row) {
        String valueStr = row.split(":")[1].trim();
        return Long.parseLong(valueStr);
    }
}
