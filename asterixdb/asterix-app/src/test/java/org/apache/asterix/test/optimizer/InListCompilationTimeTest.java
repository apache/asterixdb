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
package org.apache.asterix.test.optimizer;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.api.java.AsterixJavaClient;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.NamespaceResolver;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.translator.SessionConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Compiles queries with a long IN list of constants and bounds the time each takes. Handled in time linear in the
 * length of the list, one compiles in about a second; the index-selection rules have handled such a list in time
 * quadratic in its length, which takes minutes. The bound sits far from both, so it detects the growth rate rather
 * than the speed of the machine.
 */
@RunWith(Parameterized.class)
public class InListCompilationTimeTest {

    private static final String CONFIG_FILE = "src/main/resources/cc_no_cbo.conf";
    private static final int LIST_LENGTH = 30_000;
    private static final long MAX_COMPILE_SECONDS = 15;

    private static final String DDL = "DROP DATAVERSE test IF EXISTS; CREATE DATAVERSE test; USE test;"
            + " CREATE TYPE T AS { id: bigint };" + " CREATE DATASET ds(T) PRIMARY KEY id;"
            + " CREATE INDEX idx_a ON ds(a: bigint);" + " CREATE INDEX idx_ca ON ds(c: bigint, b: bigint);";

    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    private final String query;
    private final boolean cbo;
    private final String expectedIndex;

    public InListCompilationTimeTest(String name, String query, boolean cbo, String expectedIndex) {
        this.query = query;
        this.cbo = cbo;
        this.expectedIndex = expectedIndex;
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> shapes() {
        String list = list(1);
        Object[][] shapes = { { "secondary index", "WHERE d.a IN " + list, "idx_a" },
                { "no index", "WHERE d.b IN " + list, null }, { "primary index", "WHERE d.id IN " + list, "ds" },
                { "composite index", "WHERE d.c = 1 AND d.b IN " + list, "idx_ca" }, { "two IN lists on one key",
                        "WHERE d.a IN " + list + " AND d.a IN " + list(LIST_LENGTH / 2), "idx_a" } };
        List<Object[]> tests = new ArrayList<>();
        for (Object[] shape : shapes) {
            String query = "SELECT VALUE d.id FROM ds d " + shape[1] + ";";
            tests.add(new Object[] { shape[0] + ", no CBO", query, false, shape[2] });
            // Without statistics, CBO may prefer a scan; it still costs every candidate index.
            tests.add(new Object[] { shape[0] + ", CBO", query, true, null });
        }
        return tests;
    }

    private static String list(long first) {
        return LongStream.range(first, first + LIST_LENGTH).mapToObj(Long::toString)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, CONFIG_FILE);
        integrationUtil.init(true, CONFIG_FILE);
        compile(DDL);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void test() throws Exception {
        String script = "USE test; SET `compiler.cbo` \"" + cbo + "\"; " + query;
        long start = System.nanoTime();
        String plan = compile(script);
        long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        Assert.assertTrue("compiling an IN list of " + LIST_LENGTH + " took " + seconds + " s",
                seconds < MAX_COMPILE_SECONDS);
        if (expectedIndex != null) {
            Assert.assertTrue("the plan does not search " + expectedIndex,
                    plan.contains("index-search(\"" + expectedIndex + "\""));
        }
    }

    private static String compile(String script) throws Exception {
        AsterixJavaClient client = new AsterixJavaClient(
                (ICcApplicationContext) integrationUtil.cc.getApplicationContext(),
                integrationUtil.getHyracksClientConnection(), new StringReader(script),
                new PrintWriter(Writer.nullWriter()), new SqlppCompilationProvider(new NamespaceResolver(false)),
                new DefaultStatementExecutorFactory(), new StorageComponentProvider());
        client.compile(true, false, false, true, false, false, false, SessionConfig.PlanFormat.STRING, false);
        return client.getExecutionPlans().getOptimizedLogicalPlan();
    }
}
