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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.asterix.api.http.server.QueryServiceRequestParameters;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.lang.sqlpp.parser.SqlppHint;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * RQG testsuite for hash joins.
 * Tests:
 * <ul>
 * <li> Fields with / without NULL or MISSING values
 * <li> Inner / Left Outer joins</li>
 * <li> Repartitioning / Broadcast joins </li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class SqlppHashJoinRQJTest {

    static final Logger LOGGER = LogManager.getLogger(SqlppHashJoinRQJTest.class);

    static final String CONF_PROPERTY_SEED =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppHashJoinRQJTest.class, "seed");
    static final long CONF_PROPERTY_SEED_DEFAULT = System.currentTimeMillis();

    static final String CONF_PROPERTY_LIMIT =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppHashJoinRQJTest.class, "limit");
    static final int CONF_PROPERTY_LIMIT_DEFAULT = 40;

    static final String CONF_PROPERTY_OFFSET =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppHashJoinRQJTest.class, "offset");
    static final int CONF_PROPERTY_OFFSET_DEFAULT = 0;

    static final Path OUTPUT_DIR = Paths.get("target", SqlppHashJoinRQJTest.class.getSimpleName());

    static final String DATAVERSE_NAME = "dvTest";
    static final String[] DATASET_NAMES = new String[] { "ds1", "ds2" };
    static final String ID_COLUMN_NAME = "id";
    static final String BASE_COLUMN_NAME = "i";
    static final List<Integer> DATASET_ROWS = Arrays.asList(20000, 40000);
    static final List<Integer> DATASET_COLUMNS = Arrays.asList(4, 10, 100, 1000, 10000);
    static final int DATASET_COLUMN_LENGTH_MIN =
            String.valueOf(DATASET_COLUMNS.stream().mapToInt(Integer::intValue).max().orElse(0)).length();
    static final int DATASET_COLUMN_LENGTH_MAX = Math.max(20, DATASET_COLUMN_LENGTH_MIN);
    static final int NULLABLE_COLUMN_RATIO = 2;
    static final int OUTER_JOIN_RATIO = 3;
    static final int BROADCAST_RATIO = 4;

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final ObjectReader OBJECT_READER = OBJECT_MAPPER.readerFor(ObjectNode.class);

    static long datasetRowCount;
    static int datasetColumnLength;
    static TestExecutor testExecutor;

    final TestInstance testInstance;

    public SqlppHashJoinRQJTest(TestInstance testInstance) {
        this.testInstance = testInstance;
    }

    @Parameterized.Parameters(name = "SqlppHashJoinRQJTest {index}: {0}")
    public static Collection<TestInstance> tests() {
        long seed = SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_SEED, CONF_PROPERTY_SEED_DEFAULT);
        int limit =
                (int) SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_LIMIT, CONF_PROPERTY_LIMIT_DEFAULT);
        int testOffset =
                (int) SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_OFFSET, CONF_PROPERTY_OFFSET_DEFAULT);

        LOGGER.info(String.format("Testsuite configuration: -D%s=%d -D%s=%d -D%s=%d", CONF_PROPERTY_SEED, seed,
                CONF_PROPERTY_LIMIT, limit, CONF_PROPERTY_OFFSET, testOffset));

        RandomGenerator random = new MersenneTwister(seed);
        datasetRowCount = randomElement(DATASET_ROWS, random);
        datasetColumnLength =
                DATASET_COLUMN_LENGTH_MIN + random.nextInt(DATASET_COLUMN_LENGTH_MAX - DATASET_COLUMN_LENGTH_MIN);

        LOGGER.info(String.format("Dataset row count=%d, column length=%d", datasetRowCount, datasetColumnLength));

        LinkedHashMap<IntBuffer, TestInstance> testCases = new LinkedHashMap<>();
        int i = 0;
        while (i < limit) {
            int c0 = randomElement(DATASET_COLUMNS, random);
            boolean c0nullable = random.nextInt(NULLABLE_COLUMN_RATIO) == 0;
            int c1 = randomElement(DATASET_COLUMNS, random);
            boolean c1nullable = random.nextInt(NULLABLE_COLUMN_RATIO) == 0;
            boolean outerJoin = random.nextInt(OUTER_JOIN_RATIO) == 0;
            boolean broadcast = random.nextInt(BROADCAST_RATIO) == 0;
            TestInstance test = new TestInstance(i, c0, c0nullable, c1, c1nullable, outerJoin, broadcast);
            IntBuffer testSignature = test.signature();
            if (testCases.containsKey(testSignature)) {
                continue;
            }
            if (i >= testOffset) {
                testCases.put(testSignature, test);
            }
            i++;
        }
        return testCases.values();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(SqlppRQGTestBase.TEST_CONFIG_FILE_NAME, testExecutor, false);

        FileUtils.forceMkdir(OUTPUT_DIR.toFile());
        for (String datasetName : DATASET_NAMES) {
            Path datasetFilePath = OUTPUT_DIR.resolve(datasetName + ".adm");
            LOGGER.info("Writing data file: " + datasetFilePath.toAbsolutePath());
            try (PrintWriter pw = new PrintWriter(datasetFilePath.toFile())) {
                for (int i = 0; i < datasetRowCount; i++) {
                    writeRecord(pw, datasetName, i);
                }
            }
        }

        StringBuilder sb = new StringBuilder(2048);
        addDropDataverse(sb, DATAVERSE_NAME);
        addCreateDataverse(sb, DATAVERSE_NAME);
        for (String datasetName : DATASET_NAMES) {
            addCreateDataset(sb, DATAVERSE_NAME, datasetName);
            addLoadDataset(sb, DATAVERSE_NAME, datasetName);
        }
        executeUpdateOrDdl(sb.toString());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Test
    public void test() throws Exception {
        LOGGER.info(testInstance);
        testInstance.execute();
    }

    private static void addDropDataverse(StringBuilder sb, String dataverseName) {
        sb.append(String.format("DROP DATAVERSE %s IF EXISTS;\n", dataverseName));
    }

    private static void addCreateDataverse(StringBuilder sb, String dataverseName) {
        sb.append(String.format("CREATE DATAVERSE %s;\n", dataverseName));
    }

    private static void addCreateDataset(StringBuilder sb, String dataverseName, String datasetName) {
        sb.append("CREATE DATASET ").append(dataverseName).append('.').append(datasetName);
        sb.append(" (").append(ID_COLUMN_NAME).append(" string");
        sb.append(") ");
        sb.append("OPEN TYPE PRIMARY KEY ").append(ID_COLUMN_NAME).append(";\n");
    }

    private static void addLoadDataset(StringBuilder sb, String dataverseName, String datasetName) {
        sb.append(String.format(
                "LOAD DATASET %s.%s USING localfs((`path`=`asterix_nc1://%s/%s.adm`),(`format`=`adm`));%n",
                dataverseName, datasetName, OUTPUT_DIR, datasetName));
    }

    private static void writeRecord(PrintWriter pw, String datasetName, int id) throws IOException {
        pw.print("{");
        pw.print(String.format("\"%s\": \"%s:%d\"", ID_COLUMN_NAME, datasetName, id));
        int nColumns = DATASET_COLUMNS.size();
        for (int i = 0; i < nColumns; i++) {
            long c = DATASET_COLUMNS.get(i);
            writeColumn(pw, c, false, id); // no NULL/MISSING
            writeColumn(pw, c, true, id); // with NULL/MISSING
        }
        pw.println("}");
    }

    private static String getColumnName(long c, boolean nullable) {
        return BASE_COLUMN_NAME + c + (nullable ? "n" : "");
    }

    private static void writeColumn(Appendable out, long c, boolean nullable, long id) throws IOException {
        String columnName = getColumnName(c, nullable);
        boolean isNull = false;
        long v;
        if (nullable) {
            long r = id % (2 * c);
            if (r < c) {
                v = r + 1;
            } else if (r % 2 == 0) {
                v = 0;
                isNull = true;
            } else {
                // MISSING -> nothing to do
                return;
            }
        } else {
            long r = id % c;
            v = r + 1;
        }
        String text;
        if (isNull) {
            text = "null";
        } else {
            int cLen = datasetColumnLength;
            StringBuilder textBuilder = new StringBuilder(cLen + 2);
            textBuilder.append('"').append(v);
            int pad = cLen - (textBuilder.length() - 1);
            for (int i = 0; i < pad; i++) {
                textBuilder.append(' ');
            }
            textBuilder.append('"');
            text = textBuilder.toString();
        }
        out.append(String.format(",\"%s\":%s", columnName, text));
    }

    private static void executeUpdateOrDdl(String statement) throws Exception {
        LOGGER.debug("Executing: " + statement);
        testExecutor.executeSqlppUpdateOrDdl(statement, TestCaseContext.OutputFormat.CLEAN_JSON);
    }

    private static Pair<ArrayNode, String> executeQuery(String query, boolean fetchPlan) throws Exception {
        LOGGER.debug("Executing: " + query);

        List<TestCase.CompilationUnit.Parameter> params;
        if (fetchPlan) {
            TestCase.CompilationUnit.Parameter planParameter = new TestCase.CompilationUnit.Parameter();
            planParameter.setName(QueryServiceRequestParameters.Parameter.OPTIMIZED_LOGICAL_PLAN.str());
            planParameter.setValue(Boolean.TRUE.toString());
            planParameter.setType(ParameterTypeEnum.STRING);
            params = Collections.singletonList(planParameter);
        } else {
            params = Collections.emptyList();
        }

        try (InputStream resultStream = testExecutor.executeQueryService(query, TestCaseContext.OutputFormat.CLEAN_JSON,
                testExecutor.getEndpoint(Servlets.QUERY_SERVICE), params, true, StandardCharsets.UTF_8)) {
            JsonNode r = OBJECT_READER.readTree(resultStream);
            JsonNode errors = r.get("errors");
            if (errors != null) {
                Assert.fail("Query failed: " + errors);
            }
            JsonNode results = r.get("results");
            if (!results.isArray()) {
                Assert.fail("Expected array result, got: " + results);
            }
            ArrayNode resultsArray = (ArrayNode) results;
            String plan = fetchPlan ? r.get("plans").get("optimizedLogicalPlan").asText() : null;
            return new Pair<>(resultsArray, plan);
        }
    }

    private static <T> T randomElement(List<T> list, RandomGenerator randomGenerator) {
        return list.get(randomGenerator.nextInt(list.size()));
    }

    private static class TestInstance {

        private final int id;

        private final int c0;
        private final int c1;
        private final boolean c0nullable;
        private final boolean c1nullable;
        private final String col0;
        private final String col1;
        private final boolean outerJoin;
        private final boolean broadcastJoin;

        public TestInstance(int id, int c0, boolean c0nullable, int c1, boolean c1nullable, boolean outerJoin,
                boolean broadcastJoin) {
            this.id = id;
            this.outerJoin = outerJoin;
            this.c0 = c0;
            this.c1 = c1;
            this.c0nullable = c0nullable;
            this.c1nullable = c1nullable;
            this.broadcastJoin = broadcastJoin;
            this.col0 = getColumnName(c0, c0nullable);
            this.col1 = getColumnName(c1, c1nullable);
        }

        IntBuffer signature() {
            return IntBuffer.wrap(
                    new int[] { c0, toInt(c0nullable), c1, toInt(c1nullable), toInt(outerJoin), toInt(broadcastJoin) });
        }

        void execute() throws Exception {
            String query = createQuery();
            Pair<ArrayNode, String> res = executeQuery(query, true);
            String plan = res.second;
            if (!plan.contains(PhysicalOperatorTag.HYBRID_HASH_JOIN.toString())) {
                Assert.fail(PhysicalOperatorTag.HYBRID_HASH_JOIN + " operator was not used in query plan " + plan);
            }
            if (broadcastJoin && !plan.contains(PhysicalOperatorTag.BROADCAST_EXCHANGE.toString())) {
                Assert.fail(PhysicalOperatorTag.BROADCAST_EXCHANGE + " operator was not used in query plan " + plan);
            }
            ArrayNode resultArray = res.first;

            long expectedRowCount;
            long expectedRowCountInnerJoin = Math.min(c0, c1);
            if (outerJoin) {
                expectedRowCount = expectedRowCountInnerJoin + (c0nullable ? 2 : 0) + Math.max(0, c0 - c1);
            } else {
                expectedRowCount = expectedRowCountInnerJoin;
            }

            long expectedAggCountInnerJoin = (datasetRowCount * datasetRowCount) / (((long) c0) * c1)
                    / (c0nullable ? 2 : 1) / (c1nullable ? 2 : 1);

            int actualRowCount = resultArray.size();
            if (actualRowCount != expectedRowCount) {
                String commentHash = String.format("%s;%s", this, query);
                File fHash = SqlppRQGTestBase.writeResult(OUTPUT_DIR, resultArray, id, "hash", commentHash);
                Assert.fail(String.format("Unexpected row count %d for query #%d [%s]. Expected row count: %d %n %s ",
                        actualRowCount, id, this, expectedRowCount, fHash.getAbsolutePath()));
            }

            String col0Alias = String.format("%s_%s", DATASET_NAMES[0], col0);

            for (int i = 0; i < actualRowCount; i++) {
                JsonNode resultRecord = resultArray.get(i);
                long actualAggCount = resultRecord.get("cnt").longValue();

                long expectedAggCount;
                if (outerJoin) {
                    JsonNode col0Node = resultRecord.get(col0Alias);
                    if (col0Node == null || col0Node.isNull()) {
                        expectedAggCount = datasetRowCount / 4;
                    } else {
                        if (getValueAsLong(col0Node) > c1) {
                            expectedAggCount = datasetRowCount / (c0nullable ? 2 : 1) / c0;
                        } else {
                            expectedAggCount = expectedAggCountInnerJoin;
                        }
                    }
                } else {
                    expectedAggCount = expectedAggCountInnerJoin;
                }

                if (actualAggCount != expectedAggCount) {
                    String commentHash = String.format("%s;%s", this, query);
                    File fHash = SqlppRQGTestBase.writeResult(OUTPUT_DIR, resultArray, id, "hash", commentHash);
                    Assert.fail(String.format(
                            "Unexpected agg count %d in row %d for query #%d [%s]. Expected agg count: %d %n %s ",
                            actualAggCount, i, id, this, expectedAggCount, fHash.getAbsolutePath()));
                }
            }
        }

        private long getValueAsLong(JsonNode node) throws Exception {
            String text = node.textValue().trim();
            if (text.isEmpty()) {
                throw new Exception("Unexpected result value: " + node);
            }
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e) {
                throw new Exception("Unexpected result value: " + node);
            }
        }

        String createQuery() {
            return String.format(
                    "USE %s; SELECT t1.%s AS %s_%s, t2.%s AS %s_%s, count(*) AS cnt FROM %s t1 %s JOIN %s t2 ON t1.%s /*%s*/ = t2.%s /*%s*/ GROUP BY t1.%s, t2.%s ORDER BY t1.%s, t2.%s",
                    DATAVERSE_NAME, col0, DATASET_NAMES[0], col0, col1, DATASET_NAMES[1], col1, DATASET_NAMES[0],
                    getJoinKind(), DATASET_NAMES[1], col0, getJoinHint(), col1, getGroupByHint(), col0, col1, col0,
                    col1);
        }

        private String getJoinKind() {
            return outerJoin ? "LEFT OUTER" : "INNER";
        }

        private String getJoinHint() {
            return broadcastJoin ? "+" + SqlppHint.HASH_BROADCAST_JOIN_HINT.getIdentifier() : "";
        }

        private String getGroupByHint() {
            return "";
        }

        @Override
        public String toString() {
            return String.format("%s ON %s=%s %s", getJoinKind(), col0, col1, getJoinHint());
        }

        static int toInt(boolean b) {
            return b ? 1 : 0;
        }
    }
}
