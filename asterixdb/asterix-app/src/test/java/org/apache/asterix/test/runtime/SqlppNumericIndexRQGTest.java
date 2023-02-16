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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.api.http.server.QueryServiceRequestParameters;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.parser.SqlppHint;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
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
 * RQG testsuite for indexes on numeric fields
 */
@RunWith(Parameterized.class)
public class SqlppNumericIndexRQGTest {

    static final Logger LOGGER = LogManager.getLogger(SqlppNumericIndexRQGTest.class);

    static final String CONF_PROPERTY_SEED =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppNumericIndexRQGTest.class, "seed");
    static final long CONF_PROPERTY_SEED_DEFAULT = System.currentTimeMillis();

    static final String CONF_PROPERTY_LIMIT =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppNumericIndexRQGTest.class, "limit");
    static final int CONF_PROPERTY_LIMIT_DEFAULT = 50;

    static final String CONF_PROPERTY_OFFSET =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppNumericIndexRQGTest.class, "offset");
    static final int CONF_PROPERTY_OFFSET_DEFAULT = 0;

    static final String CONF_PROPERTY_OFFSET_QUERY =
            SqlppRQGTestBase.getConfigurationPropertyName(SqlppNumericIndexRQGTest.class, "offset.query");
    static final int CONF_PROPERTY_OFFSET_QUERY_DEFAULT = 0;

    static final Path OUTPUT_DIR = Paths.get("target", SqlppNumericIndexRQGTest.class.getSimpleName());

    static final String DATAVERSE_NAME = "dvTest";
    static final String DATASET_NAME_TYPED = "dsTyped";
    static final String DATASET_NAME_UNTYPED = "dsUntyped";
    static final String COMPILER_OPTION_FORMAT = "set `%s` '%s';";
    static final String ID_COLUMN_NAME = "id";

    static final List<AlgebricksBuiltinFunctions.ComparisonKind> CMP_KINDS =
            Arrays.asList(AlgebricksBuiltinFunctions.ComparisonKind.GT, AlgebricksBuiltinFunctions.ComparisonKind.GE,
                    AlgebricksBuiltinFunctions.ComparisonKind.LT, AlgebricksBuiltinFunctions.ComparisonKind.LE,
                    AlgebricksBuiltinFunctions.ComparisonKind.EQ);

    static final List<BuiltinType> FIELD_TYPES = Arrays.asList(BuiltinType.ANY, BuiltinType.AINT8, BuiltinType.AINT16,
            BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AFLOAT, BuiltinType.ADOUBLE);

    static final double MIN_VALUE = -3.5;
    static final double MAX_VALUE = 3.5;
    static final double PROBE_STEP = 1.0 / 4;
    static final double DATA_STEP = PROBE_STEP / 2;
    static final int NON_NUMERIC_DATA_RATIO = 8;
    static final int INDEX_ONLY_ENABLED_RATIO = 3;
    static final int INDEXED_AS_IS_RATIO = 4;
    static final int JOIN_RATIO = 8;

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final ObjectReader OBJECT_READER = OBJECT_MAPPER.readerFor(ObjectNode.class);
    static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc_no_cbo.conf";

    static TestExecutor testExecutor;

    private final TestInstance testInstance;

    @BeforeClass
    public static void setUp() throws Exception {
        testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor, false);

        StringBuilder sb = new StringBuilder(2048);
        addDropDataverse(sb, DATAVERSE_NAME);
        addCreateDataverse(sb, DATAVERSE_NAME);
        addCreateDataset(sb, DATAVERSE_NAME, DATASET_NAME_TYPED, false);
        addLoadDataset(sb, DATAVERSE_NAME, DATASET_NAME_TYPED);
        addCreateDataset(sb, DATAVERSE_NAME, DATASET_NAME_UNTYPED, true);
        addLoadDataset(sb, DATAVERSE_NAME, DATASET_NAME_UNTYPED);
        executeUpdateOrDdl(sb.toString());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameterized.Parameters(name = "SqlppNumericIndexRQGTest {index}: {0}")
    public static Collection<TestInstance> tests() {

        long seed = SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_SEED, CONF_PROPERTY_SEED_DEFAULT);
        int limit =
                (int) SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_LIMIT, CONF_PROPERTY_LIMIT_DEFAULT);
        int testOffset =
                (int) SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_OFFSET, CONF_PROPERTY_OFFSET_DEFAULT);
        int queryOffset = (int) SqlppRQGTestBase.getLongConfigurationProperty(CONF_PROPERTY_OFFSET_QUERY,
                CONF_PROPERTY_OFFSET_QUERY_DEFAULT);

        LOGGER.info(String.format("Testsuite configuration: -D%s=%d -D%s=%d -D%s=%d -D%s=%d", CONF_PROPERTY_SEED, seed,
                CONF_PROPERTY_LIMIT, limit, CONF_PROPERTY_OFFSET, testOffset, CONF_PROPERTY_OFFSET_QUERY, queryOffset));

        String indexName = "idx_" + seed;

        List<TestInstance> testCases = new ArrayList<>(limit);
        RandomGenerator random = new MersenneTwister(seed);

        int i = 0;
        while (i < limit) {
            BuiltinType fieldType = randomElement(FIELD_TYPES, random);

            BuiltinType indexedType;
            if (random.nextInt(INDEXED_AS_IS_RATIO) == 0) {
                if (fieldType.getTypeTag() == ATypeTag.ANY) {
                    continue;
                }
                indexedType = null;
            } else {
                indexedType = randomElement(FIELD_TYPES, random);
                if (indexedType.getTypeTag() == ATypeTag.ANY) {
                    continue;
                }
            }

            boolean join = random.nextInt(JOIN_RATIO) == 0;
            BuiltinType probeType;
            if (join) {
                probeType = indexedType != null ? indexedType : fieldType;
            } else {
                probeType = randomElement(FIELD_TYPES, random);
            }
            if (probeType.getTypeTag() == ATypeTag.ANY) {
                continue;
            }

            Set<TestOption> options = EnumSet.noneOf(TestOption.class);
            if (random.nextBoolean()) {
                options.add(TestOption.EXCLUDE_UNKNOWN_KEY);
            }
            if (indexedType != null && random.nextBoolean()) {
                options.add(TestOption.CAST_DEFAULT_NULL);
            }
            if (random.nextInt(INDEX_ONLY_ENABLED_RATIO) == 0) {
                options.add(TestOption.INDEX_ONLY_ENABLED);
            }
            if (join) {
                options.add(random.nextBoolean() ? TestOption.INNER_JOIN_QUERY : TestOption.LEFT_OUTER_JOIN_QUERY);
            }
            String searchedDatasetName = indexedType == null ? DATASET_NAME_TYPED : DATASET_NAME_UNTYPED;
            String probeSourceDatasetName = indexedType == null ? DATASET_NAME_UNTYPED : DATASET_NAME_TYPED;
            if (i >= testOffset) {
                TestInstance testCase = new TestInstance(i, DATAVERSE_NAME, searchedDatasetName, indexName, fieldType,
                        indexedType, probeType, probeSourceDatasetName, options, queryOffset);
                testCases.add(testCase);
            }
            i++;
        }

        return testCases;
    }

    public SqlppNumericIndexRQGTest(TestInstance testInstance) {
        this.testInstance = testInstance;
    }

    @Test
    public void test() throws Exception {
        LOGGER.info(testInstance);
        testInstance.execute();
    }

    private static boolean hasIndexSearch(ArrayNode planLines, String indexName) {
        for (int i = 0, n = planLines.size(); i < n; i++) {
            String line = planLines.get(i).textValue();
            if (line.contains(BuiltinFunctions.INDEX_SEARCH.getName()) && line.contains('"' + indexName + '"')) {
                return true;
            }
        }
        return false;
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

    private static void addDropDataverse(StringBuilder sb, String dataverseName) {
        sb.append(String.format("DROP DATAVERSE %s IF EXISTS;\n", dataverseName));
    }

    private static void addCreateDataverse(StringBuilder sb, String dataverseName) {
        sb.append(String.format("CREATE DATAVERSE %s;\n", dataverseName));
    }

    private static void addCreateDataset(StringBuilder sb, String dataverseName, String datasetName, boolean untyped) {
        sb.append("CREATE DATASET ").append(dataverseName).append('.').append(datasetName);
        sb.append(" (").append(ID_COLUMN_NAME).append(" string ");
        if (!untyped) {
            for (BuiltinType t : FIELD_TYPES) {
                if (t.getTypeTag() == ATypeTag.ANY) {
                    continue;
                }
                sb.append(", ").append(getColumnName(t)).append(' ').append(t.getTypeName());
            }
        }
        sb.append(") ");
        sb.append("OPEN TYPE PRIMARY KEY ").append(ID_COLUMN_NAME).append(";\n");
    }

    private static void addLoadDataset(StringBuilder sb, String dataverseName, String datasetName) {
        int id = 0, nonNumeric = 0;
        for (double v = MIN_VALUE; v <= MAX_VALUE; v += DATA_STEP) {
            addInsert(sb, dataverseName, datasetName, id, v);
            id++;

            // Insert non-numeric values once in a while
            if (NON_NUMERIC_DATA_RATIO > 0 && id % NON_NUMERIC_DATA_RATIO == 0) {
                Double nnv;
                switch (nonNumeric % 3) {
                    case 0:
                        nnv = null;
                        break;
                    case 1:
                        nnv = Double.POSITIVE_INFINITY;
                        break;
                    case 2:
                        nnv = Double.NEGATIVE_INFINITY;
                        break;
                    default:
                        throw new IllegalStateException(String.valueOf(nonNumeric));
                }
                addInsert(sb, dataverseName, datasetName, id, nnv);
                nonNumeric++;
                id++;
            }
        }
    }

    private static void addInsert(StringBuilder sb, String dataverseName, String datasetName, int id, Double v) {
        sb.append("INSERT INTO ").append(dataverseName).append('.').append(datasetName).append(" ( { ");
        sb.append("'").append(ID_COLUMN_NAME).append("': \"").append(datasetName).append(':').append(id).append('"');
        for (BuiltinType fieldType : FIELD_TYPES) {
            String columnName = getColumnName(fieldType);
            sb.append(", '").append(columnName).append("':");
            if (v == null) {
                sb.append("null");
            } else if (Double.isInfinite(v)) {
                sb.append(fieldType.getTypeTag() == ATypeTag.DOUBLE || fieldType.getTypeTag() == ATypeTag.FLOAT
                        ? String.format("%s('%sINF')", fieldType.getTypeName(), v < 0 ? "-" : "") : "null");
            } else {
                BuiltinType valueType;
                if (fieldType.getTypeTag() == ATypeTag.ANY) {
                    valueType = FIELD_TYPES.get(id % FIELD_TYPES.size());
                    if (valueType.getTypeTag() == ATypeTag.ANY) {
                        valueType = BuiltinType.ASTRING;
                    }
                } else {
                    valueType = fieldType;
                }
                String castFn = valueType.getTypeName();
                sb.append(castFn).append('(').append(v).append(')');
            }
        }
        sb.append("} );\n");
    }

    private static String getColumnName(BuiltinType t) {
        return "c_" + t.getTypeName();
    }

    private static <T> T randomElement(List<T> list, RandomGenerator randomGenerator) {
        return list.get(randomGenerator.nextInt(list.size()));
    }

    private static ArrayNode readLinesIntoArrayNode(String str) {
        try {
            ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            BufferedReader br = new BufferedReader(new StringReader(str));
            String line;
            while ((line = br.readLine()) != null) {
                arrayNode.add(line);
            }
            return arrayNode;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private enum TestOption {
        EXCLUDE_UNKNOWN_KEY,
        CAST_DEFAULT_NULL,
        INDEX_ONLY_ENABLED,
        JOIN_QUERY,
        INNER_JOIN_QUERY,
        LEFT_OUTER_JOIN_QUERY

    }

    private static class TestInstance {

        private final int id;

        private final String dataverseName;

        private final String datasetName;

        private final String indexName;

        private final BuiltinType fieldType;

        private final BuiltinType indexedType;

        private final BuiltinType probeType;

        private final String probeSourceDatasetName; // for JOIN query

        private final Set<TestOption> options;

        private final int queryOffset;

        public TestInstance(int id, String dataverseName, String datasetName, String indexName, BuiltinType fieldType,
                BuiltinType indexedType, BuiltinType probeType, String probeSourceDatasetName,
                Set<TestOption> testOptions, int queryOffset) {
            this.id = id;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.indexName = indexName;
            this.fieldType = fieldType;
            this.indexedType = indexedType;
            this.probeType = probeType;
            this.probeSourceDatasetName = probeSourceDatasetName;
            this.options = testOptions == null ? Collections.emptySet() : testOptions;
            this.queryOffset = queryOffset;
        }

        void execute() throws Exception {
            List<String> queries;
            if (options.contains(TestOption.INNER_JOIN_QUERY)) {
                queries = Collections.singletonList(generateJoinQuery(false));
            } else if (options.contains(TestOption.LEFT_OUTER_JOIN_QUERY)) {
                queries = Collections.singletonList(generateJoinQuery(true));
            } else {
                queries = new ArrayList<>((int) ((MAX_VALUE - MIN_VALUE) / PROBE_STEP) + 1);
                for (double v = MIN_VALUE; v <= MAX_VALUE; v += PROBE_STEP) {
                    for (AlgebricksBuiltinFunctions.ComparisonKind cmpKind : CMP_KINDS) {
                        queries.add(generateSearchQuery(cmpKind, v));
                    }
                }
            }

            int queryCount = queries.size();

            executeUpdateOrDdl(generateDropIndex());

            LOGGER.info(String.format("Running queries [%d to %d] without index ...", queryOffset, queryCount - 1));
            ArrayNode[] resNoIndex = new ArrayNode[queryCount];
            for (int i = queryOffset; i < queryCount; i++) {
                String query = queries.get(i);
                ArrayNode result = executeQuery(query, false).first;
                resNoIndex[i] = result;
            }

            LOGGER.info("Creating index");
            executeUpdateOrDdl(generateCreateIndex());

            LOGGER.info(String.format("Running queries [%d to %d] with index ...", queryOffset, queryCount - 1));
            for (int i = queryOffset; i < queryCount; i++) {
                String query = queries.get(i);
                ArrayNode rNoIndex = resNoIndex[i];
                Pair<ArrayNode, String> pWithIndex = executeQuery(query, true);
                ArrayNode planWithIndex = readLinesIntoArrayNode(pWithIndex.second);
                String comment = String.format("%s;%s", this, query);
                if (!hasIndexSearch(planWithIndex, indexName)) {
                    File fPlan = SqlppRQGTestBase.writeResult(OUTPUT_DIR, planWithIndex, id, "plan", comment,
                            (out, node) -> out.println(node.asText()));
                    Assert.fail(
                            String.format("Index was not used. Expected to find search of index [%s] in query plan: %s",
                                    indexName, fPlan.getAbsolutePath()));
                }
                ArrayNode rWithIndex = pWithIndex.first;
                if (!rNoIndex.equals(rWithIndex)) {
                    File fNoIndex = SqlppRQGTestBase.writeResult(OUTPUT_DIR, rNoIndex, id, "no_index", comment);
                    File fWithIndex = SqlppRQGTestBase.writeResult(OUTPUT_DIR, rWithIndex, id, "with_index", comment);
                    Assert.fail(
                            String.format("Different results for query #%d [%s].%nWithout index: %s%nWith index: %s", i,
                                    query, fNoIndex.getAbsolutePath(), fWithIndex.getAbsolutePath()));
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(64);
            sb.append('#').append(id).append(' ');
            sb.append(generateCreateIndex(true));
            sb.append(String.format(" PROBE(%s)", probeType.getTypeName()));
            if (options.contains(TestOption.INDEX_ONLY_ENABLED)) {
                sb.append(" INDEX_ONLY");
            }
            if (options.contains(TestOption.INNER_JOIN_QUERY)) {
                sb.append(" JOIN");
            } else if (options.contains(TestOption.LEFT_OUTER_JOIN_QUERY)) {
                sb.append(" LEFT JOIN");
            }
            return sb.toString();
        }

        private String generateDropIndex() {
            return String.format("DROP INDEX %s.%s.%s IF EXISTS", dataverseName, datasetName, indexName);
        }

        private String generateCreateIndex() {
            return generateCreateIndex(false);
        }

        private String generateCreateIndex(boolean displayOnly) {
            StringBuilder sb = new StringBuilder(64);
            String columnName = getColumnName(fieldType);
            if (!displayOnly) {
                sb.append("CREATE INDEX ").append(indexName).append(" ON ").append(dataverseName).append('.');
            }
            sb.append(datasetName).append("(").append(columnName);
            if (indexedType != null) {
                sb.append(':').append(indexedType.getTypeName());
            }
            sb.append(")");
            sb.append(" ");
            if (options.contains(TestOption.EXCLUDE_UNKNOWN_KEY)) {
                sb.append(displayOnly ? "-UNKN_KEY" : "EXCLUDE UNKNOWN KEY");
            } else {
                sb.append(displayOnly ? "+UNKN_KEY" : "INCLUDE UNKNOWN KEY");
            }
            if (options.contains(TestOption.CAST_DEFAULT_NULL)) {
                sb.append(" CAST(DEFAULT NULL)");
            }
            return sb.toString();
        }

        private String generateSearchQuery(AlgebricksBuiltinFunctions.ComparisonKind cmpKind, double p) {
            String columnName = getColumnName(fieldType);
            boolean castDefaultNull = options.contains(TestOption.CAST_DEFAULT_NULL);

            StringBuilder sb = new StringBuilder(128);
            addQueryProlog(sb);

            String indexedFieldCastFn = castDefaultNull ? getTypeConstructorFunction(indexedType) : "";
            String probeValueCastFn = probeType.getTypeName();

            sb.append(String.format("SELECT VALUE %s ", columnName));
            sb.append(String.format("FROM %s.%s ", dataverseName, datasetName));
            sb.append(String.format("WHERE %s(%s) %s %s(%s) ", indexedFieldCastFn, columnName, getCmpOp(cmpKind),
                    probeValueCastFn, p));
            sb.append(String.format("ORDER BY %s", columnName));
            return sb.toString();
        }

        private String generateJoinQuery(boolean leftOuterJoin) {
            if (!canJoin(fieldType, indexedType, probeType)) {
                throw new IllegalStateException();
            }

            String columnName = getColumnName(fieldType);
            String cmpOp = getCmpOp(AlgebricksBuiltinFunctions.ComparisonKind.EQ);
            boolean castDefaultNull = options.contains(TestOption.CAST_DEFAULT_NULL);

            StringBuilder sb = new StringBuilder(128);
            addQueryProlog(sb);

            String indexedFieldCastFn = castDefaultNull ? getTypeConstructorFunction(indexedType) : "";
            String probeValueCastFn = probeType.getTypeName();

            sb.append(String.format("SELECT t1.%s AS t1_%s, t2.%s AS t2_%s ", ID_COLUMN_NAME, ID_COLUMN_NAME,
                    ID_COLUMN_NAME, ID_COLUMN_NAME));
            sb.append(String.format("FROM %s.%s t1 ", dataverseName, probeSourceDatasetName));
            sb.append(String.format("%s JOIN %s.%s t2 ", leftOuterJoin ? "LEFT" : "", dataverseName, datasetName));
            sb.append(String.format("ON %s(t1.%s) /* +%s */ %s %s(t2.%s) ", probeValueCastFn, columnName,
                    SqlppHint.INDEXED_NESTED_LOOP_JOIN_HINT.getIdentifier(), cmpOp, indexedFieldCastFn, columnName));
            sb.append(String.format("ORDER BY t1_%s, t2_%s", ID_COLUMN_NAME, ID_COLUMN_NAME));
            return sb.toString();
        }

        private void addQueryProlog(StringBuilder sb) {
            sb.append(String.format(COMPILER_OPTION_FORMAT, CompilerProperties.COMPILER_SORT_PARALLEL_KEY, false));
            sb.append(String.format(COMPILER_OPTION_FORMAT, CompilerProperties.COMPILER_INDEXONLY_KEY,
                    options.contains(TestOption.INDEX_ONLY_ENABLED)));
            if (options.contains(TestOption.CAST_DEFAULT_NULL)) {
                sb.append(String.format(COMPILER_OPTION_FORMAT, FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, true));
            }
        }

        private String getTypeConstructorFunction(BuiltinType probeType) {
            return options.contains(TestOption.CAST_DEFAULT_NULL)
                    ? Objects.requireNonNull(TypeUtil.getTypeConstructorDefaultNull(probeType, false)).getName()
                            .replace('-', '_')
                    : probeType.getTypeName();
        }

        private String getCmpOp(AlgebricksBuiltinFunctions.ComparisonKind cmpKind) {
            switch (cmpKind) {
                case GT:
                    return ">";
                case GE:
                    return ">=";
                case LE:
                    return "<";
                case LT:
                    return "<=";
                case EQ:
                    return "=";
                case NEQ:
                default:
                    throw new IllegalStateException(String.valueOf(cmpKind));
            }
        }

        private static boolean canJoin(BuiltinType fieldType, BuiltinType indexedType, BuiltinType probeType) {
            return probeType.getTypeTag() == (indexedType != null ? indexedType : fieldType).getTypeTag();
        }
    }
}
