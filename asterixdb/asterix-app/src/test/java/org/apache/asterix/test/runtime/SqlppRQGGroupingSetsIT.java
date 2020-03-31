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
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.test.common.ExtractedResult;
import org.apache.asterix.test.common.ResultExtractor;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.common.TestHelper;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

// Prerequisite:
// setenv TESTCONTAINERS_RYUK_DISABLED true

@RunWith(Parameterized.class)
public class SqlppRQGGroupingSetsIT {

    private static final String CONF_PROPERTY_SEED = getConfigurationPropertyName("seed");

    private static final long CONF_PROPERTY_SEED_DEFAULT = System.currentTimeMillis();

    private static final String CONF_PROPERTY_LIMIT = getConfigurationPropertyName("limit");

    private static final int CONF_PROPERTY_LIMIT_DEFAULT = 100;

    private static final String TESTCONTAINERS_RYUK_DISABLED = "TESTCONTAINERS_RYUK_DISABLED";

    private static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";

    private static final String POSTGRES_IMAGE = "postgres:12.2";

    private static final String TABLE_NAME = "tenk";

    private static final Path TABLE_FILE = Paths.get("data", "tenk.tbl");

    private static final char TABLE_FILE_COLUMN_SEPARATOR = '|';

    private static final Path RESULT_OUTPUT_DIR = Paths.get("target", SqlppRQGGroupingSetsIT.class.getSimpleName());

    private static final int ROLLUP_ELEMENT_LIMIT = 2;
    private static final int CUBE_ELEMENT_LIMIT = 1;
    private static final int GROUPING_SETS_ELEMENT_LIMIT = 2;
    private static final int MULTI_ELEMENT_LIMIT = 2;

    private static final String UNIQUE_1 = "unique1";
    private static final String UNIQUE_2 = "unique2";
    private static final String TWO = "two";
    private static final String FOUR = "four";
    private static final String TEN = "ten";
    private static final String TWENTY = "twenty";
    private static final String HUNDRED = "hundred";
    private static final String THOUSAND = "thousand";
    private static final String TWOTHOUSAND = "twothousand";
    private static final String FIVETHOUS = "fivethous";
    private static final String TENTHOUS = "tenthous";
    private static final String ODD100 = "odd100";
    private static final String EVEN100 = "even100";
    private static final String STRINGU1 = "stringu1";
    private static final String STRINGU2 = "stringu2";
    private static final String STRING4 = "string4";

    private static final List<String> GROUPBY_COLUMNS = Arrays.asList(TWO, FOUR, TEN, TWENTY, HUNDRED, ODD100, EVEN100);

    private static final LinkedHashMap<String, JDBCType> TABLE_SCHEMA = createTableSchema();

    private static final ObjectReader JSON_NODE_READER = new ObjectMapper().readerFor(JsonNode.class);

    private static final Logger LOGGER = LogManager.getLogger(SqlppRQGGroupingSetsIT.class);

    private static TestExecutor testExecutor;

    private static PostgreSQLContainer<?> postgres;

    private static Connection conn;

    private static Statement stmt;

    private final int testcaseId;

    private final String sqlQuery;

    private final String sqlppQuery;

    private final String groupByClause;

    @Parameters(name = "SqlppRQGGroupingSetsIT {index}: {3}")
    public static Collection<Object[]> tests() {
        List<Object[]> testCases = new ArrayList<>();

        long seed = getLongConfigurationProperty(CONF_PROPERTY_SEED, CONF_PROPERTY_SEED_DEFAULT);
        int limit = (int) getLongConfigurationProperty(CONF_PROPERTY_LIMIT, CONF_PROPERTY_LIMIT_DEFAULT);

        LOGGER.info(String.format("Testsuite configuration: -D%s=%d -D%s=%d", CONF_PROPERTY_SEED, seed,
                CONF_PROPERTY_LIMIT, limit));

        Random random = new Random(seed);
        for (int i = 0; i < limit; i++) {
            TestQuery query = generateQuery(i, random);
            testCases.add(new Object[] { i, query.sqlQuery, query.sqlppQuery, query.groupbyClause });
        }

        return testCases;
    }

    public SqlppRQGGroupingSetsIT(int testcaseId, String sqlQuery, String sqlppQuery, String groupByClause) {
        this.testcaseId = testcaseId;
        this.sqlQuery = sqlQuery;
        this.sqlppQuery = sqlppQuery;
        this.groupByClause = groupByClause;
    }

    @Test
    public void test() throws Exception {
        LOGGER.info(String.format("Starting testcase #%d: %s", testcaseId, groupByClause));

        LOGGER.info("Running SQL");
        LOGGER.info(sqlQuery);
        stmt.execute(sqlQuery);
        ArrayNode sqlResult;
        try (ResultSet rs = stmt.getResultSet()) {
            sqlResult = asJson(rs);
        }

        LOGGER.info("Running SQL++");
        LOGGER.info(sqlppQuery);
        ArrayNode sqlppResult;
        try (InputStream resultStream = testExecutor.executeQueryService(sqlppQuery,
                testExecutor.getEndpoint(Servlets.QUERY_SERVICE), TestCaseContext.OutputFormat.ADM)) {
            sqlppResult = asJson(
                    ResultExtractor.extract(resultStream, StandardCharsets.UTF_8, TestCaseContext.OutputFormat.ADM));
        }

        boolean eq = TestHelper.equalJson(sqlResult, sqlppResult, false);
        if (!eq) {
            File sqlResultFile = writeResult(sqlResult, "sql");
            File sqlppResultFile = writeResult(sqlppResult, "sqlpp");

            Assert.fail(String.format("Results do not match.\n%s\n%s", sqlResultFile.getCanonicalPath(),
                    sqlppResultFile.getCanonicalPath()));
        }
    }

    private static TestQuery generateQuery(int testcaseId, Random random) {
        Set<String> allColumns = new LinkedHashSet<>();
        List<String> groupingElements = new ArrayList<>();
        int nElements = 1 + random.nextInt(3);

        int rollupCount = 0, cubeCount = 0, groupingSetsCount = 0;
        for (int i = 0; i < nElements; i++) {
            String prefix;
            int minItems, maxItems;
            boolean allowSimpleSubgroup = false, allowComplexSubgroup = false;

            switch (random.nextInt(6)) {
                case 0:
                case 1:
                    prefix = "";
                    minItems = 1;
                    maxItems = 4;
                    break;
                case 2:
                case 3:
                    if (isSingleElementLimitReached(rollupCount, ROLLUP_ELEMENT_LIMIT)
                            || isMultiElementLimitReached(rollupCount, cubeCount, groupingSetsCount)) {
                        // skip this element
                        nElements++;
                        continue;
                    }
                    prefix = "ROLLUP";
                    minItems = 1;
                    maxItems = 4;
                    allowSimpleSubgroup = true;
                    rollupCount++;
                    break;
                case 4:
                    if (isSingleElementLimitReached(cubeCount, CUBE_ELEMENT_LIMIT)
                            || isMultiElementLimitReached(rollupCount, cubeCount, groupingSetsCount)) {
                        // skip this element
                        nElements++;
                        continue;
                    }
                    prefix = "CUBE";
                    minItems = 2;
                    maxItems = 2;
                    allowSimpleSubgroup = true; // allowed, not actually used, because we set maxItems to 2
                    cubeCount++;
                    break;
                case 5:
                    if (isSingleElementLimitReached(groupingSetsCount, GROUPING_SETS_ELEMENT_LIMIT)
                            || isMultiElementLimitReached(rollupCount, cubeCount, groupingSetsCount)) {
                        // skip this element
                        nElements++;
                        continue;
                    }
                    prefix = "GROUPING SETS";
                    minItems = 0;
                    maxItems = 3;
                    allowSimpleSubgroup = allowComplexSubgroup = true;
                    groupingSetsCount++;
                    break;
                default:
                    throw new IllegalStateException();
            }
            int nItems = minItems + random.nextInt(maxItems - minItems + 1);
            List<String> elementItems =
                    nItems == 0 ? Collections.emptyList() : randomize(GROUPBY_COLUMNS, random).subList(0, nItems);
            allColumns.addAll(elementItems);
            if (allowSimpleSubgroup && nItems >= 3 && random.nextInt(2) == 0) {
                makeSubgroup(elementItems, random, allowComplexSubgroup);
            }
            String elementItemsText = elementItems.isEmpty() ? "()" : String.join(",", elementItems);
            String element = String.format("%s(%s)", prefix, elementItemsText);
            groupingElements.add(element);
        }

        StringBuilder selectClause = new StringBuilder();
        for (String col : allColumns) {
            selectClause.append(col).append(',');
        }
        for (String col : allColumns) {
            selectClause.append(String.format("GROUPING(%s) AS grp_%s", col, col)).append(',');
        }
        if (allColumns.size() > 1) {
            selectClause.append(String.format("GROUPING(%s) AS grp", String.join(",", randomize(allColumns, random))))
                    .append(',');
        }
        selectClause.append(String.format("SUM(%s) AS agg_sum", UNIQUE_1));

        String groupingElementText = groupingElements.isEmpty() ? "()" : String.join(",", groupingElements);
        String groupbyClause = String.format("GROUP BY %s", groupingElementText);

        String orderbyClauseSql = generateOrderBy(allColumns, true);
        String orderbyClauseSqlpp = generateOrderBy(allColumns, false);

        String queryTemplate = "SELECT %s FROM %s %s %s";
        String sqlQuery = String.format(queryTemplate, selectClause, TABLE_NAME, groupbyClause, orderbyClauseSql);
        String sqlppQuery = String.format(queryTemplate, selectClause, TABLE_NAME, groupbyClause, orderbyClauseSqlpp);

        LOGGER.info(String.format("Testcase #%d: %s", testcaseId, groupbyClause));

        return new TestQuery(sqlQuery, sqlppQuery, groupbyClause);
    }

    private static boolean isSingleElementLimitReached(int elementCount, int limit) {
        return elementCount >= limit;
    }

    private static boolean isMultiElementLimitReached(int elementCount1, int elementCount2, int elementCount3) {
        return elementCount1 + elementCount2 + elementCount3 >= MULTI_ELEMENT_LIMIT;
    }

    private static String generateOrderBy(Set<String> allColumns, boolean insertNullsFirst) {
        if (allColumns.isEmpty()) {
            return "";
        }
        return "ORDER BY " + allColumns.stream().map(c -> c + (insertNullsFirst ? " NULLS FIRST" : ""))
                .collect(Collectors.joining(", "));
    }

    private static void makeSubgroup(List<String> elementColumns, Random random, boolean allowComplexSubgroup) {
        // rewrite (a, b, c, ... ) into (a,(b,c), ...) or (a,ROLLUP(b,c), ...)
        String subgroupSpecifier = "";
        if (allowComplexSubgroup && random.nextInt(2) == 0) {
            subgroupSpecifier = "ROLLUP";
        }
        int start = random.nextInt(elementColumns.size() - 1);
        List<String> sublist = elementColumns.subList(start, start + 2);
        String s = String.format("%s(%s)", subgroupSpecifier, String.join(",", sublist));
        sublist.clear();
        sublist.add(s);
    }

    private ArrayNode asJson(ExtractedResult aresult) throws IOException {
        ArrayNode result = (ArrayNode) JSON_NODE_READER.createArrayNode();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(aresult.getResult(), StandardCharsets.UTF_8))) {
            reader.lines().forEachOrdered(l -> {
                try {
                    result.add(JSON_NODE_READER.readTree(l));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return result;
    }

    private ArrayNode asJson(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int rsColumnCount = rsmd.getColumnCount();
        ArrayNode result = (ArrayNode) JSON_NODE_READER.createArrayNode();
        while (rs.next()) {
            ObjectNode row = (ObjectNode) JSON_NODE_READER.createObjectNode();
            for (int i = 0; i < rsColumnCount; i++) {
                int jdbcColumnIdx = i + 1;
                String columnName = rsmd.getColumnName(jdbcColumnIdx);
                switch (rsmd.getColumnType(jdbcColumnIdx)) {
                    case Types.INTEGER:
                        int intValue = rs.getInt(jdbcColumnIdx);
                        if (rs.wasNull()) {
                            row.putNull(columnName);
                        } else {
                            row.put(columnName, intValue);
                        }
                        break;
                    case Types.BIGINT:
                        long longValue = rs.getLong(jdbcColumnIdx);
                        if (rs.wasNull()) {
                            row.putNull(columnName);
                        } else {
                            row.put(columnName, longValue);
                        }
                        break;
                    case Types.VARCHAR:
                        String stringValue = rs.getString(jdbcColumnIdx);
                        if (rs.wasNull()) {
                            row.putNull(columnName);
                        } else {
                            row.put(columnName, stringValue);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            result.add(row);
        }
        return result;
    }

    private static void loadAsterixData() throws Exception {
        String tableTypeName = TABLE_NAME + "Type";
        String createTypeStmtText =
                String.format("CREATE TYPE %s AS CLOSED { %s }", tableTypeName,
                        TABLE_SCHEMA.entrySet().stream()
                                .map(e -> e.getKey() + ':' + getAsterixType(e.getValue()).getTypeName())
                                .collect(Collectors.joining(",")));

        LOGGER.debug(createTypeStmtText);
        testExecutor.executeSqlppUpdateOrDdl(createTypeStmtText, TestCaseContext.OutputFormat.ADM);

        String createDatasetStmtText =
                String.format("CREATE DATASET %s(%s) PRIMARY KEY %s", TABLE_NAME, tableTypeName, UNIQUE_2);
        LOGGER.debug(createDatasetStmtText);
        testExecutor.executeSqlppUpdateOrDdl(createDatasetStmtText, TestCaseContext.OutputFormat.ADM);

        String loadStmtText =
                String.format("LOAD DATASET %s USING localfs ((`path`=`%s`),(`format`=`%s`),(`delimiter`=`%s`))",
                        TABLE_NAME, "asterix_nc1://" + TABLE_FILE, "delimited-text", "|");
        LOGGER.debug(loadStmtText);
        testExecutor.executeSqlppUpdateOrDdl(loadStmtText, TestCaseContext.OutputFormat.ADM);
    }

    private static void loadSQLData() throws SQLException, IOException {
        String createTableStmtText = String.format("CREATE TEMPORARY TABLE %s (%s)", TABLE_NAME, TABLE_SCHEMA.entrySet()
                .stream().map(e -> e.getKey() + ' ' + getSQLType(e.getValue())).collect(Collectors.joining(",")));

        stmt.execute(createTableStmtText);

        String insertStmtText = String.format("INSERT INTO %s VALUES (%s)", TABLE_NAME,
                StringUtils.repeat("?", ",", TABLE_SCHEMA.size()));

        try (PreparedStatement insertStmt = conn.prepareStatement(insertStmtText)) {
            Files.lines(TABLE_FILE).forEachOrdered(line -> {
                String[] values = StringUtils.split(line, TABLE_FILE_COLUMN_SEPARATOR);
                try {
                    insertStmt.clearParameters();
                    int i = 0;
                    for (JDBCType type : TABLE_SCHEMA.values()) {
                        setColumnValue(insertStmt, i + 1, type, values[i]);
                        i++;
                    }
                    insertStmt.addBatch();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            insertStmt.executeBatch();
        }
    }

    private static LinkedHashMap<String, JDBCType> createTableSchema() {
        LinkedHashMap<String, JDBCType> schema = new LinkedHashMap<>();
        schema.put(UNIQUE_1, JDBCType.INTEGER);
        schema.put(UNIQUE_2, JDBCType.INTEGER);
        schema.put(TWO, JDBCType.INTEGER);
        schema.put(FOUR, JDBCType.INTEGER);
        schema.put(TEN, JDBCType.INTEGER);
        schema.put(TWENTY, JDBCType.INTEGER);
        schema.put(HUNDRED, JDBCType.INTEGER);
        schema.put(THOUSAND, JDBCType.INTEGER);
        schema.put(TWOTHOUSAND, JDBCType.INTEGER);
        schema.put(FIVETHOUS, JDBCType.INTEGER);
        schema.put(TENTHOUS, JDBCType.INTEGER);
        schema.put(ODD100, JDBCType.INTEGER);
        schema.put(EVEN100, JDBCType.INTEGER);
        schema.put(STRINGU1, JDBCType.VARCHAR);
        schema.put(STRINGU2, JDBCType.VARCHAR);
        schema.put(STRING4, JDBCType.VARCHAR);
        return schema;
    }

    private static String getSQLType(JDBCType type) {
        String suffix = "";
        if (type == JDBCType.VARCHAR) {
            suffix = "(256)";
        }
        return type.getName() + suffix;
    }

    private static IAType getAsterixType(JDBCType type) {
        switch (type) {
            case INTEGER:
                return BuiltinType.AINT32;
            case VARCHAR:
                return BuiltinType.ASTRING;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static void setColumnValue(PreparedStatement stmt, int jdbcParamIdx, JDBCType type, String value)
            throws SQLException {
        switch (type) {
            case INTEGER:
                stmt.setInt(jdbcParamIdx, Integer.parseInt(value));
                break;
            case VARCHAR:
                stmt.setString(jdbcParamIdx, value);
                break;
            default:
                throw new UnsupportedOperationException(type.getName());
        }
    }

    private static <T> List<T> randomize(Collection<T> input, Random random) {
        List<T> output = new ArrayList<>(input);
        Collections.shuffle(output, random);
        return output;
    }

    private static String getConfigurationPropertyName(String propertyName) {
        return String.format("%s.%s", SqlppRQGGroupingSetsIT.class.getSimpleName(), propertyName);
    }

    private static long getLongConfigurationProperty(String propertyName, long defValue) {
        String textValue = System.getProperty(propertyName);
        if (textValue == null) {
            return defValue;
        }
        try {
            return Long.parseLong(textValue);
        } catch (NumberFormatException e) {
            LOGGER.warn(String.format("Cannot parse configuration property: %s. Will use default value: %d",
                    propertyName, defValue));
            return defValue;
        }
    }

    private File writeResult(ArrayNode result, String resultKind) throws IOException {
        String outFileName = String.format("%d.%s.txt", testcaseId, resultKind);
        File outFile = new File(RESULT_OUTPUT_DIR.toFile(), outFileName);
        try (PrintWriter pw = new PrintWriter(outFile, StandardCharsets.UTF_8.name())) {
            pw.print("---");
            pw.println(groupByClause);
            for (int i = 0, ln = result.size(); i < ln; i++) {
                pw.println(ResultExtractor.prettyPrint(result.get(i)));
            }
        }
        return outFile;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        startAsterix();
        startPostgres();
        FileUtils.forceMkdir(RESULT_OUTPUT_DIR.toFile());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopPostgres();
        stopAsterix();
    }

    private static void startAsterix() throws Exception {
        testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        loadAsterixData();
    }

    private static void stopAsterix() throws Exception {
        LangExecutionUtil.tearDown();
    }

    private static void startPostgres() throws SQLException, IOException {
        if (!Boolean.parseBoolean(System.getenv(TESTCONTAINERS_RYUK_DISABLED))) {
            throw new IllegalStateException(
                    String.format("Set environment variable %s=%s", TESTCONTAINERS_RYUK_DISABLED, true));
        }
        LOGGER.info("Starting Postgres");
        postgres = new PostgreSQLContainer<>(POSTGRES_IMAGE);
        postgres.start();
        conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        stmt = conn.createStatement();
        loadSQLData();
    }

    private static void stopPostgres() {
        LOGGER.info("Stopping Postgres");
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (postgres != null) {
            try {
                postgres.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class TestQuery {
        final String sqlQuery;
        final String sqlppQuery;
        final String groupbyClause;

        TestQuery(String sqlQuery, String sqlppQuery, String groupbyClause) {
            this.sqlQuery = sqlQuery;
            this.sqlppQuery = sqlppQuery;
            this.groupbyClause = groupbyClause;
        }
    }
}
