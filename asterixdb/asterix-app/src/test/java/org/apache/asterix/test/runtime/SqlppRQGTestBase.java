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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
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
import org.junit.Assert;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

// Prerequisite:
// setenv TESTCONTAINERS_RYUK_DISABLED true

public abstract class SqlppRQGTestBase {

    private static final Logger LOGGER = LogManager.getLogger(SqlppRQGTestBase.class);

    protected static final String TESTCONTAINERS_RYUK_DISABLED = "TESTCONTAINERS_RYUK_DISABLED";

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";

    protected static final String POSTGRES_IMAGE = "postgres:12.2";

    protected static final String TABLE_NAME = "tenk";

    protected static final Path TABLE_FILE = Paths.get("data", "tenk.tbl");

    protected static final char TABLE_FILE_COLUMN_SEPARATOR = '|';

    protected static final String UNIQUE_1 = "unique1";
    protected static final String UNIQUE_2 = "unique2";
    protected static final String TWO = "two";
    protected static final String FOUR = "four";
    protected static final String TEN = "ten";
    protected static final String TWENTY = "twenty";
    protected static final String HUNDRED = "hundred";
    protected static final String THOUSAND = "thousand";
    protected static final String TWOTHOUSAND = "twothousand";
    protected static final String FIVETHOUS = "fivethous";
    protected static final String TENTHOUS = "tenthous";
    protected static final String ODD100 = "odd100";
    protected static final String EVEN100 = "even100";
    protected static final String STRINGU1 = "stringu1";
    protected static final String STRINGU2 = "stringu2";
    protected static final String STRING4 = "string4";

    protected static final LinkedHashMap<String, JDBCType> TABLE_SCHEMA = createTableSchema();

    protected static final ObjectReader JSON_NODE_READER = new ObjectMapper().readerFor(JsonNode.class);

    protected static TestExecutor testExecutor;

    protected static PostgreSQLContainer<?> postgres;

    protected static Connection conn;

    protected static Statement stmt;

    protected final Path outputDir = Paths.get("target", getClass().getSimpleName());

    public static void setUpBeforeClass() throws Exception {
        startAsterix();
        startPostgres();
    }

    public static void tearDownAfterClass() throws Exception {
        stopPostgres();
        stopAsterix();
    }

    protected ArrayNode asJson(ExtractedResult aresult) throws IOException {
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

    protected void runTestCase(int testcaseId, String testcaseDescription, String sqlQuery, String sqlppQuery)
            throws Exception {
        LOGGER.info(String.format("Starting testcase #%d: %s", testcaseId, testcaseDescription));

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

        File sqlResultFile = writeResult(sqlResult, testcaseId, "sql", testcaseDescription);
        File sqlppResultFile = writeResult(sqlppResult, testcaseId, "sqlpp", testcaseDescription);

        if (!eq) {
            /*
            File sqlResultFile = writeResult(sqlResult, testcaseId, "sql", testcaseDescription);
            File sqlppResultFile = writeResult(sqlppResult, testcaseId, "sqlpp", testcaseDescription);
            */

            Assert.fail(String.format("Results do not match.\n%s\n%s", sqlResultFile.getCanonicalPath(),
                    sqlppResultFile.getCanonicalPath()));
        }
    }

    protected ArrayNode asJson(ResultSet rs) throws SQLException {
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

    protected static void loadAsterixData() throws Exception {
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

    protected static void loadSQLData() throws SQLException, IOException {
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

    protected static LinkedHashMap<String, JDBCType> createTableSchema() {
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

    protected static String getSQLType(JDBCType type) {
        String suffix = "";
        if (type == JDBCType.VARCHAR) {
            suffix = "(256)";
        }
        return type.getName() + suffix;
    }

    protected static IAType getAsterixType(JDBCType type) {
        switch (type) {
            case INTEGER:
                return BuiltinType.AINT32;
            case VARCHAR:
                return BuiltinType.ASTRING;
            default:
                throw new UnsupportedOperationException();
        }
    }

    protected static void setColumnValue(PreparedStatement stmt, int jdbcParamIdx, JDBCType type, String value)
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

    protected File writeResult(ArrayNode result, int testcaseId, String resultKind, String comment) throws IOException {
        File outDir = outputDir.toFile();
        String outFileName = String.format("%d.%s.txt", testcaseId, resultKind);
        FileUtils.forceMkdir(outDir);
        File outFile = new File(outDir, outFileName);
        try (PrintWriter pw = new PrintWriter(outFile, StandardCharsets.UTF_8.name())) {
            pw.print("---");
            pw.println(comment);
            for (int i = 0, ln = result.size(); i < ln; i++) {
                pw.println(ResultExtractor.prettyPrint(result.get(i)));
            }
        }
        return outFile;
    }

    protected static <T> List<T> randomize(Collection<T> input, Random random) {
        List<T> output = new ArrayList<>(input);
        Collections.shuffle(output, random);
        return output;
    }

    protected static String getConfigurationPropertyName(Class<?> testClass, String propertyName) {
        return String.format("%s.%s", testClass.getSimpleName(), propertyName);
    }

    protected static long getLongConfigurationProperty(String propertyName, long defValue) {
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

    protected static void startAsterix() throws Exception {
        testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);
        loadAsterixData();
    }

    protected static void stopAsterix() throws Exception {
        LangExecutionUtil.tearDown();
    }

    protected static void startPostgres() throws SQLException, IOException {
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

    protected static void stopPostgres() {
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
}
