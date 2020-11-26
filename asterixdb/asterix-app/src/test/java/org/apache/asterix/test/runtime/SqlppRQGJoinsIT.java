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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntUnaryOperator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SqlppRQGJoinsIT extends SqlppRQGTestBase {

    private final int testcaseId;

    private final String sqlQuery;

    private final String sqlppQuery;

    private final String desc;

    static final String PROJECT_FIELD = "unique1";

    static final String JOIN_FIELD = "unique2";

    static final String FILTER_FIELD = JOIN_FIELD;

    static final char[] SHAPES = new char[] { 'c', 's' }; //TODO: 'q'

    @Parameterized.Parameters(name = "SqlppRQGJoinsIT {index}: {3}")
    public static Collection<Object[]> tests() {
        List<Object[]> testCases = new ArrayList<>();

        IntUnaryOperator filterComputer = i -> 2 * (i + 1);

        String[] allJoinKinds = new String[] { "INNER", "LEFT", "RIGHT" };
        String[] queryJoinKinds = new String[3];
        int id = 0;

        for (String jk0 : allJoinKinds) {
            queryJoinKinds[0] = jk0;
            TestQuery q1 = generateQuery(queryJoinKinds, 1, filterComputer, SHAPES[0]);
            testCases.add(new Object[] { id++, q1.sqlQuery, q1.sqlppQuery, q1.summary });

            for (char s : SHAPES) {
                for (String jk1 : allJoinKinds) {
                    queryJoinKinds[1] = jk1;
                    TestQuery q2 = generateQuery(queryJoinKinds, 2, filterComputer, s);
                    testCases.add(new Object[] { id++, q2.sqlQuery, q2.sqlppQuery, q2.summary });

                    for (String jk2 : allJoinKinds) {
                        queryJoinKinds[2] = jk2;
                        TestQuery q3 = generateQuery(queryJoinKinds, 3, filterComputer, s);
                        testCases.add(new Object[] { id++, q3.sqlQuery, q3.sqlppQuery, q3.summary });
                    }
                }
            }
        }

        return testCases;
    }

    private static TestQuery generateQuery(String[] joinKinds, int joinKindsSize, IntUnaryOperator filterComputer,
            char shape) {
        int tCount = joinKindsSize + 1;
        List<String> tDefs = new ArrayList<>(tCount);
        for (int i = 0; i < tCount; i++) {
            int filterValue = filterComputer.applyAsInt(i);
            String tDef = String.format("SELECT %s, %s FROM %s WHERE %s < %d", PROJECT_FIELD, JOIN_FIELD, TABLE_NAME,
                    FILTER_FIELD, filterValue);
            tDefs.add(tDef);
        }

        StringBuilder joinCondBuilderSql = new StringBuilder(128);
        StringBuilder joinCondBuilderSqlpp = new StringBuilder(128);
        StringBuilder selectClauseSql = new StringBuilder(128);
        StringBuilder selectClauseSqlpp = new StringBuilder(128);
        StringBuilder fromClauseSql = new StringBuilder(128);
        StringBuilder fromClauseSqlpp = new StringBuilder(128);
        StringBuilder orderbyClauseSql = new StringBuilder(128);
        StringBuilder orderbyClauseSqlpp = new StringBuilder(128);
        StringBuilder summary = new StringBuilder(128);

        String fieldExprFormat = "%s.%s";

        for (int i = 0; i < tCount; i++) {
            String tThis = "t" + i;
            String clause = i == 0 ? "FROM" : joinKinds[i - 1] + " JOIN";

            String joinConditionSql, joinConditionSqlpp;
            if (i == 0) {
                joinConditionSqlpp = joinConditionSql = "";
            } else {
                String joinFieldThisExprSql = String.format(fieldExprFormat, tThis, JOIN_FIELD);
                String joinFieldThisExprSqlpp = joinFieldThisExprSql; //missing2Null(joinFieldThisExprSql);
                String joinConditionFormat;
                switch (shape) {
                    case 'c':
                        String tPrev = "t" + (i - 1);
                        String joinFieldPrevExprSql = String.format(fieldExprFormat, tPrev, JOIN_FIELD);
                        String joinFieldPrevExprSqlpp = joinFieldPrevExprSql; // missing2Null(joinFieldPrevExprSql);
                        joinConditionFormat = "ON %s = %s ";
                        joinConditionSql =
                                String.format(joinConditionFormat, joinFieldPrevExprSql, joinFieldThisExprSql);
                        joinConditionSqlpp =
                                String.format(joinConditionFormat, joinFieldPrevExprSqlpp, joinFieldThisExprSqlpp);
                        break;
                    case 's':
                        String t0 = "t" + 0;
                        String joinField0ExprSql = String.format(fieldExprFormat, t0, JOIN_FIELD);
                        String joinField0ExprSqlpp = joinField0ExprSql; // missing2Null(joinField0ExprSql);
                        joinConditionFormat = "ON %s = %s ";
                        joinConditionSql = String.format(joinConditionFormat, joinField0ExprSql, joinFieldThisExprSql);
                        joinConditionSqlpp =
                                String.format(joinConditionFormat, joinField0ExprSqlpp, joinFieldThisExprSqlpp);
                        break;
                    case 'q':
                        joinCondBuilderSql.setLength(0);
                        joinCondBuilderSqlpp.setLength(0);
                        joinConditionFormat = "%s %s = %s ";
                        for (int j = 0; j < i; j++) {
                            String kwj = j == 0 ? "ON" : "AND";
                            String tj = "t" + j;
                            String joinFieldJExprSql = String.format(fieldExprFormat, tj, JOIN_FIELD);
                            String joinFieldJExprSqlpp = joinFieldJExprSql; //missing2Null(joinFieldJExprSql);
                            String joinCondPartSql =
                                    String.format(joinConditionFormat, kwj, joinFieldJExprSql, joinFieldThisExprSql);
                            String joinCondPartSqlpp = String.format(joinConditionFormat, kwj, joinFieldJExprSqlpp,
                                    joinFieldThisExprSqlpp);
                            joinCondBuilderSql.append(joinCondPartSql);
                            joinCondBuilderSqlpp.append(joinCondPartSqlpp);
                        }
                        joinConditionSql = joinCondBuilderSql.toString();
                        joinConditionSqlpp = joinCondBuilderSqlpp.toString();
                        break;
                    default:
                        throw new IllegalArgumentException(String.valueOf(shape));
                }
            }

            String fromClauseFormat = "%s (%s) %s %s";
            fromClauseSql.append(String.format(fromClauseFormat, clause, tDefs.get(i), tThis, joinConditionSql));
            fromClauseSqlpp.append(String.format(fromClauseFormat, clause, tDefs.get(i), tThis, joinConditionSqlpp));

            if (i > 0) {
                selectClauseSql.append(", ");
                selectClauseSqlpp.append(", ");
                orderbyClauseSql.append(", ");
                orderbyClauseSqlpp.append(", ");
                if (i > 1) {
                    summary.append(',');
                }
                summary.append(joinKinds[i - 1]);
            }
            String projectFieldExprSql = String.format(fieldExprFormat, tThis, PROJECT_FIELD);
            String projectFieldExprSqlpp = missing2Null(projectFieldExprSql);
            String projectFieldAlias = String.format("%s_%s", tThis, PROJECT_FIELD);
            String projectFormat = "%s AS %s";
            selectClauseSql.append(String.format(projectFormat, projectFieldExprSql, projectFieldAlias));
            selectClauseSqlpp.append(String.format(projectFormat, projectFieldExprSqlpp, projectFieldAlias));
            orderbyClauseSql.append(String.format("%s NULLS FIRST", projectFieldAlias));
            orderbyClauseSqlpp.append(projectFieldAlias);
        }

        if (tCount > 1) {
            summary.append(';').append(shape);
        }

        String queryFormat = "SELECT %s %sORDER BY %s";
        String sqlQuery = String.format(queryFormat, selectClauseSql, fromClauseSql, orderbyClauseSql);
        String sqlppQuery = String.format(queryFormat, selectClauseSqlpp, fromClauseSqlpp, orderbyClauseSqlpp);

        return new TestQuery(sqlQuery, sqlppQuery, summary.toString());
    }

    private static String missing2Null(String expr) {
        return String.format("if_missing(%s, null)", expr);
    }

    public SqlppRQGJoinsIT(int testcaseId, String sqlQuery, String sqlppQuery, String desc) {
        this.testcaseId = testcaseId;
        this.sqlQuery = sqlQuery;
        this.sqlppQuery = sqlppQuery;
        this.desc = desc;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        setUpBeforeClass();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        tearDownAfterClass();
    }

    @Test
    public void test() throws Exception {
        runTestCase(testcaseId, desc, sqlQuery, sqlppQuery);
    }

    private static class TestQuery {
        final String sqlQuery;
        final String sqlppQuery;
        final String summary;

        TestQuery(String sqlQuery, String sqlppQuery, String summary) {
            this.sqlQuery = sqlQuery;
            this.sqlppQuery = sqlppQuery;
            this.summary = summary;
        }
    }
}
