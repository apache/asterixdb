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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class SqlppRQGGroupingSetsIT extends SqlppRQGTestBase {

    private static final Logger LOGGER = LogManager.getLogger(SqlppRQGGroupingSetsIT.class);

    private static final String CONF_PROPERTY_SEED = getConfigurationPropertyName(SqlppRQGTestBase.class, "seed");
    private static final long CONF_PROPERTY_SEED_DEFAULT = System.currentTimeMillis();

    private static final String CONF_PROPERTY_LIMIT = getConfigurationPropertyName(SqlppRQGTestBase.class, "limit");
    private static final int CONF_PROPERTY_LIMIT_DEFAULT = 100;

    private static final int ROLLUP_ELEMENT_LIMIT = 2;
    private static final int CUBE_ELEMENT_LIMIT = 1;
    private static final int GROUPING_SETS_ELEMENT_LIMIT = 2;
    private static final int MULTI_ELEMENT_LIMIT = 2;

    private static final List<String> GROUPBY_COLUMNS = Arrays.asList(TWO, FOUR, TEN, TWENTY, HUNDRED, ODD100, EVEN100);

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
        runTestCase(testcaseId, groupByClause, sqlQuery, sqlppQuery);
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
        String agg = String.format("SUM(%s)", UNIQUE_1);
        if (random.nextInt(3) == 0) {
            int filterLimit = 1 + random.nextInt(9999);
            agg = String.format("%s FILTER(WHERE %s < %d)", agg, UNIQUE_2, filterLimit);
        }
        selectClause.append(String.format("%s AS agg_sum", agg));

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
