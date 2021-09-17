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
package org.apache.asterix.test.array;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.array.ArrayQuery.Conjunct;
import org.apache.asterix.test.array.ArrayQuery.ExistsConjunct;
import org.apache.asterix.test.array.ArrayQuery.QuantifiedConjunct;
import org.apache.asterix.test.array.ArrayQuery.SimpleConjunct;
import org.apache.asterix.test.array.ArrayQuery.UnnestStep;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectReader;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This test will perform the following, using a flat schema that is defined ahead of time:
 * <ol>
 * <li>Generate a random schema for a single dataset using fields from the predefined flat schema, composed of
 * various nested objects and an array of scalars or objects (whose contents can themselves contain scalars, arrays of
 * scalars or arrays of objects following the same definition this is defined in).</li>
 * <li>Using the randomly generated schema, create a query that will build the corresponding dataset from the flat
 * schema using a series of CTEs and GROUP BY GROUP AS clauses.</li>
 * <li>Create the array index DDL using the randomly generated schema.</li>
 * <li>Randomly generate a query that will utilize the aforementioned array index, exploring the parameter space:<ul>
 * <li>The number of non-indexed field terms defined in the query.</li>
 * <li>The number of joins to perform for the query.</li>
 * <li>The type of query.<ul>
 *
 * <li>For quantification queries...<ul>
 * <li>The type of quantification (SOME AND EVERY | SOME).</li>
 * <li>The presence of multiple quantifiers.</li>
 * <li>Whether to use specify multiple levels of nesting with EXISTs or another quantification expression.</li>
 * </ul></li>
 *
 * <li>For UNNEST queries...<ul>
 * <li>The number of unrelated UNNESTs.</li>
 * </ul></li>
 *
 * <li>For JOIN queries...<ul>
 * <li>The type of JOIN (INNER | LEFT OUTER | Quantified).</li>
 * <li><i>All of the options from the previous two items.</i></li>
 * </ul></li>
 * </ul></li>
 * </ul>
 * <li>Execute the query to build the dataset of our random schema and then execute the array index DDL, OR execute
 * the array index DDL then build the dataset of our random schema. This decision is made randomly.</li>
 * <li>Execute the randomly generated query with array index optimization enabled and without array index optimization
 * enabled. Verify that the two result sets are exactly the same AND verify that the array index is being utilized.</li>
 * <li>Repeat this entire process (from step 1) {@code ArrayIndexRQGIT.limit} amount of times.</li>
 * </ol>
 */
@RunWith(Parameterized.class)
public class SqlppArrayIndexRQGTest {
    private static final Logger LOGGER = LogManager.getLogger(SqlppArrayIndexRQGTest.class);
    private static final String TEST_CONFIG_FILE_NAME = "src/test/resources/cc.conf";
    private static final String CONF_PROPERTY_LIMIT = "ArrayIndexRQGIT.limit";
    private static final String CONF_PROPERTY_SEED = "ArrayIndexRQGIT.seed";
    private static final String CONF_PROPERTY_JOIN = "ArrayIndexRQGIT.join";
    private static final long CONF_PROPERTY_SEED_DEFAULT = System.currentTimeMillis();
    private static final int CONF_PROPERTY_LIMIT_DEFAULT = 50;
    private static final int CONF_PROPERTY_JOIN_DEFAULT = 25;
    private static TestExecutor testExecutor;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectReader OBJECT_READER = OBJECT_MAPPER.readerFor(ObjectNode.class);

    private static final int GROUP_MEMORY_MB = 4;
    private static final int JOIN_MEMORY_MB = 4;
    private static final int PROBE_DOCUMENT_LIMIT = 50;
    private static final int QUERY_RESULT_LIMIT = 100;
    private static final int JOIN_COUNT_LIMIT = 3;
    private final TestInstance testInstance;

    @BeforeClass
    public static void setUp() throws Exception {
        testExecutor = new TestExecutor();
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor);

        StringBuilder sb = new StringBuilder();
        sb.append("DROP DATAVERSE TestDataverse IF EXISTS;\n");
        sb.append("CREATE DATAVERSE TestDataverse;\n");
        sb.append("USE TestDataverse;\n\n");
        sb.append("CREATE TYPE TestType AS { _id: uuid };\n");
        sb.append("CREATE DATASET ").append(BaseWisconsinTable.TABLE_NAME);
        sb.append(" (TestType)\nPRIMARY KEY _id AUTOGENERATED;\n");
        sb.append("LOAD DATASET ").append(BaseWisconsinTable.TABLE_NAME).append('\n');
        sb.append("USING localfs").append(String.format("((\"path\"=\"%s\"),(\"format\"=\"adm\"));\n\n",
                "asterix_nc1://" + BaseWisconsinTable.TABLE_FILE));
        for (int i = 0; i < JOIN_COUNT_LIMIT; i++) {
            sb.append("CREATE DATASET ProbeDataset").append(i + 1);
            sb.append(" (TestType)\nPRIMARY KEY _id AUTOGENERATED;\n");
            sb.append("INSERT INTO ProbeDataset").append(i + 1).append('\n');
            sb.append("FROM ").append(BaseWisconsinTable.TABLE_NAME);
            sb.append(" B\nSELECT VALUE B LIMIT ").append(PROBE_DOCUMENT_LIMIT).append(";\n");
        }

        LOGGER.debug("Now executing setup DDL:\n" + sb);
        testExecutor.executeSqlppUpdateOrDdl(sb.toString(), TestCaseContext.OutputFormat.ADM);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    private static long getConfigurationProperty(String propertyName, long defValue) {
        String textValue = System.getProperty(propertyName);
        if (textValue != null) {
            try {
                return Long.parseLong(textValue);

            } catch (NumberFormatException e) {
                LOGGER.warn(String.format("Cannot parse configuration property: %s. Will use default value: %d",
                        propertyName, defValue));
            }
        }
        return defValue;
    }

    @Parameterized.Parameters(name = "ArrayIndexRQGIT {index}: {3}")
    public static Collection<TestInstance> tests() {
        List<TestInstance> testCases = new ArrayList<>();

        long seed = getConfigurationProperty(CONF_PROPERTY_SEED, CONF_PROPERTY_SEED_DEFAULT);
        int limit = (int) getConfigurationProperty(CONF_PROPERTY_LIMIT, CONF_PROPERTY_LIMIT_DEFAULT);
        int join = (int) getConfigurationProperty(CONF_PROPERTY_JOIN, CONF_PROPERTY_JOIN_DEFAULT);
        LOGGER.info(String.format("Testsuite configuration: -D%s=%d -D%s=%d -D%s=%d", CONF_PROPERTY_SEED, seed,
                CONF_PROPERTY_LIMIT, limit, CONF_PROPERTY_JOIN, join));

        // Initialize our random number generators.
        BuilderContext context = new BuilderContext();
        context.randomGenerator = new MersenneTwister(seed);
        context.distBelow05 = new ExponentialDistribution(context.randomGenerator, 0.25, 1e-9);
        context.distBelow1 = new ExponentialDistribution(context.randomGenerator, 0.5, 1e-9);
        context.distEqual1 = new ExponentialDistribution(context.randomGenerator, 1.0, 1e-9);
        context.distAbove1 = new ExponentialDistribution(context.randomGenerator, 1.5, 1e-9);
        context.valueSupplierFactory = new ValueSupplierFactory(context.distBelow1, context.distEqual1,
                context.distAbove1, context.randomGenerator);

        for (int i = 0; i < limit; i++) {
            TestInstance testCase = new TestInstance(i, i < join, context.randomGenerator.nextBoolean());
            context.isJoin = testCase.isJoin;

            // Build the array index first (implicitly, the schema for our dataset).
            testCase.arrayIndex = buildArrayIndex(context);

            // Build the dataset that corresponds to the array index.
            testCase.arrayDataset = buildArrayDataset(context);

            // Build the query that corresponds to the array index.
            testCase.arrayQuery = buildArrayQuery(context);

            // Finally, add the test case to our test cases.
            testCases.add(testCase);
        }

        return testCases;
    }

    private static ArrayIndex buildArrayIndex(BuilderContext context) {
        ArrayIndex.Builder indexBuilder = new ArrayIndex.Builder("testIndex", "IndexedDataset");

        // Determine the number of fields and the depth of our array.
        int totalNumberOfFields, depthOfArray;
        do {
            int numberOfAtomicPrefixes, numberOfFieldsInArray, numberOfAtomicSuffixes;
            if (context.isJoin) {
                // TODO (GLENN): Avoid performing joins with composite-atomic indexes for now.
                numberOfAtomicPrefixes = 0; // (int) Math.round(context.distBelow05.sample());
                numberOfFieldsInArray = Math.max(1, (int) Math.round(context.distBelow1.sample()));
                numberOfAtomicSuffixes = 0; // (int) Math.round(context.distBelow05.sample());
            } else {
                numberOfAtomicPrefixes = (int) Math.round(context.distBelow05.sample());
                numberOfFieldsInArray = Math.max(1, (int) Math.round(context.distBelow1.sample()));
                numberOfAtomicSuffixes = (int) Math.round(context.distBelow05.sample());
            }
            indexBuilder.setNumberOfAtomicPrefixes(numberOfAtomicPrefixes);
            indexBuilder.setNumberOfFieldsInArray(numberOfFieldsInArray);
            indexBuilder.setNumberOfAtomicSuffixes(numberOfAtomicSuffixes);
            indexBuilder.setIsArrayOfScalars(numberOfFieldsInArray == 1 && context.randomGenerator.nextBoolean());
            depthOfArray = Math.max(1, (int) Math.round(context.distBelow1.sample()));
            indexBuilder.setDepthOfArray(depthOfArray);
            totalNumberOfFields = numberOfAtomicPrefixes + numberOfFieldsInArray + numberOfAtomicSuffixes;
        } while (totalNumberOfFields > BaseWisconsinTable.NUMBER_OF_NON_GROUPING_FIELDS
                || depthOfArray > BaseWisconsinTable.NUMBER_OF_GROUPING_FIELDS);

        // Characterize how fields are generated (uniformly random).
        indexBuilder.setValueSupplier(context.valueSupplierFactory.getCompleteArrayIndexValueSupplier());
        context.arrayIndex = indexBuilder.build();
        return context.arrayIndex;
    }

    private static ArrayDataset buildArrayDataset(BuilderContext context) {
        ArrayDataset.Builder datasetBuilder = new ArrayDataset.Builder();
        context.arrayIndex.getElements().forEach(datasetBuilder::addElement);
        return datasetBuilder.build();
    }

    private static ArrayQuery buildArrayQuery(BuilderContext context) {
        ArrayQuery.Builder queryBuilder = new ArrayQuery.Builder(context.arrayIndex);
        queryBuilder.setLimitCount(QUERY_RESULT_LIMIT);

        // Determine constants that should only be generated once.
        int joinCount = Math.min(JOIN_COUNT_LIMIT, 1 + (int) Math.round(context.distBelow1.sample()));
        int depthOfArray = context.arrayIndex.getArrayPath().size();
        if (context.isJoin) {
            // TODO: This is to avoid performing UNNEST joins on composite-atomic indexes. Refer to ASTERIXDB-2964.
            // queryBuilder.setUnnestStepDepth(Math.min(depthOfArray, (int) Math.round(context.distBelow1.sample())));
            queryBuilder.setUnnestStepDepth(0);
            queryBuilder.setNumberOfJoins(joinCount);
            if (joinCount == 1) {
                // TODO: This is to avoid performing multiple UNNEST style explicit joins (falls under ASTERIXDB-2964).
                queryBuilder.setNumberOfExplicitJoins(context.randomGenerator.nextBoolean() ? 1 : 0);
            } else {
                queryBuilder.setNumberOfExplicitJoins(0);
            }
        } else {
            // TODO: This is to avoid specifying extra UNNESTs. Refer to ASTERIXDB-2962.
            // queryBuilder.setUnnestStepDepth(Math.min(depthOfArray, (int) Math.round(context.distBelow1.sample())));
            queryBuilder.setUnnestStepDepth(depthOfArray);
            queryBuilder.setNumberOfJoins(0);
            queryBuilder.setNumberOfExplicitJoins(0);
        }

        // Characterize the randomness in areas that occur more than once.
        queryBuilder.setValueSupplier(context.valueSupplierFactory.getWorkingArrayQueryValueSupplier());
        return queryBuilder.build();
    }

    public SqlppArrayIndexRQGTest(TestInstance testInstance) {
        this.testInstance = testInstance;
    }

    @Test
    public void test() throws Exception {
        LOGGER.info("\n" + testInstance);
        LOGGER.debug("Now executing test setup:\n" + testInstance.getTestSetup());
        testExecutor.executeSqlppUpdateOrDdl(testInstance.getTestSetup(), TestCaseContext.OutputFormat.ADM);

        // In addition to the results, we want the query plan as well.
        String nonOptimizedQueryPrefix = "SET `compiler.arrayindex` \"false\";\n";
        String optimizedQueryPrefix = "SET `compiler.arrayindex` \"true\";\n";
        TestCase.CompilationUnit.Parameter planParameter = new TestCase.CompilationUnit.Parameter();
        planParameter.setName("optimized-logical-plan");
        planParameter.setValue("true");
        planParameter.setType(ParameterTypeEnum.STRING);

        try (InputStream notOptimizedResultStream =
                testExecutor.executeQueryService(nonOptimizedQueryPrefix + testInstance.getTestQuery(),
                        TestCaseContext.OutputFormat.ADM, testExecutor.getEndpoint(Servlets.QUERY_SERVICE),
                        Collections.singletonList(planParameter), true, StandardCharsets.UTF_8);
                InputStream optimizedResultStream =
                        testExecutor.executeQueryService(optimizedQueryPrefix + testInstance.getTestQuery(),
                                TestCaseContext.OutputFormat.ADM, testExecutor.getEndpoint(Servlets.QUERY_SERVICE),
                                Collections.singletonList(planParameter), true, StandardCharsets.UTF_8)) {
            LOGGER.debug("Now executing query:\n" + testInstance.getTestQuery());

            // Verify that the optimized result is the same as the non optimized result.
            ObjectNode r1 = OBJECT_READER.readValue(notOptimizedResultStream);
            ObjectNode r2 = OBJECT_READER.readValue(optimizedResultStream);
            boolean doesR1HaveErrors = (r1.get("errors") != null);
            boolean doesR2HaveErrors = (r2.get("errors") != null);

            if (r1.get("results") == null || r2.get("results") == null) {
                LOGGER.error("Results not found. Errors are:\n"
                        + (doesR1HaveErrors ? ("Non Optimized Error: " + r1.get("errors") + "\n")
                                : ("No errors thrown from the non-optimized query!\n"))
                        + (doesR2HaveErrors ? ("Optimized Error: " + r2.get("errors") + "\n")
                                : ("No errors thrown from the optimized query!\n")));
                if (!doesR1HaveErrors && doesR2HaveErrors) {
                    Assert.fail("Optimized query resulted in an error.");
                } else {
                    LOGGER.error("Both queries have resulted in an error (not logging issue w/ query gen itself).");
                }

            } else if (!r1.get("results").equals(r2.get("results"))) {
                LOGGER.error("Non optimized query returned: " + r1.get("results") + "\n");
                LOGGER.error("Optimized query returned: " + r2.get("results") + "\n");
                Assert.fail("Optimized result is not equal to the non-optimized result.");

            } else {
                Iterator<JsonNode> results = r1.get("results").elements();
                if (!results.hasNext()) {
                    LOGGER.error("Query has produced no results!");
                } else {
                    LOGGER.debug("First result of query is: " + results.next().toString());
                }
            }

            // Verify that the array index is being used by the "optimized" query.
            String indexName = testInstance.arrayIndex.getIndexName();
            if (!doesR1HaveErrors && !doesR2HaveErrors
                    && !r2.get("plans").get("optimizedLogicalPlan").asText().contains(indexName)) {
                LOGGER.error("Optimized query plan: \n" + r2.get("plans").get("optimizedLogicalPlan").asText());
                Assert.fail("Index " + indexName + " is not being used.");
            }
        }
    }

    private static class TestInstance {
        private final int testCaseID;
        private final boolean isJoin;
        private final boolean isLoadFirst;

        private ArrayDataset arrayDataset;
        private ArrayIndex arrayIndex;
        private ArrayQuery arrayQuery;

        private String getTestSetup() {
            String compilerSetStmt = "SET `compiler.groupmemory` \"" + GROUP_MEMORY_MB + "MB\";\n"
                    + "SET `compiler.joinmemory` \"" + JOIN_MEMORY_MB + "MB\";\n";
            String dropDatasetStmt = "DROP DATASET IndexedDataset IF EXISTS;\n";
            String createDatasetStmt = "CREATE DATASET IndexedDataset (TestType)\nPRIMARY KEY _id AUTOGENERATED;\n";
            String leadingStatements = compilerSetStmt + "USE TestDataverse;\n" + dropDatasetStmt + createDatasetStmt;
            if (isLoadFirst) {
                return leadingStatements + "INSERT INTO IndexedDataset (\n" + arrayDataset + "\n);\n" + arrayIndex;
            } else {
                return leadingStatements + arrayIndex + "INSERT INTO IndexedDataset (\n" + arrayDataset + "\n);\n";
            }
        }

        private String getTestQuery() {
            String groupMemStmt = "SET `compiler.groupmemory` \"" + GROUP_MEMORY_MB + "MB\";\n";
            String joinMemStmt = "SET `compiler.joinmemory` \"" + JOIN_MEMORY_MB + "MB\";\n";
            String dataverseUseStmt = "USE TestDataverse;\n";
            return groupMemStmt + joinMemStmt + dataverseUseStmt + arrayQuery;
        }

        public TestInstance(int testCaseID, boolean isJoin, boolean isLoadFirst) {
            this.testCaseID = testCaseID;
            this.isJoin = isJoin;
            this.isLoadFirst = isLoadFirst;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("-------------------------------------------------------\n");
            sb.append("Test Case Name: #").append(testCaseID).append('\n');
            sb.append("Test Type: ").append(isJoin ? "JOIN" : "NON-JOIN").append('\n');
            sb.append("Test Order: ");
            sb.append(isLoadFirst ? "INSERT -> CREATE INDEX" : "CREATE INDEX -> INSERT").append(" -> QUERY\n");
            sb.append("Indexed Elements:\n");
            arrayIndex.getElements().forEach(e -> {
                sb.append("\tElement #").append(e.elementPosition).append(":\n");
                sb.append("\t\tKind: ").append(e.kind).append('\n');
                e.unnestList.forEach(f -> sb.append("\t\tUNNEST Field: ").append(f).append('\n'));
                e.projectList.forEach(f -> sb.append("\t\tPROJECT Field: ").append(f).append('\n'));
            });
            sb.append("Indexed Query: \n");
            arrayQuery.getFromExprs().forEach(e -> {
                sb.append("\tStarting FROM Expr:\n");
                sb.append("\t\tDataset Name: ").append(e.datasetName).append('\n');
                sb.append("\t\tAs Alias: ").append(e.alias).append('\n');
            });
            arrayQuery.getUnnestSteps().forEach(e -> {
                sb.append("\tUNNEST Step:\n");
                sb.append("\t\tSource Alias: ").append(e.sourceAlias).append('\n');
                sb.append("\t\tArray Field: ").append(e.arrayField).append('\n');
                sb.append("\t\tAs Alias: ").append(e.alias).append('\n');
            });
            arrayQuery.getJoinSteps().forEach(e -> {
                sb.append("\tJoin Step:\n");
                sb.append("\t\tSubquery FROM Expr:\n");
                sb.append("\t\t\tDataset Name: ").append(e.subqueryFromExpr.datasetName).append('\n');
                sb.append("\t\t\tAs Alias: ").append(e.subqueryFromExpr.alias).append('\n');
                for (UnnestStep unnestStep : e.subqueryUnnestSteps) {
                    sb.append("\t\tSubquery UNNEST Step:\n");
                    sb.append("\t\t\tSource Alias: ").append(unnestStep.sourceAlias).append('\n');
                    sb.append("\t\t\tArray Field: ").append(unnestStep.arrayField).append('\n');
                    sb.append("\t\t\tAs Alias: ").append(unnestStep.alias).append('\n');
                }
            });
            arrayQuery.getWhereConjuncts().forEach(e -> printConjunct(sb, e, 1));
            arrayQuery.getSelectExprs().forEach(e -> {
                sb.append("\tSelect Expr:\n");
                sb.append("\t\tExpr: ").append(e.expr).append('\n');
                sb.append("\t\tAs Alias: ").append(e.alias).append('\n');
            });
            sb.append("-------------------------------------------------------\n");
            return sb.toString();
        }

        private static void printConjunct(StringBuilder sb, Conjunct conjunct, int depth) {
            String localTabPrefix = "\t".repeat(depth);
            if (conjunct instanceof SimpleConjunct) {
                SimpleConjunct simpleConjunct = (SimpleConjunct) conjunct;
                sb.append(localTabPrefix).append("WHERE Conjunct (Simple):\n");
                localTabPrefix = localTabPrefix + "\t";
                sb.append(localTabPrefix).append("Expression 1: ").append(simpleConjunct.expressionOne).append('\n');
                sb.append(localTabPrefix).append("Expression 2: ").append(simpleConjunct.expressionTwo).append('\n');
                sb.append(localTabPrefix).append("Expression 3: ").append(simpleConjunct.expressionThree).append('\n');
                sb.append(localTabPrefix).append("Operator: ").append(simpleConjunct.operator).append('\n');
                sb.append(localTabPrefix).append("Annotation: ").append(simpleConjunct.annotation).append('\n');

            } else if (conjunct instanceof QuantifiedConjunct) {
                QuantifiedConjunct quantifiedConjunct = (QuantifiedConjunct) conjunct;
                sb.append(localTabPrefix).append("WHERE Conjunct (Quantified):\n");
                localTabPrefix = localTabPrefix + "\t";
                for (int i = 0; i < quantifiedConjunct.arraysToQuantify.size(); i++) {
                    String arrayToQuantify = quantifiedConjunct.arraysToQuantify.get(i);
                    String quantifiedVar = quantifiedConjunct.quantifiedVars.get(i);
                    sb.append(localTabPrefix).append("Quantified Array: ").append(arrayToQuantify).append('\n');
                    sb.append(localTabPrefix).append('\t').append("As Alias: ").append(quantifiedVar).append('\n');
                }
                quantifiedConjunct.conjuncts.forEach(e -> printConjunct(sb, e, depth + 1));

            } else {
                ExistsConjunct existsConjunct = (ExistsConjunct) conjunct;
                ArrayQuery.FromExpr fromExpr = existsConjunct.fromExpr;
                sb.append(localTabPrefix).append("WHERE Conjunct (EXISTS):\n");
                localTabPrefix = localTabPrefix + "\t";
                sb.append(localTabPrefix).append("Subquery FROM Expr:\n");
                sb.append(localTabPrefix).append("\t").append("FROM Expr: ").append(fromExpr.datasetName).append('\n');
                sb.append(localTabPrefix).append("\t").append("As Alias: ").append(fromExpr.alias).append('\n');
                for (UnnestStep e : existsConjunct.unnestSteps) {
                    sb.append(localTabPrefix).append("Subquery UNNEST Steps:\n");
                    sb.append(localTabPrefix).append('\t').append("Source Alias: ").append(e.sourceAlias).append('\n');
                    sb.append(localTabPrefix).append('\t').append("Array Field: ").append(e.arrayField).append('\n');
                    sb.append(localTabPrefix).append('\t').append("As Alias: ").append(e.alias).append('\n');
                }
                sb.append(localTabPrefix).append("Subquery WHERE Conjuncts:\n");
                existsConjunct.conjuncts.forEach(e -> printConjunct(sb, e, depth + 1));
            }
        }
    }

    private static class BuilderContext {
        ValueSupplierFactory valueSupplierFactory;
        ExponentialDistribution distBelow05;
        ExponentialDistribution distBelow1;
        ExponentialDistribution distEqual1;
        ExponentialDistribution distAbove1;
        RandomGenerator randomGenerator;
        ArrayIndex arrayIndex;
        boolean isJoin;
    }
}
