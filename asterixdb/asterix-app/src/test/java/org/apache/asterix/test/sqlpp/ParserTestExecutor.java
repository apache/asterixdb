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
package org.apache.asterix.test.sqlpp;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.apache.asterix.lang.sqlpp.util.SqlppAstPrintUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.test.common.ComparisonException;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.ComparisonEnum;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import junit.extensions.PA;

public class ParserTestExecutor extends TestExecutor {

    private IParserFactory sqlppParserFactory = new SqlppParserFactory();
    private IRewriterFactory sqlppRewriterFactory = new SqlppRewriterFactory();

    @Override
    public void executeTest(String actualPath, TestCaseContext testCaseCtx, ProcessBuilder pb,
            boolean isDmlRecoveryTest, TestGroup failedGroup) throws Exception {
        int queryCount = 0;
        List<CompilationUnit> cUnits = testCaseCtx.getTestCase().getCompilationUnit();
        for (CompilationUnit cUnit : cUnits) {
            LOGGER.info(
                    "Starting [TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " ... ");
            List<TestFileContext> testFileCtxs = testCaseCtx.getTestFiles(cUnit);
            List<TestFileContext> expectedResultFileCtxs = testCaseCtx.getExpectedResultFiles(cUnit);
            for (TestFileContext ctx : testFileCtxs) {
                File testFile = ctx.getFile();
                try {
                    if (queryCount >= expectedResultFileCtxs.size()
                            && !cUnit.getOutputDir().getValue().equals("none")) {
                        throw new ComparisonException("no result file for " + testFile.toString() + "; queryCount: "
                                + queryCount + ", filectxs.size: " + expectedResultFileCtxs.size());
                    }

                    // Runs the test query.
                    File expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();
                    File actualResultFile =
                            testCaseCtx.getActualResultFile(cUnit, expectedResultFile, new File(actualPath));
                    testSQLPPParser(testFile, actualResultFile, expectedResultFile);

                    LOGGER.info(
                            "[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " PASSED ");
                    queryCount++;
                } catch (Exception e) {
                    System.err.println("testFile " + testFile.toString() + " raised an exception: " + e);
                    if (cUnit.getExpectedError().isEmpty()) {
                        e.printStackTrace();
                        System.err.println("...Unexpected!");
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    } else {
                        // must compare with the expected failure message
                        if (e instanceof ComparisonException) {
                            throw e;
                        }
                        LOGGER.info("[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName()
                                + " failed as expected: " + e.getMessage());
                        System.err.println("...but that was expected.");
                    }
                }
            }
        }
    }

    // Tests the SQL++ parser.
    public void testSQLPPParser(File queryFile, File actualResultFile, File expectedFile) throws Exception {
        actualResultFile.getParentFile().mkdirs();
        PrintWriter writer = new PrintWriter(new FileOutputStream(actualResultFile));
        IParser parser = sqlppParserFactory.createParser(readTestFile(queryFile));
        GlobalConfig.ASTERIX_LOGGER.info(queryFile.toString());
        try {
            List<Statement> statements = parser.parse();
            List<FunctionDecl> functions = getDeclaredFunctions(statements);
            String dvName = getDefaultDataverse(statements);
            MetadataProvider metadataProvider = mock(MetadataProvider.class);

            @SuppressWarnings("unchecked")
            Map<String, Object> config = mock(Map.class);
            when(metadataProvider.getDefaultDataverseName()).thenReturn(dvName);
            when(metadataProvider.getConfig()).thenReturn(config);
            when(config.get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS)).thenReturn("true");
            when(metadataProvider.findDataset(anyString(), anyString())).thenAnswer(new Answer<Dataset>() {
                @Override
                public Dataset answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    final Dataset mockDataset = mock(Dataset.class);
                    String fullyQualifiedName = args[0] != null ? args[0] + "." + args[1] : (String) args[1];
                    when(mockDataset.getFullyQualifiedName()).thenReturn(fullyQualifiedName);
                    return mockDataset;
                }
            });

            for (Statement st : statements) {
                if (st.getKind() == Statement.Kind.QUERY) {
                    Query query = (Query) st;
                    IQueryRewriter rewriter = sqlppRewriterFactory.createQueryRewriter();
                    rewrite(rewriter, functions, query, metadataProvider,
                            new LangRewritingContext(query.getVarCounter(), TestUtils.NOOP_WARNING_COLLECTOR));

                    // Tests deep copy and deep equality.
                    Query copiedQuery = (Query) SqlppRewriteUtil.deepCopy(query);
                    Assert.assertEquals(query.hashCode(), copiedQuery.hashCode());
                    Assert.assertEquals(query, copiedQuery);
                }
                SqlppAstPrintUtil.print(st, writer);
            }
            writer.close();
            // Compares the actual result and the expected result.
            runScriptAndCompareWithResult(queryFile, expectedFile, actualResultFile, ComparisonEnum.TEXT,
                    StandardCharsets.UTF_8);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.warn("Failed while testing file " + queryFile);
            throw e;
        } finally {
            writer.close();
        }
    }

    // Extracts declared functions.
    private List<FunctionDecl> getDeclaredFunctions(List<Statement> statements) {
        List<FunctionDecl> functionDecls = new ArrayList<FunctionDecl>();
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.FUNCTION_DECL) {
                functionDecls.add((FunctionDecl) st);
            }
        }
        return functionDecls;
    }

    // Gets the default dataverse for the input statements.
    private String getDefaultDataverse(List<Statement> statements) {
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.DATAVERSE_DECL) {
                DataverseDecl dv = (DataverseDecl) st;
                return dv.getDataverseName().getValue();
            }
        }
        return null;
    }

    // Rewrite queries.
    // Note: we do not do inline function rewriting here because this needs real
    // metadata access.
    private void rewrite(IQueryRewriter rewriter, List<FunctionDecl> declaredFunctions, Query topExpr,
            MetadataProvider metadataProvider, LangRewritingContext context) throws AsterixException {
        PA.invokeMethod(rewriter,
                "setup(java.util.List, org.apache.asterix.lang.common.base.IReturningStatement, "
                        + "org.apache.asterix.metadata.declared.MetadataProvider, "
                        + "org.apache.asterix.lang.common.rewrites.LangRewritingContext, " + "java.util.Collection)",
                declaredFunctions, topExpr, metadataProvider, context, null);
        PA.invokeMethod(rewriter, "inlineColumnAlias()");
        PA.invokeMethod(rewriter, "generateColumnNames()");
        PA.invokeMethod(rewriter, "substituteGroupbyKeyExpression()");
        PA.invokeMethod(rewriter, "rewriteGroupBys()");
        PA.invokeMethod(rewriter, "rewriteSetOperations()");
        PA.invokeMethod(rewriter, "variableCheckAndRewrite()");
        PA.invokeMethod(rewriter, "rewriteGroupByAggregationSugar()");

    }

}
