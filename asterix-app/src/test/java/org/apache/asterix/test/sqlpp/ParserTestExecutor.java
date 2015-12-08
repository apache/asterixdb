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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
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
import org.apache.asterix.lang.common.base.Statement.Kind;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.apache.asterix.lang.sqlpp.util.SqlppAstPrintUtil;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.test.aql.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.context.TestFileContext;
import org.apache.asterix.testframework.xml.TestCase.CompilationUnit;
import org.apache.asterix.testframework.xml.TestGroup;

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
                    if (queryCount >= expectedResultFileCtxs.size()) {
                        throw new IllegalStateException("no result file for " + testFile.toString() + "; queryCount: "
                                + queryCount + ", filectxs.size: " + expectedResultFileCtxs.size());
                    }

                    // Runs the test query.
                    File actualResultFile = testCaseCtx.getActualResultFile(cUnit, new File(actualPath));
                    File expectedResultFile = expectedResultFileCtxs.get(queryCount).getFile();
                    testSQLPPParser(testFile, actualResultFile, expectedResultFile);

                    LOGGER.info(
                            "[TEST]: " + testCaseCtx.getTestCase().getFilePath() + "/" + cUnit.getName() + " PASSED ");
                    queryCount++;
                } catch (Exception e) {
                    System.err.println("testFile " + testFile.toString() + " raised an exception:");
                    e.printStackTrace();
                    if (cUnit.getExpectedError().isEmpty()) {
                        System.err.println("...Unexpected!");
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        throw new Exception("Test \"" + testFile + "\" FAILED!", e);
                    } else {
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
            AqlMetadataProvider aqlMetadataProvider = mock(AqlMetadataProvider.class);

            @SuppressWarnings("unchecked")
            Map<String, String> config = mock(Map.class);
            when(aqlMetadataProvider.getDefaultDataverseName()).thenReturn(dvName);
            when(aqlMetadataProvider.getConfig()).thenReturn(config);
            when(config.get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS)).thenReturn("true");

            for (Statement st : statements) {
                if (st.getKind() == Kind.QUERY) {
                    Query query = (Query) st;
                    IQueryRewriter rewriter = sqlppRewriterFactory.createQueryRewriter();
                    rewrite(rewriter, functions, query, aqlMetadataProvider,
                            new LangRewritingContext(query.getVarCounter()));
                }
                SqlppAstPrintUtil.print(st, writer);
            }
            writer.close();
            // Compares the actual result and the expected result.
            runScriptAndCompareWithResult(queryFile, new PrintWriter(System.err), expectedFile, actualResultFile);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.warning("Failed while testing file " + queryFile);
            throw e;
        } finally {
            writer.close();
        }
    }

    // Extracts declared functions.
    private List<FunctionDecl> getDeclaredFunctions(List<Statement> statements) {
        List<FunctionDecl> functionDecls = new ArrayList<FunctionDecl>();
        for (Statement st : statements) {
            if (st.getKind().equals(Statement.Kind.FUNCTION_DECL)) {
                functionDecls.add((FunctionDecl) st);
            }
        }
        return functionDecls;
    }

    // Gets the default dataverse for the input statements.
    private String getDefaultDataverse(List<Statement> statements) {
        for (Statement st : statements) {
            if (st.getKind().equals(Statement.Kind.DATAVERSE_DECL)) {
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
            AqlMetadataProvider metadataProvider, LangRewritingContext context) throws AsterixException {
        PA.invokeMethod(rewriter,
                "setup(java.util.List, org.apache.asterix.lang.common.statement.Query, org.apache.asterix.metadata.declared.AqlMetadataProvider, "
                        + "org.apache.asterix.lang.common.rewrites.LangRewritingContext)",
                declaredFunctions, topExpr, metadataProvider, context);
        PA.invokeMethod(rewriter, "inlineColumnAlias()");
        PA.invokeMethod(rewriter, "variableCheckAndRewrite(boolean)", Boolean.TRUE);
    }

}
