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

import static org.apache.hyracks.util.file.FileUtil.canonicalize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.SqlppRewriterFactory;
import org.apache.asterix.lang.sqlpp.util.SqlppAstPrintUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
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
    private IRewriterFactory sqlppRewriterFactory = new SqlppRewriterFactory(sqlppParserFactory);
    private Set<FunctionSignature> createdFunctions = new HashSet<>();

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
                        throw new ComparisonException("no result file for " + canonicalize(testFile) + "; queryCount: "
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
                    System.err.println("testFile " + canonicalize(testFile) + " raised an exception: " + e);
                    if (cUnit.getExpectedError().isEmpty()) {
                        e.printStackTrace();
                        System.err.println("...Unexpected!");
                        if (failedGroup != null) {
                            failedGroup.getTestCase().add(testCaseCtx.getTestCase());
                        }
                        throw new Exception("Test \"" + canonicalize(testFile) + "\" FAILED!", e);
                    } else {
                        // must compare with the expected failure message
                        if (e instanceof ComparisonException) {
                            throw e;
                        }
                        LOGGER.info("[TEST]: " + canonicalize(testCaseCtx.getTestCase().getFilePath()) + "/"
                                + cUnit.getName() + " failed as expected: " + e.getMessage());
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
            DataverseName dvName = getDefaultDataverse(statements);
            List<FunctionDecl> functions = getDeclaredFunctions(statements, dvName);
            List<FunctionSignature> createdFunctionsList = getCreatedFunctions(statements, dvName);
            createdFunctions.addAll(createdFunctionsList);

            MetadataProvider metadataProvider = mock(MetadataProvider.class);

            @SuppressWarnings("unchecked")
            Map<String, Object> config = mock(Map.class);
            when(metadataProvider.getDefaultDataverseName()).thenReturn(dvName);
            when(metadataProvider.getConfig()).thenReturn(config);
            when(config.get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS)).thenReturn("true");
            when(metadataProvider.findDataverse(any(DataverseName.class))).thenAnswer(new Answer<Dataverse>() {
                @Override
                public Dataverse answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    final Dataverse mockDataverse = mock(Dataverse.class);
                    when(mockDataverse.getDataverseName()).thenReturn((DataverseName) args[0]);
                    return mockDataverse;
                }
            });
            when(metadataProvider.findDataset(any(DataverseName.class), anyString())).thenAnswer(new Answer<Dataset>() {
                @Override
                public Dataset answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    final Dataset mockDataset = mock(Dataset.class);
                    when(mockDataset.getDataverseName()).thenReturn((DataverseName) args[0]);
                    when(mockDataset.getDatasetName()).thenReturn((String) args[1]);
                    return mockDataset;
                }
            });
            when(metadataProvider.lookupUserDefinedFunction(any(FunctionSignature.class)))
                    .thenAnswer(new Answer<Function>() {
                        @Override
                        public Function answer(InvocationOnMock invocation) {
                            Object[] args = invocation.getArguments();
                            FunctionSignature fs = (FunctionSignature) args[0];
                            if (!createdFunctions.contains(fs)) {
                                return null;
                            }
                            Function mockFunction = mock(Function.class);
                            when(mockFunction.getSignature()).thenReturn(fs);
                            return mockFunction;
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
                    StandardCharsets.UTF_8, null);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.warn("Failed while testing file " + canonicalize(queryFile));
            throw e;
        } finally {
            writer.close();
        }
    }

    // Extracts declared functions.
    private List<FunctionDecl> getDeclaredFunctions(List<Statement> statements, DataverseName defaultDataverseName) {
        List<FunctionDecl> functionDecls = new ArrayList<>();
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.FUNCTION_DECL) {
                FunctionDecl fds = (FunctionDecl) st;
                FunctionSignature signature = fds.getSignature();
                if (signature.getDataverseName() == null) {
                    signature.setDataverseName(defaultDataverseName);
                }
                functionDecls.add(fds);
            }
        }
        return functionDecls;
    }

    // Extracts created functions.
    private List<FunctionSignature> getCreatedFunctions(List<Statement> statements,
            DataverseName defaultDataverseName) {
        List<FunctionSignature> createdFunctions = new ArrayList<>();
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.CREATE_FUNCTION) {
                CreateFunctionStatement cfs = (CreateFunctionStatement) st;
                FunctionSignature signature = cfs.getFunctionSignature();
                if (signature.getDataverseName() == null) {
                    signature = new FunctionSignature(defaultDataverseName, signature.getName(), signature.getArity());
                }
                createdFunctions.add(signature);
            }
        }
        return createdFunctions;
    }

    // Gets the default dataverse for the input statements.
    private DataverseName getDefaultDataverse(List<Statement> statements) {
        for (Statement st : statements) {
            if (st.getKind() == Statement.Kind.DATAVERSE_DECL) {
                DataverseDecl dv = (DataverseDecl) st;
                return dv.getDataverseName();
            }
        }
        return MetadataBuiltinEntities.DEFAULT_DATAVERSE_NAME;
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
        PA.invokeMethod(rewriter, "resolveFunctionCalls()");
        PA.invokeMethod(rewriter, "generateColumnNames()");
        PA.invokeMethod(rewriter, "substituteGroupbyKeyExpression()");
        PA.invokeMethod(rewriter, "rewriteGroupBys()");
        PA.invokeMethod(rewriter, "rewriteSetOperations()");
        PA.invokeMethod(rewriter, "inlineColumnAlias()");
        PA.invokeMethod(rewriter, "rewriteWindowExpressions()");
        PA.invokeMethod(rewriter, "rewriteGroupingSets()");
        PA.invokeMethod(rewriter, "variableCheckAndRewrite()");
        PA.invokeMethod(rewriter, "extractAggregatesFromCaseExpressions()");
        PA.invokeMethod(rewriter, "rewriteGroupByAggregationSugar()");
        PA.invokeMethod(rewriter, "rewriteWindowAggregationSugar()");
        PA.invokeMethod(rewriter, "rewriteSpecialFunctionNames()");
    }

}
