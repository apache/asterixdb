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
package org.apache.asterix.api.java;

import java.io.PrintWriter;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.RequestReference;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.utils.Job;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.DataverseDecl;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionConfig.OutputFormat;
import org.apache.asterix.translator.SessionConfig.PlanFormat;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobSpecification;

public class AsterixJavaClient {
    private IHyracksClientConnection hcc;
    private Reader queryText;
    private PrintWriter writer;

    private Job[] dmlJobs;
    private JobSpecification queryJobSpec;

    private final ILangCompilationProvider compilationProvider;
    private final IParserFactory parserFactory;
    private final APIFramework apiFramework;
    private final IStatementExecutorFactory statementExecutorFactory;
    private final IStorageComponentProvider storageComponentProvider;
    private ICcApplicationContext appCtx;
    private Map<String, IAObject> statementParams;
    private ExecutionPlans executionPlans;
    private final boolean addAnalyzeForOptimizerTests;

    public AsterixJavaClient(ICcApplicationContext appCtx, IHyracksClientConnection hcc, Reader queryText,
            PrintWriter writer, ILangCompilationProvider compilationProvider,
            IStatementExecutorFactory statementExecutorFactory, IStorageComponentProvider storageComponentProvider,
            boolean addAnalyzeForOptimizerTests) {
        this.appCtx = appCtx;
        this.hcc = hcc;
        this.queryText = queryText;
        this.writer = writer;
        this.compilationProvider = compilationProvider;
        this.statementExecutorFactory = statementExecutorFactory;
        this.storageComponentProvider = storageComponentProvider;
        apiFramework = new APIFramework(compilationProvider);
        parserFactory = compilationProvider.getParserFactory();
        this.addAnalyzeForOptimizerTests = addAnalyzeForOptimizerTests;
    }

    public AsterixJavaClient(ICcApplicationContext appCtx, IHyracksClientConnection hcc, Reader queryText,
            ILangCompilationProvider compilationProvider, IStatementExecutorFactory statementExecutorFactory,
            IStorageComponentProvider storageComponentProvider) {
        this(appCtx, hcc, queryText,
                // This is a commandline client and so System.out is appropriate
                new PrintWriter(System.out, true), // NOSONAR
                compilationProvider, statementExecutorFactory, storageComponentProvider, false);
    }

    public void setStatementParameters(Map<String, IAObject> statementParams) {
        this.statementParams = statementParams;
    }

    public void compile() throws Exception {
        compile(true, false, true, false, false, false, false, addAnalyzeForOptimizerTests);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob,
            boolean addAnalyzeForOptimizerTests) throws Exception {
        compile(optimize, printRewrittenExpressions, printLogicalPlan, printOptimizedPlan, printPhysicalOpsOnly,
                generateBinaryRuntime, printJob, PlanFormat.STRING, addAnalyzeForOptimizerTests);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob,
            PlanFormat pformat, boolean addAnalyzeForOptimizerTests) throws Exception {
        queryJobSpec = null;
        dmlJobs = null;
        executionPlans = null;

        if (queryText == null) {
            return;
        }
        int ch;
        StringBuilder builder = new StringBuilder();
        while ((ch = queryText.read()) != -1) {
            builder.append((char) ch);
        }
        String statement = builder.toString();
        IParser parser = parserFactory.createParser(statement);
        List<Statement> statements = parser.parse();
        if (addAnalyzeForOptimizerTests) {
            insertAnalyzeStatements(statements);
        }

        MetadataManager.INSTANCE.init();

        SessionConfig conf = new SessionConfig(OutputFormat.ADM, optimize, true, generateBinaryRuntime, pformat,
                SessionConfig.HyracksJobFormat.JSON);
        conf.setOOBData(false, printRewrittenExpressions, printLogicalPlan, printOptimizedPlan, printJob);
        if (printPhysicalOpsOnly) {
            conf.set(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS, true);
        }
        SessionOutput output = new SessionOutput(conf, writer);

        IStatementExecutor translator = statementExecutorFactory.create(appCtx, statements, output, compilationProvider,
                storageComponentProvider, new ResponsePrinter(output));
        final RequestReference requestReference =
                RequestReference.of(UUID.randomUUID().toString(), "CC", System.currentTimeMillis());
        final IRequestParameters requestParameters = new RequestParameters(requestReference, statement, null,
                new ResultProperties(IStatementExecutor.ResultDelivery.IMMEDIATE), new IStatementExecutor.Stats(),
                new IStatementExecutor.StatementProperties(), null, null, null, null, statementParams, true);
        translator.compileAndExecute(hcc, requestParameters);
        executionPlans = translator.getExecutionPlans();
        writer.flush();
    }

    private void insertAnalyzeStatements(List<Statement> statements) throws Exception {
        Set<String> alreadyInserted = new HashSet<>();
        String currentDv = null;

        for (int i = 0; i < statements.size(); i++) {
            Statement s = statements.get(i);

            if (s.getKind() == Statement.Kind.DATAVERSE_DECL) {
                currentDv = ((DataverseDecl) s).getDataverseName().toString();
                continue;
            }

            if (s.getKind() == Statement.Kind.DATASET_DECL) {
                DatasetDecl dsDecl = (DatasetDecl) s;
                String dsName = dsDecl.getName().getValue();
                String dv = currentDv;
                if (dsDecl.getNamespace() != null && dsDecl.getNamespace().getDataverseName() != null) {
                    String[] parts = dsDecl.getNamespace().getDataverseName().getCanonicalForm().split("/");
                    dv = String.join(".", parts);
                }

                // add analyze statements for INTERNAL datasets only
                // as we don't support analyze for external datasets
                if (dsDecl.getDatasetType() == DatasetConfig.DatasetType.INTERNAL) {
                    String fq = (dv != null && !dv.isEmpty()) ? dv + "." + dsName : dsName;
                    if (alreadyInserted.add(fq)) {
                        String sql = "ANALYZE DATASET " + fq + ";";
                        List<Statement> analyzeStmts = parserFactory.createParser(sql).parse();
                        //insert analyze statement right after the create dataset statement
                        statements.addAll(i + 1, analyzeStmts);
                        i += analyzeStmts.size();
                    }
                }
            }
        }
    }

    public void execute() throws Exception {
        if (dmlJobs != null) {
            apiFramework.executeJobArray(hcc, dmlJobs, writer);
        }
        if (queryJobSpec != null) {
            apiFramework.executeJobArray(hcc, new JobSpecification[] { queryJobSpec }, writer);
        }
    }

    public ExecutionPlans getExecutionPlans() {
        return executionPlans;
    }
}
