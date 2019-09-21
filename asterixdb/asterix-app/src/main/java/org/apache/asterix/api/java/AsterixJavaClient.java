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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.RequestReference;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.utils.Job;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
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

    public AsterixJavaClient(ICcApplicationContext appCtx, IHyracksClientConnection hcc, Reader queryText,
            PrintWriter writer, ILangCompilationProvider compilationProvider,
            IStatementExecutorFactory statementExecutorFactory, IStorageComponentProvider storageComponentProvider) {
        this.appCtx = appCtx;
        this.hcc = hcc;
        this.queryText = queryText;
        this.writer = writer;
        this.compilationProvider = compilationProvider;
        this.statementExecutorFactory = statementExecutorFactory;
        this.storageComponentProvider = storageComponentProvider;
        apiFramework = new APIFramework(compilationProvider);
        parserFactory = compilationProvider.getParserFactory();
    }

    public AsterixJavaClient(ICcApplicationContext appCtx, IHyracksClientConnection hcc, Reader queryText,
            ILangCompilationProvider compilationProvider, IStatementExecutorFactory statementExecutorFactory,
            IStorageComponentProvider storageComponentProvider) {
        this(appCtx, hcc, queryText,
                // This is a commandline client and so System.out is appropriate
                new PrintWriter(System.out, true), // NOSONAR
                compilationProvider, statementExecutorFactory, storageComponentProvider);
    }

    public void setStatementParameters(Map<String, IAObject> statementParams) {
        this.statementParams = statementParams;
    }

    public void compile() throws Exception {
        compile(true, false, true, false, false, false, false);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob)
            throws Exception {
        compile(optimize, printRewrittenExpressions, printLogicalPlan, printOptimizedPlan, printPhysicalOpsOnly,
                generateBinaryRuntime, printJob, PlanFormat.STRING);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob,
            PlanFormat pformat) throws Exception {
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
        MetadataManager.INSTANCE.init();

        SessionConfig conf = new SessionConfig(OutputFormat.ADM, optimize, true, generateBinaryRuntime, pformat);
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
                new ResultProperties(IStatementExecutor.ResultDelivery.IMMEDIATE), new IStatementExecutor.Stats(), null,
                null, null, statementParams, true);
        translator.compileAndExecute(hcc, requestParameters);
        executionPlans = translator.getExecutionPlans();
        writer.flush();
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
