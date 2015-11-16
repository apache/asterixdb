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

import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.api.common.Job;
import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.translator.QueryTranslator;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
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

    public AsterixJavaClient(IHyracksClientConnection hcc, Reader queryText, PrintWriter writer,
            ILangCompilationProvider compilationProvider) {
        this.hcc = hcc;
        this.queryText = queryText;
        this.writer = writer;
        this.compilationProvider = compilationProvider;
        parserFactory = compilationProvider.getParserFactory();
        this.apiFramework = new APIFramework(compilationProvider);
    }

    public AsterixJavaClient(IHyracksClientConnection hcc, Reader queryText,
            ILangCompilationProvider compilationProvider) {
        this(hcc, queryText, new PrintWriter(System.out, true), compilationProvider);
    }

    public void compile() throws Exception {
        compile(true, false, false, false, false, false, false);
    }

    public void compile(boolean optimize, boolean printRewrittenExpressions, boolean printLogicalPlan,
            boolean printOptimizedPlan, boolean printPhysicalOpsOnly, boolean generateBinaryRuntime, boolean printJob)
                    throws Exception {
        queryJobSpec = null;
        dmlJobs = null;

        if (queryText == null) {
            return;
        }
        int ch;
        StringBuilder builder = new StringBuilder();
        while ((ch = queryText.read()) != -1) {
            builder.append((char) ch);
        }
        IParser parser = parserFactory.createParser(builder.toString());
        List<Statement> statements = parser.parse();
        MetadataManager.INSTANCE.init();

        SessionConfig conf = new SessionConfig(writer, OutputFormat.ADM, optimize, true, generateBinaryRuntime);
        conf.setOOBData(false, printRewrittenExpressions, printLogicalPlan, printOptimizedPlan, printJob);
        if (printPhysicalOpsOnly) {
            conf.set(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS, true);
        }

        QueryTranslator translator = new QueryTranslator(statements, conf, compilationProvider);
        translator.compileAndExecute(hcc, null, QueryTranslator.ResultDelivery.SYNC);
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

}
