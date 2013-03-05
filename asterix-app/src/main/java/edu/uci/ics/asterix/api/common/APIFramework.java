/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.api.common;

import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import edu.uci.ics.asterix.api.common.Job.SubmissionMode;
import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.visitor.AQLPrintVisitor;
import edu.uci.ics.asterix.aql.rewrites.AqlRewriter;
import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.common.AqlExpressionTypeComputer;
import edu.uci.ics.asterix.dataflow.data.common.AqlMergeAggregationExpressionFactory;
import edu.uci.ics.asterix.dataflow.data.common.AqlNullableTypeComputer;
import edu.uci.ics.asterix.dataflow.data.common.AqlPartialAggregationTypeComputer;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.jobgen.AqlLogicalExpressionJobGen;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.optimizer.base.RuleCollections;
import edu.uci.ics.asterix.runtime.job.listener.JobEventListenerFactory;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.translator.AqlExpressionToPlanTranslator;
import edu.uci.ics.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompiler;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompilerFactory;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

/**
 * Provides helper methods for compilation of a query into a JobSpec and submission
 * to Hyracks through the Hyracks client interface.
 */
public class APIFramework {

    private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildDefaultLogicalRewrites() {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultLogicalRewrites = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
        SequentialFixpointRuleController seqCtrlNoDfs = new SequentialFixpointRuleController(false);
        SequentialFixpointRuleController seqCtrlFullDfs = new SequentialFixpointRuleController(true);
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RuleCollections.buildTypeInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RuleCollections.buildNormalizationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RuleCollections.buildCondPushDownAndJoinInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RuleCollections.buildLoadFieldsRuleCollection()));
        // fj
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RuleCollections.buildFuzzyJoinRuleCollection()));
        //
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RuleCollections.buildNormalizationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RuleCollections.buildCondPushDownAndJoinInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlFullDfs,
                RuleCollections.buildLoadFieldsRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RuleCollections.buildDataExchangeRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RuleCollections.buildConsolidationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RuleCollections.buildAccessMethodRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqCtrlNoDfs,
                RuleCollections.buildPlanCleanupRuleCollection()));
        return defaultLogicalRewrites;
    }

    private static List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> buildDefaultPhysicalRewrites() {
        List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> defaultPhysicalRewrites = new ArrayList<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>>();
        SequentialOnceRuleController seqOnceCtrl = new SequentialOnceRuleController(true);
        SequentialOnceRuleController seqOnceTopLevel = new SequentialOnceRuleController(false);
        defaultPhysicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RuleCollections.buildPhysicalRewritesAllLevelsRuleCollection()));
        defaultPhysicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceTopLevel,
                RuleCollections.buildPhysicalRewritesTopLevelRuleCollection()));
        defaultPhysicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RuleCollections.prepareForJobGenRuleCollection()));
        return defaultPhysicalRewrites;
    }

    private static class AqlOptimizationContextFactory implements IOptimizationContextFactory {

        public static final AqlOptimizationContextFactory INSTANCE = new AqlOptimizationContextFactory();

        private AqlOptimizationContextFactory() {
        }

        @Override
        public IOptimizationContext createOptimizationContext(int varCounter, int frameSize,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
                PhysicalOptimizationConfig physicalOptimizationConfig) {
            return new AlgebricksOptimizationContext(varCounter, frameSize, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, nullableTypeComputer,
                    physicalOptimizationConfig);
        }

    }

    public enum DisplayFormat {
        TEXT,
        HTML
    }

    public static Pair<Query, Integer> reWriteQuery(List<FunctionDecl> declaredFunctions,
            AqlMetadataProvider metadataProvider, Query q, SessionConfig pc, PrintWriter out, DisplayFormat pdf)
            throws AsterixException {
        if (!pc.isPrintPhysicalOpsOnly() && pc.isPrintExprParam()) {
            out.println();
            switch (pdf) {
                case HTML: {
                    out.println("<H1>Expression tree:</H1>");
                    out.println("<PRE>");
                    break;
                }
                case TEXT: {
                    out.println("----------Expression tree:");
                    break;
                }
            }
            if (q != null) {
                q.accept(new AQLPrintVisitor(out), 0);
            }
            switch (pdf) {
                case HTML: {
                    out.println("</PRE>");
                    break;
                }
            }
        }
        AqlRewriter rw = new AqlRewriter(declaredFunctions, q, metadataProvider.getMetadataTxnContext());
        rw.rewrite();
        Query rwQ = rw.getExpr();
        return new Pair(rwQ, rw.getVarCounter());
    }

    public static JobSpecification compileQuery(List<FunctionDecl> declaredFunctions,
            AqlMetadataProvider queryMetadataProvider, Query rwQ, int varCounter, String outputDatasetName,
            SessionConfig pc, PrintWriter out, DisplayFormat pdf, ICompiledDmlStatement statement)
            throws AsterixException, AlgebricksException, JSONException, RemoteException, ACIDException {

        if (!pc.isPrintPhysicalOpsOnly() && pc.isPrintRewrittenExprParam()) {
            out.println();

            switch (pdf) {
                case HTML: {
                    out.println("<H1>Rewriten expression tree:</H1>");
                    out.println("<PRE>");
                    break;
                }
                case TEXT: {
                    out.println("----------Rewritten expression:");
                    break;
                }
            }

            if (rwQ != null) {
                rwQ.accept(new AQLPrintVisitor(out), 0);
            }

            switch (pdf) {
                case HTML: {
                    out.println("</PRE>");
                    break;
                }
            }

        }

        AqlExpressionToPlanTranslator t = new AqlExpressionToPlanTranslator(queryMetadataProvider, varCounter,
                outputDatasetName, statement);

        ILogicalPlan plan = t.translate(rwQ);
        boolean isWriteTransaction = queryMetadataProvider.isWriteTransaction();

        LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();
        if (!pc.isPrintPhysicalOpsOnly() && pc.isPrintLogicalPlanParam()) {

            switch (pdf) {
                case HTML: {
                    out.println("<H1>Logical plan:</H1>");
                    out.println("<PRE>");
                    break;
                }
                case TEXT: {
                    out.println("----------Logical plan:");
                    break;
                }
            }

            if (rwQ != null) {
                StringBuilder buffer = new StringBuilder();
                PlanPrettyPrinter.printPlan(plan, buffer, pvisitor, 0);
                out.print(buffer);
            }

            switch (pdf) {
                case HTML: {
                    out.println("</PRE>");
                    break;
                }
            }
        }

        int frameSize = GlobalConfig.DEFAULT_FRAME_SIZE;
        String frameSizeStr = System.getProperty(GlobalConfig.FRAME_SIZE_PROPERTY);
        if (frameSizeStr != null) {
            int fz = -1;
            try {
                fz = Integer.parseInt(frameSizeStr);
            } catch (NumberFormatException nfe) {
                GlobalConfig.ASTERIX_LOGGER.warning("Wrong frame size size argument. Picking default value ("
                        + GlobalConfig.DEFAULT_FRAME_SIZE + ") instead.\n");
                throw new AlgebricksException(nfe);
            }
            if (fz >= 0) {
                frameSize = fz;
            }
        }

        HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder(
                AqlOptimizationContextFactory.INSTANCE);
        builder.setLogicalRewrites(buildDefaultLogicalRewrites());
        builder.setPhysicalRewrites(buildDefaultPhysicalRewrites());
        IDataFormat format = queryMetadataProvider.getFormat();
        ICompilerFactory compilerFactory = builder.create();
        builder.setFrameSize(frameSize);
        builder.setExpressionEvalSizeComputer(format.getExpressionEvalSizeComputer());
        builder.setIMergeAggregationExpressionFactory(new AqlMergeAggregationExpressionFactory());
        builder.setPartialAggregationTypeComputer(new AqlPartialAggregationTypeComputer());
        builder.setExpressionTypeComputer(AqlExpressionTypeComputer.INSTANCE);
        builder.setNullableTypeComputer(AqlNullableTypeComputer.INSTANCE);

        OptimizationConfUtil.getPhysicalOptimizationConfig().setFrameSize(frameSize);
        builder.setPhysicalOptimizationConfig(OptimizationConfUtil.getPhysicalOptimizationConfig());

        ICompiler compiler = compilerFactory.createCompiler(plan, queryMetadataProvider, t.getVarCounter());
        if (pc.isOptimize()) {
            compiler.optimize();
            if (pc.isPrintOptimizedLogicalPlanParam()) {
                if (pc.isPrintPhysicalOpsOnly()) {
                    // For Optimizer tests.
                    StringBuilder buffer = new StringBuilder();
                    PlanPrettyPrinter.printPhysicalOps(plan, buffer, 0);
                    out.print(buffer);
                } else {
                    switch (pdf) {
                        case HTML: {
                            out.println("<H1>Optimized logical plan:</H1>");
                            out.println("<PRE>");
                            break;
                        }
                        case TEXT: {
                            out.println("----------Optimized plan ");
                            break;
                        }
                    }
                    if (rwQ != null) {
                        StringBuilder buffer = new StringBuilder();
                        PlanPrettyPrinter.printPlan(plan, buffer, pvisitor, 0);
                        out.print(buffer);
                    }
                    switch (pdf) {
                        case HTML: {
                            out.println("</PRE>");
                            break;
                        }
                    }
                }
            }
        }

        if (!pc.isGenerateJobSpec()) {
            return null;
        }
        
        AlgebricksPartitionConstraint clusterLocs = queryMetadataProvider.getClusterLocations();
        builder.setBinaryBooleanInspectorFactory(format.getBinaryBooleanInspectorFactory());
        builder.setBinaryIntegerInspectorFactory(format.getBinaryIntegerInspectorFactory());
        builder.setClusterLocations(clusterLocs);
        builder.setComparatorFactoryProvider(format.getBinaryComparatorFactoryProvider());
        builder.setExpressionRuntimeProvider(new LogicalExpressionJobGenToExpressionRuntimeProviderAdapter(
                AqlLogicalExpressionJobGen.INSTANCE));
        builder.setHashFunctionFactoryProvider(format.getBinaryHashFunctionFactoryProvider());
        builder.setHashFunctionFamilyProvider(format.getBinaryHashFunctionFamilyProvider());
        builder.setNullWriterFactory(format.getNullWriterFactory());
        builder.setPrinterProvider(format.getPrinterFactoryProvider());
        builder.setSerializerDeserializerProvider(format.getSerdeProvider());
        builder.setTypeTraitProvider(format.getTypeTraitProvider());
        builder.setNormalizedKeyComputerFactoryProvider(format.getNormalizedKeyComputerFactoryProvider());

        JobSpecification spec = compiler.createJob(AsterixAppContextInfoImpl.getInstance());
        // set the job event listener
        spec.setJobletEventListenerFactory(new JobEventListenerFactory(queryMetadataProvider.getJobTxnId(),
                isWriteTransaction));
        if (pc.isPrintJob()) {
            switch (pdf) {
                case HTML: {
                    out.println("<H1>Hyracks job:</H1>");
                    out.println("<PRE>");
                    break;
                }
                case TEXT: {
                    out.println("----------Hyracks job:");
                    break;
                }
            }
            if (rwQ != null) {
                out.println(spec.toJSON().toString(1));
                out.println(spec.getUserConstraints());
            }
            switch (pdf) {
                case HTML: {
                    out.println("</PRE>");
                    break;
                }
            }
        }
        return spec;
    }

    public static void executeJobArray(IHyracksClientConnection hcc, JobSpecification[] specs, PrintWriter out,
            DisplayFormat pdf) throws Exception {
        for (int i = 0; i < specs.length; i++) {
            specs[i].setMaxReattempts(0);
            JobId jobId = hcc.startJob(GlobalConfig.HYRACKS_APP_NAME, specs[i]);
            long startTime = System.currentTimeMillis();
            hcc.waitForCompletion(jobId);
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.00;
            out.println("<PRE>Duration: " + duration + "</PRE>");
        }

    }

    public static void executeJobArray(IHyracksClientConnection hcc, Job[] jobs, PrintWriter out, DisplayFormat pdf)
            throws Exception {
        for (int i = 0; i < jobs.length; i++) {
            jobs[i].getJobSpec().setMaxReattempts(0);
            long startTime = System.currentTimeMillis();
            try {
                JobId jobId = hcc.startJob(GlobalConfig.HYRACKS_APP_NAME, jobs[i].getJobSpec());
                if (jobs[i].getSubmissionMode() == SubmissionMode.ASYNCHRONOUS) {
                    continue;
                }
                hcc.waitForCompletion(jobId);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.00;
            out.println("<PRE>Duration: " + duration + "</PRE>");
        }

    }

    private static IDataFormat getDataFormat(MetadataTransactionContext mdTxnCtx, String dataverseName)
            throws AsterixException {
        Dataverse dataverse = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverseName);
        IDataFormat format;
        try {
            format = (IDataFormat) Class.forName(dataverse.getDataFormat()).newInstance();
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return format;
    }

}
