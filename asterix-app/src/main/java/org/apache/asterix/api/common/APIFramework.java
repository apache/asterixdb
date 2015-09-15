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
package org.apache.asterix.api.common;

import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import org.apache.asterix.api.common.Job.SubmissionMode;
import org.apache.asterix.aql.base.Statement.Kind;
import org.apache.asterix.aql.expression.FunctionDecl;
import org.apache.asterix.aql.expression.Query;
import org.apache.asterix.aql.expression.visitor.AQLPrintVisitor;
import org.apache.asterix.aql.rewrites.AqlRewriter;
import org.apache.asterix.common.config.AsterixCompilerProperties;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.common.AqlExpressionTypeComputer;
import org.apache.asterix.dataflow.data.common.AqlMergeAggregationExpressionFactory;
import org.apache.asterix.dataflow.data.common.AqlNullableTypeComputer;
import org.apache.asterix.dataflow.data.common.AqlPartialAggregationTypeComputer;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.jobgen.AqlLogicalExpressionJobGen;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.optimizer.base.RuleCollections;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.asterix.translator.AqlExpressionToPlanTranslator;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import org.apache.hyracks.algebricks.compiler.api.ICompiler;
import org.apache.hyracks.algebricks.compiler.api.ICompilerFactory;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import org.apache.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPlotter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

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
                RuleCollections.buildInitialTranslationRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RuleCollections.buildTypeInferenceRuleCollection()));
        defaultLogicalRewrites.add(new Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>(seqOnceCtrl,
                RuleCollections.buildAutogenerateIDRuleCollection()));
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

        //put TXnRuleCollection!
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
        public IOptimizationContext createOptimizationContext(int varCounter,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
                PhysicalOptimizationConfig physicalOptimizationConfig) {
            return new AlgebricksOptimizationContext(varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, nullableTypeComputer,
                    physicalOptimizationConfig);
        }

    }

    public static Pair<Query, Integer> reWriteQuery(List<FunctionDecl> declaredFunctions,
            AqlMetadataProvider metadataProvider, Query q, SessionConfig conf) throws AsterixException {

        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_EXPR_TREE)) {
            conf.out().println();

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("<h4>Expression tree:</h4>");
                conf.out().println("<pre>");
            } else {
                conf.out().println("----------Expression tree:");
            }

            if (q != null) {
                q.accept(new AQLPrintVisitor(conf.out()), 0);
            }

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("</pre>");
            }
        }
        AqlRewriter rw = new AqlRewriter(declaredFunctions, q, metadataProvider);
        rw.rewrite();
        Query rwQ = rw.getExpr();
        return new Pair(rwQ, rw.getVarCounter());
    }

    public static JobSpecification compileQuery(List<FunctionDecl> declaredFunctions,
            AqlMetadataProvider queryMetadataProvider, Query rwQ, int varCounter, String outputDatasetName,
            SessionConfig conf, ICompiledDmlStatement statement) throws AsterixException, AlgebricksException,
            JSONException, RemoteException, ACIDException {

        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_REWRITTEN_EXPR_TREE)) {
            conf.out().println();

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("<h4>Rewritten expression tree:</h4>");
                conf.out().println("<pre>");
            } else {
                conf.out().println("----------Rewritten expression:");
            }

            if (rwQ != null) {
                rwQ.accept(new AQLPrintVisitor(conf.out()), 0);
            }

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("</pre>");
            }
        }

        org.apache.asterix.common.transactions.JobId asterixJobId = JobIdFactory.generateJobId();
        queryMetadataProvider.setJobId(asterixJobId);
        AqlExpressionToPlanTranslator t = new AqlExpressionToPlanTranslator(queryMetadataProvider, varCounter,
                outputDatasetName, statement);

        ILogicalPlan plan;
        // statement = null when it's a query
        if (statement == null || statement.getKind() != Kind.LOAD) {
            plan = t.translate(rwQ);
        } else {
            plan = t.translateLoad();
        }

        LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();
        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_LOGICAL_PLAN)) {
            conf.out().println();

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("<h4>Logical plan:</h4>");
                conf.out().println("<pre>");
            } else {
                conf.out().println("----------Logical plan:");
            }

            if (rwQ != null || statement.getKind() == Kind.LOAD) {
                StringBuilder buffer = new StringBuilder();
                PlanPrettyPrinter.printPlan(plan, buffer, pvisitor, 0);
                conf.out().print(buffer);
            }

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("</pre>");
            }
        }

        //print the plot for the logical plan
        AsterixExternalProperties xProps = AsterixAppContextInfo.getInstance().getExternalProperties();
        Boolean plot = xProps.getIsPlottingEnabled();
        if (plot) {
            PlanPlotter.printLogicalPlan(plan);
        }

        AsterixCompilerProperties compilerProperties = AsterixAppContextInfo.getInstance().getCompilerProperties();
        int frameSize = compilerProperties.getFrameSize();
        int sortFrameLimit = (int) (compilerProperties.getSortMemorySize() / frameSize);
        int groupFrameLimit = (int) (compilerProperties.getGroupMemorySize() / frameSize);
        int joinFrameLimit = (int) (compilerProperties.getJoinMemorySize() / frameSize);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setFrameSize(frameSize);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesExternalSort(sortFrameLimit);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesExternalGroupBy(groupFrameLimit);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesHybridHash(joinFrameLimit);

        HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder(
                AqlOptimizationContextFactory.INSTANCE);
        builder.setPhysicalOptimizationConfig(OptimizationConfUtil.getPhysicalOptimizationConfig());
        builder.setLogicalRewrites(buildDefaultLogicalRewrites());
        builder.setPhysicalRewrites(buildDefaultPhysicalRewrites());
        IDataFormat format = queryMetadataProvider.getFormat();
        ICompilerFactory compilerFactory = builder.create();
        builder.setExpressionEvalSizeComputer(format.getExpressionEvalSizeComputer());
        builder.setIMergeAggregationExpressionFactory(new AqlMergeAggregationExpressionFactory());
        builder.setPartialAggregationTypeComputer(new AqlPartialAggregationTypeComputer());
        builder.setExpressionTypeComputer(AqlExpressionTypeComputer.INSTANCE);
        builder.setNullableTypeComputer(AqlNullableTypeComputer.INSTANCE);

        ICompiler compiler = compilerFactory.createCompiler(plan, queryMetadataProvider, t.getVarCounter());
        if (conf.isOptimize()) {
            compiler.optimize();
            //plot optimized logical plan
            if (plot)
                PlanPlotter.printOptimizedLogicalPlan(plan);
            if (conf.is(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN)) {
                if (conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS)) {
                    // For Optimizer tests.
                    StringBuilder buffer = new StringBuilder();
                    PlanPrettyPrinter.printPhysicalOps(plan, buffer, 0);
                    conf.out().print(buffer);
                } else {
                    if (conf.is(SessionConfig.FORMAT_HTML)) {
                        conf.out().println("<h4>Optimized logical plan:</h4>");
                        conf.out().println("<pre>");
                    } else {
                        conf.out().println("----------Optimized logical plan:");
                    }

                    if (rwQ != null || statement.getKind() == Kind.LOAD) {
                        StringBuilder buffer = new StringBuilder();
                        PlanPrettyPrinter.printPlan(plan, buffer, pvisitor, 0);
                        conf.out().print(buffer);
                    }

                    if (conf.is(SessionConfig.FORMAT_HTML)) {
                        conf.out().println("</pre>");
                    }
                }
            }
        }

        if (!conf.isGenerateJobSpec()) {
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
        builder.setPredicateEvaluatorFactoryProvider(format.getPredicateEvaluatorFactoryProvider());

        switch (conf.fmt()) {
            case JSON:
                builder.setPrinterProvider(format.getJSONPrinterFactoryProvider());
                break;
            case CSV:
                builder.setPrinterProvider(format.getCSVPrinterFactoryProvider());
                break;
            case ADM:
                builder.setPrinterProvider(format.getPrinterFactoryProvider());
                break;
            default:
                throw new RuntimeException("Unexpected OutputFormat!");
        }

        builder.setSerializerDeserializerProvider(format.getSerdeProvider());
        builder.setTypeTraitProvider(format.getTypeTraitProvider());
        builder.setNormalizedKeyComputerFactoryProvider(format.getNormalizedKeyComputerFactoryProvider());

        JobEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(asterixJobId,
                queryMetadataProvider.isWriteTransaction());
        JobSpecification spec = compiler.createJob(AsterixAppContextInfo.getInstance(), jobEventListenerFactory);

        if (conf.is(SessionConfig.OOB_HYRACKS_JOB)) {
            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("<h4>Hyracks job:</h4>");
                conf.out().println("<pre>");
            } else {
                conf.out().println("----------Hyracks job:");
            }

            if (rwQ != null) {
                conf.out().println(spec.toJSON().toString(1));
                conf.out().println(spec.getUserConstraints());
            }

            if (conf.is(SessionConfig.FORMAT_HTML)) {
                conf.out().println("</pre>");
            }
        }
        return spec;
    }

    public static void executeJobArray(IHyracksClientConnection hcc, JobSpecification[] specs, PrintWriter out)
            throws Exception {
        for (int i = 0; i < specs.length; i++) {
            specs[i].setMaxReattempts(0);
            JobId jobId = hcc.startJob(specs[i]);
            long startTime = System.currentTimeMillis();
            hcc.waitForCompletion(jobId);
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.00;
            out.println("<pre>Duration: " + duration + " sec</pre>");
        }

    }

    public static void executeJobArray(IHyracksClientConnection hcc, Job[] jobs, PrintWriter out) throws Exception {
        for (int i = 0; i < jobs.length; i++) {
            jobs[i].getJobSpec().setMaxReattempts(0);
            long startTime = System.currentTimeMillis();
            try {
                JobId jobId = hcc.startJob(jobs[i].getJobSpec());
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
            out.println("<pre>Duration: " + duration + " sec</pre>");
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
