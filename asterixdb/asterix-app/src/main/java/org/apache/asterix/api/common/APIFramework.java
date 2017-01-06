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

import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.api.common.Job.SubmissionMode;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.dataflow.data.common.ConflictingTypeResolver;
import org.apache.asterix.dataflow.data.common.ExpressionTypeComputer;
import org.apache.asterix.dataflow.data.common.MergeAggregationExpressionFactory;
import org.apache.asterix.dataflow.data.common.MissableTypeComputer;
import org.apache.asterix.dataflow.data.common.PartialAggregationTypeComputer;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.jobgen.QueryLogicalExpressionJobGen;
import org.apache.asterix.lang.common.base.IAstPrintVisitorFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.util.AppContextInfo;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import org.apache.hyracks.algebricks.compiler.api.ICompiler;
import org.apache.hyracks.algebricks.compiler.api.ICompilerFactory;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.LogicalExpressionJobGenToExpressionRuntimeProviderAdapter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPlotter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

/**
 * Provides helper methods for compilation of a query into a JobSpec and submission
 * to Hyracks through the Hyracks client interface.
 */
public class APIFramework {

    private final IRewriterFactory rewriterFactory;
    private final IAstPrintVisitorFactory astPrintVisitorFactory;
    private final ILangExpressionToPlanTranslatorFactory translatorFactory;
    private final IRuleSetFactory ruleSetFactory;

    public APIFramework(ILangCompilationProvider compilationProvider) {
        this.rewriterFactory = compilationProvider.getRewriterFactory();
        this.astPrintVisitorFactory = compilationProvider.getAstPrintVisitorFactory();
        this.translatorFactory = compilationProvider.getExpressionToPlanTranslatorFactory();
        this.ruleSetFactory = compilationProvider.getRuleSetFactory();
    }

    private static class OptimizationContextFactory implements IOptimizationContextFactory {

        public static final OptimizationContextFactory INSTANCE = new OptimizationContextFactory();

        private OptimizationContextFactory() {
        }

        @Override
        public IOptimizationContext createOptimizationContext(int varCounter,
                IExpressionEvalSizeComputer expressionEvalSizeComputer,
                IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
                IExpressionTypeComputer expressionTypeComputer, IMissableTypeComputer missableTypeComputer,
                IConflictingTypeResolver conflictingTypeResolver, PhysicalOptimizationConfig physicalOptimizationConfig,
                AlgebricksPartitionConstraint clusterLocations) {
            return new AlgebricksOptimizationContext(varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, missableTypeComputer,
                    conflictingTypeResolver, physicalOptimizationConfig, clusterLocations);
        }
    }

    private void printPlanPrefix(SessionConfig conf, String planName) {
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            conf.out().println("<h4>" + planName + ":</h4>");
            conf.out().println("<pre>");
        } else {
            conf.out().println("----------" + planName + ":");
        }
    }

    private void printPlanPostfix(SessionConfig conf) {
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            conf.out().println("</pre>");
        }
    }

    public Pair<IReturningStatement, Integer> reWriteQuery(List<FunctionDecl> declaredFunctions,
            MetadataProvider metadataProvider, IReturningStatement q, SessionConfig conf) throws AsterixException {
        if (q == null) {
            return null;
        }
        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_EXPR_TREE)) {
            conf.out().println();
            printPlanPrefix(conf, "Expression tree");
            q.accept(astPrintVisitorFactory.createLangVisitor(conf.out()), 0);
            printPlanPostfix(conf);
        }
        IQueryRewriter rw = rewriterFactory.createQueryRewriter();
        rw.rewrite(declaredFunctions, q, metadataProvider, new LangRewritingContext(q.getVarCounter()));
        return new Pair<>(q, q.getVarCounter());
    }

    public JobSpecification compileQuery(IClusterInfoCollector clusterInfoCollector, MetadataProvider metadataProvider,
            Query rwQ, int varCounter, String outputDatasetName, SessionConfig conf, ICompiledDmlStatement statement)
            throws AlgebricksException, RemoteException, ACIDException {

        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_REWRITTEN_EXPR_TREE)) {
            conf.out().println();

            printPlanPrefix(conf, "Rewritten expression tree");
            if (rwQ != null) {
                rwQ.accept(astPrintVisitorFactory.createLangVisitor(conf.out()), 0);
            }
            printPlanPostfix(conf);
        }

        org.apache.asterix.common.transactions.JobId asterixJobId = JobIdFactory.generateJobId();
        metadataProvider.setJobId(asterixJobId);
        ILangExpressionToPlanTranslator t = translatorFactory.createExpressionToPlanTranslator(metadataProvider,
                varCounter);

        ILogicalPlan plan;
        // statement = null when it's a query
        if (statement == null || statement.getKind() != Statement.Kind.LOAD) {
            plan = t.translate(rwQ, outputDatasetName, statement);
        } else {
            plan = t.translateLoad(statement);
        }

        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_LOGICAL_PLAN)) {
            conf.out().println();

            printPlanPrefix(conf, "Logical plan");
            if (rwQ != null || (statement != null && statement.getKind() == Statement.Kind.LOAD)) {
                LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor(conf.out());
                PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
            }
            printPlanPostfix(conf);
        }

        //print the plot for the logical plan
        ExternalProperties xProps = AppContextInfo.INSTANCE.getExternalProperties();
        Boolean plot = xProps.getIsPlottingEnabled();
        if (plot) {
            PlanPlotter.printLogicalPlan(plan);
        }

        CompilerProperties compilerProperties = AppContextInfo.INSTANCE.getCompilerProperties();
        int frameSize = compilerProperties.getFrameSize();
        int sortFrameLimit = (int) (compilerProperties.getSortMemorySize() / frameSize);
        int groupFrameLimit = (int) (compilerProperties.getGroupMemorySize() / frameSize);
        int joinFrameLimit = (int) (compilerProperties.getJoinMemorySize() / frameSize);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setFrameSize(frameSize);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesExternalSort(sortFrameLimit);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesExternalGroupBy(groupFrameLimit);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesForJoin(joinFrameLimit);

        HeuristicCompilerFactoryBuilder builder = new HeuristicCompilerFactoryBuilder(
                OptimizationContextFactory.INSTANCE);
        builder.setPhysicalOptimizationConfig(OptimizationConfUtil.getPhysicalOptimizationConfig());
        builder.setLogicalRewrites(ruleSetFactory.getLogicalRewrites());
        builder.setPhysicalRewrites(ruleSetFactory.getPhysicalRewrites());
        IDataFormat format = metadataProvider.getFormat();
        ICompilerFactory compilerFactory = builder.create();
        builder.setExpressionEvalSizeComputer(format.getExpressionEvalSizeComputer());
        builder.setIMergeAggregationExpressionFactory(new MergeAggregationExpressionFactory());
        builder.setPartialAggregationTypeComputer(new PartialAggregationTypeComputer());
        builder.setExpressionTypeComputer(ExpressionTypeComputer.INSTANCE);
        builder.setMissableTypeComputer(MissableTypeComputer.INSTANCE);
        builder.setConflictingTypeResolver(ConflictingTypeResolver.INSTANCE);

        int parallelism = compilerProperties.getParallelism();
        builder.setClusterLocations(parallelism == CompilerProperties.COMPILER_PARALLELISM_AS_STORAGE
                ? metadataProvider.getClusterLocations() : getComputationLocations(clusterInfoCollector, parallelism));

        ICompiler compiler = compilerFactory.createCompiler(plan, metadataProvider, t.getVarCounter());
        if (conf.isOptimize()) {
            compiler.optimize();
            //plot optimized logical plan
            if (plot) {
                PlanPlotter.printOptimizedLogicalPlan(plan);
            }
            if (conf.is(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN)) {
                if (conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS)) {
                    // For Optimizer tests.
                    AlgebricksAppendable buffer = new AlgebricksAppendable(conf.out());
                    PlanPrettyPrinter.printPhysicalOps(plan, buffer, 0);
                } else {
                    printPlanPrefix(conf, "Optimized logical plan");
                    if (rwQ != null || (statement != null && statement.getKind() == Statement.Kind.LOAD)) {
                        LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor(conf.out());
                        PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
                    }
                    printPlanPostfix(conf);
                }
            }
        }
        if (rwQ != null && rwQ.isExplain()) {
            try {
                LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();
                PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
                ResultUtil.printResults(pvisitor.get().toString(), conf, new Stats(), null);
                return null;
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }

        if (!conf.isGenerateJobSpec()) {
            return null;
        }

        builder.setBinaryBooleanInspectorFactory(format.getBinaryBooleanInspectorFactory());
        builder.setBinaryIntegerInspectorFactory(format.getBinaryIntegerInspectorFactory());
        builder.setComparatorFactoryProvider(format.getBinaryComparatorFactoryProvider());
        builder.setExpressionRuntimeProvider(
                new LogicalExpressionJobGenToExpressionRuntimeProviderAdapter(QueryLogicalExpressionJobGen.INSTANCE));
        builder.setHashFunctionFactoryProvider(format.getBinaryHashFunctionFactoryProvider());
        builder.setHashFunctionFamilyProvider(format.getBinaryHashFunctionFamilyProvider());
        builder.setMissingWriterFactory(format.getMissingWriterFactory());
        builder.setPredicateEvaluatorFactoryProvider(format.getPredicateEvaluatorFactoryProvider());

        final SessionConfig.OutputFormat outputFormat = conf.fmt();
        switch (outputFormat) {
            case LOSSLESS_JSON:
                builder.setPrinterProvider(format.getLosslessJSONPrinterFactoryProvider());
                break;
            case CSV:
                builder.setPrinterProvider(format.getCSVPrinterFactoryProvider());
                break;
            case ADM:
                builder.setPrinterProvider(format.getADMPrinterFactoryProvider());
                break;
            case CLEAN_JSON:
                builder.setPrinterProvider(format.getCleanJSONPrinterFactoryProvider());
                break;
            default:
                throw new AlgebricksException("Unexpected OutputFormat: " + outputFormat);
        }

        builder.setSerializerDeserializerProvider(format.getSerdeProvider());
        builder.setTypeTraitProvider(format.getTypeTraitProvider());
        builder.setNormalizedKeyComputerFactoryProvider(format.getNormalizedKeyComputerFactoryProvider());

        JobEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(asterixJobId,
                metadataProvider.isWriteTransaction());
        JobSpecification spec = compiler.createJob(AppContextInfo.INSTANCE, jobEventListenerFactory);

        if (conf.is(SessionConfig.OOB_HYRACKS_JOB)) {
            printPlanPrefix(conf, "Hyracks job");
            if (rwQ != null) {
                try {
                    conf.out().println(
                            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(spec.toJSON()));
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
                conf.out().println(spec.getUserConstraints());
            }
            printPlanPostfix(conf);
        }
        return spec;
    }

    public void executeJobArray(IHyracksClientConnection hcc, JobSpecification[] specs, PrintWriter out)
            throws Exception {
        for (JobSpecification spec : specs) {
            spec.setMaxReattempts(0);
            JobId jobId = hcc.startJob(spec);
            long startTime = System.currentTimeMillis();
            hcc.waitForCompletion(jobId);
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.00;
            out.println("<pre>Duration: " + duration + " sec</pre>");
        }

    }

    public void executeJobArray(IHyracksClientConnection hcc, Job[] jobs, PrintWriter out) throws Exception {
        for (Job job : jobs) {
            job.getJobSpec().setMaxReattempts(0);
            long startTime = System.currentTimeMillis();
            try {
                JobId jobId = hcc.startJob(job.getJobSpec());
                if (job.getSubmissionMode() == SubmissionMode.ASYNCHRONOUS) {
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

    // Computes the location constraints based on user-configured parallelism parameter.
    // Note that the parallelism parameter is only a hint -- it will not be respected if it is too small or too large.
    private AlgebricksAbsolutePartitionConstraint getComputationLocations(IClusterInfoCollector clusterInfoCollector,
            int parallelismHint) throws AlgebricksException {
        try {
            Map<String, NodeControllerInfo> ncMap = clusterInfoCollector.getNodeControllerInfos();

            // Unifies the handling of non-positive parallelism.
            int parallelism = parallelismHint <= 0 ? -2 * ncMap.size() : parallelismHint;

            // Calculates per node parallelism, with load balance, i.e., randomly selecting nodes with larger
            // parallelism.
            int numNodes = ncMap.size();
            int numNodesWithOneMorePartition = parallelism % numNodes;
            int perNodeParallelismMin = parallelism / numNodes;
            int perNodeParallelismMax = parallelism / numNodes + 1;
            List<String> allNodes = new ArrayList<>();
            Set<String> selectedNodesWithOneMorePartition = new HashSet<>();
            for (Map.Entry<String, NodeControllerInfo> entry : ncMap.entrySet()) {
                allNodes.add(entry.getKey());
            }
            Random random = new Random();
            for (int index = numNodesWithOneMorePartition; index >= 1; --index) {
                int pick = random.nextInt(index);
                selectedNodesWithOneMorePartition.add(allNodes.get(pick));
                Collections.swap(allNodes, pick, index - 1);
            }

            // Generates cluster locations, which has duplicates for a node if it contains more than one partitions.
            List<String> locations = new ArrayList<>();
            for (Map.Entry<String, NodeControllerInfo> entry : ncMap.entrySet()) {
                String nodeId = entry.getKey();
                int numCores = entry.getValue().getNumCores();
                int availableCores = numCores > 1 ? numCores - 1 : numCores; // Reserves one core for heartbeat.
                int nodeParallelism = selectedNodesWithOneMorePartition.contains(nodeId) ? perNodeParallelismMax
                        : perNodeParallelismMin;
                int coresToUse = nodeParallelism >= 0 && nodeParallelism < availableCores ? nodeParallelism
                        : availableCores;
                for (int count = 0; count < coresToUse; ++count) {
                    locations.add(nodeId);
                }
            }
            return new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[0]));
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }
}
