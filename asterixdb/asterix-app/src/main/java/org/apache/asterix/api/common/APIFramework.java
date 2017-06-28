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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.utils.Job;
import org.apache.asterix.common.utils.Job.SubmissionMode;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.dataflow.data.common.*;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.jobgen.QueryLogicalExpressionJobGen;
import org.apache.asterix.lang.aql.statement.SubscribeFeedStatement;
import org.apache.asterix.lang.common.base.*;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.optimizer.base.FuzzyUtils;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.asterix.translator.util.FunctionCollection;
import org.apache.asterix.utils.ResourceUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import org.apache.hyracks.algebricks.compiler.api.ICompiler;
import org.apache.hyracks.algebricks.compiler.api.ICompilerFactory;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.*;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitorJson;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.common.config.OptionTypes;

import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Provides helper methods for compilation of a query into a JobSpec and submission
 * to Hyracks through the Hyracks client interface.
 */
public class APIFramework {

    private static final int MIN_FRAME_LIMIT_FOR_SORT = 3;
    private static final int MIN_FRAME_LIMIT_FOR_GROUP_BY = 4;
    private static final int MIN_FRAME_LIMIT_FOR_JOIN = 5;

    // A white list of supported configurable parameters.
    private static final Set<String> CONFIGURABLE_PARAMETER_NAMES =
            ImmutableSet.of(CompilerProperties.COMPILER_JOINMEMORY_KEY, CompilerProperties.COMPILER_GROUPMEMORY_KEY,
                    CompilerProperties.COMPILER_SORTMEMORY_KEY, CompilerProperties.COMPILER_PARALLELISM_KEY,
                    FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, FuzzyUtils.SIM_FUNCTION_PROP_NAME,
                    FuzzyUtils.SIM_THRESHOLD_PROP_NAME, SubscribeFeedStatement.WAIT_FOR_COMPLETION,
                    FeedActivityDetails.FEED_POLICY_NAME, FeedActivityDetails.COLLECT_LOCATIONS, "inline_with",
                    "hash_merge", "output-record-type");

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

    static {
        FormatUtils.getDefaultFormat().registerRuntimeFunctions(FunctionCollection.getFunctionDescriptorFactories());
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

    private void printPlanPrefix(SessionOutput output, String planName) {
        if (output.config().is(SessionConfig.FORMAT_HTML)) {
            output.out().println("<h4>" + planName + ":</h4>");
            if(planName.equalsIgnoreCase("Logical plan"))
            output.out().println("<pre class = query-plan>");
            else if(planName.equalsIgnoreCase("Optimized logical plan"))
                output.out().println("<pre class = query-optimized-plan>");
            else output.out().println("<pre>");
        } else {
            output.out().println("----------" + planName + ":");
        }

    }

    private void printPlanPostfix(SessionOutput output) {
        if (output.config().is(SessionConfig.FORMAT_HTML)) {
            output.out().println("</pre>");
        }
    }

    public Pair<IReturningStatement, Integer> reWriteQuery(List<FunctionDecl> declaredFunctions,
            MetadataProvider metadataProvider, IReturningStatement q, SessionOutput output)
            throws CompilationException {
        if (q == null) {
            return null;
        }
        SessionConfig conf = output.config();
        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_EXPR_TREE)) {
            output.out().println();
            printPlanPrefix(output, "Expression tree");
            q.accept(astPrintVisitorFactory.createLangVisitor(output.out()), 0);
            printPlanPostfix(output);
        }
        IQueryRewriter rw = rewriterFactory.createQueryRewriter();
        rw.rewrite(declaredFunctions, q, metadataProvider, new LangRewritingContext(q.getVarCounter()));
        return new Pair<>(q, q.getVarCounter());
    }

    public JobSpecification compileQuery(IClusterInfoCollector clusterInfoCollector, MetadataProvider metadataProvider,
            Query rwQ, int varCounter, String outputDatasetName, SessionOutput output, ICompiledDmlStatement statement)
            throws AlgebricksException, RemoteException, ACIDException {

        SessionConfig conf = output.config();
        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_REWRITTEN_EXPR_TREE)) {
            output.out().println();

            printPlanPrefix(output, "Rewritten expression tree");
            if (rwQ != null) {
                rwQ.accept(astPrintVisitorFactory.createLangVisitor(output.out()), 0);
            }
            printPlanPostfix(output);
        }

        org.apache.asterix.common.transactions.JobId asterixJobId = JobIdFactory.generateJobId();
        metadataProvider.setJobId(asterixJobId);
        ILangExpressionToPlanTranslator t =
                translatorFactory.createExpressionToPlanTranslator(metadataProvider, varCounter);

        ILogicalPlan plan;
        // statement = null when it's a query
        if (statement == null || statement.getKind() != Statement.Kind.LOAD) {
            plan = t.translate(rwQ, outputDatasetName, statement);
        } else {
            plan = t.translateLoad(statement);
        }

        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_LOGICAL_PLAN)) {
            output.out().println();

            printPlanPrefix(output, "Logical plan");
            if (rwQ != null || (statement != null && statement.getKind() == Statement.Kind.LOAD)) {

                if(output.config().getLpfmt().equals(SessionConfig.PlanFormat.CLEAN_JSON)||output.config().getLpfmt().equals(SessionConfig.PlanFormat.JSON)) {
                    LogicalOperatorPrettyPrintVisitorJson pvisitor = new LogicalOperatorPrettyPrintVisitorJson(output.out());
                    PlanPrettyPrinter.operatorID = 0;
                    PlanPrettyPrinter.resetOperatorID(plan);
                    PlanPrettyPrinter.printPlanJson(plan, pvisitor, 0);
                }else{
                    LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor(output.out());
                    PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
                }
            }
            printPlanPostfix(output);
        }
        CompilerProperties compilerProperties = metadataProvider.getApplicationContext().getCompilerProperties();
        int frameSize = compilerProperties.getFrameSize();
        Map<String, String> querySpecificConfig = metadataProvider.getConfig();
        validateConfig(querySpecificConfig); // Validates the user-overridden query parameters.
        int sortFrameLimit = getFrameLimit(CompilerProperties.COMPILER_SORTMEMORY_KEY,
                querySpecificConfig.get(CompilerProperties.COMPILER_SORTMEMORY_KEY),
                compilerProperties.getSortMemorySize(), frameSize, MIN_FRAME_LIMIT_FOR_SORT);
        int groupFrameLimit = getFrameLimit(CompilerProperties.COMPILER_GROUPMEMORY_KEY,
                querySpecificConfig.get(CompilerProperties.COMPILER_GROUPMEMORY_KEY),
                compilerProperties.getGroupMemorySize(), frameSize, MIN_FRAME_LIMIT_FOR_GROUP_BY);
        int joinFrameLimit = getFrameLimit(CompilerProperties.COMPILER_JOINMEMORY_KEY,
                querySpecificConfig.get(CompilerProperties.COMPILER_JOINMEMORY_KEY),
                compilerProperties.getJoinMemorySize(), frameSize, MIN_FRAME_LIMIT_FOR_JOIN);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setFrameSize(frameSize);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesExternalSort(sortFrameLimit);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesExternalGroupBy(groupFrameLimit);
        OptimizationConfUtil.getPhysicalOptimizationConfig().setMaxFramesForJoin(joinFrameLimit);

        HeuristicCompilerFactoryBuilder builder =
                new HeuristicCompilerFactoryBuilder(OptimizationContextFactory.INSTANCE);
        builder.setPhysicalOptimizationConfig(OptimizationConfUtil.getPhysicalOptimizationConfig());
        builder.setLogicalRewrites(ruleSetFactory.getLogicalRewrites(metadataProvider.getApplicationContext()));
        builder.setPhysicalRewrites(ruleSetFactory.getPhysicalRewrites(metadataProvider.getApplicationContext()));
        IDataFormat format = metadataProvider.getFormat();
        ICompilerFactory compilerFactory = builder.create();
        builder.setExpressionEvalSizeComputer(format.getExpressionEvalSizeComputer());
        builder.setIMergeAggregationExpressionFactory(new MergeAggregationExpressionFactory());
        builder.setPartialAggregationTypeComputer(new PartialAggregationTypeComputer());
        builder.setExpressionTypeComputer(ExpressionTypeComputer.INSTANCE);
        builder.setMissableTypeComputer(MissableTypeComputer.INSTANCE);
        builder.setConflictingTypeResolver(ConflictingTypeResolver.INSTANCE);

        int parallelism = getParallelism(querySpecificConfig.get(CompilerProperties.COMPILER_PARALLELISM_KEY),
                compilerProperties.getParallelism());
        AlgebricksAbsolutePartitionConstraint computationLocations =
                chooseLocations(clusterInfoCollector, parallelism, metadataProvider.getClusterLocations());
        builder.setClusterLocations(computationLocations);

        ICompiler compiler = compilerFactory.createCompiler(plan, metadataProvider, t.getVarCounter());
        if (conf.isOptimize()) {
            compiler.optimize();
            if (conf.is(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN)) {
                if (conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS)) {
                    // For Optimizer tests.
                    AlgebricksAppendable buffer = new AlgebricksAppendable(output.out());
                    PlanPrettyPrinter.printPhysicalOps(plan, buffer, 0);
                } else {
                    printPlanPrefix(output, "Optimized logical plan");
                    if (rwQ != null || (statement != null && statement.getKind() == Statement.Kind.LOAD)) {
                         if(output.config().getOplpfmt().equals(SessionConfig.PlanFormat.CLEAN_JSON)||output.config().getOplpfmt().equals(SessionConfig.PlanFormat.JSON)) {
                        LogicalOperatorPrettyPrintVisitorJson pvisitor = new LogicalOperatorPrettyPrintVisitorJson(output.out());
                        PlanPrettyPrinter.operatorID = 0;
                             PlanPrettyPrinter.resetOperatorID(plan);
                        PlanPrettyPrinter.printPlanJson(plan, pvisitor, 0);

                    } else {
                        LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor(output.out());
                        PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
                    }
                }
                    printPlanPostfix(output);
                }
            }
        }
        if (rwQ != null && rwQ.isExplain()) {
            try {
                LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();
                PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
                ResultUtil.printResults(metadataProvider.getApplicationContext(), pvisitor.get().toString(), output,
                        new Stats(), null);
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
        builder.setExpressionRuntimeProvider(new ExpressionRuntimeProvider(QueryLogicalExpressionJobGen.INSTANCE));
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

        JobEventListenerFactory jobEventListenerFactory =
                new JobEventListenerFactory(asterixJobId, metadataProvider.isWriteTransaction());
        JobSpecification spec = compiler.createJob(metadataProvider.getApplicationContext(), jobEventListenerFactory);

        // When the top-level statement is a query, the statement parameter is null.
        if (statement == null) {
            // Sets a required capacity, only for read-only queries.
            // DDLs and DMLs are considered not that frequent.
            spec.setRequiredClusterCapacity(ResourceUtils.getRequiredCompacity(plan, computationLocations,
                    sortFrameLimit, groupFrameLimit, joinFrameLimit, frameSize));
        }

        if (conf.is(SessionConfig.OOB_HYRACKS_JOB)) {
            printPlanPrefix(output, "Hyracks job");
            if (rwQ != null) {
                try {
                    output.out().println(
                            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(spec.toJSON()));
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
                output.out().println(spec.getUserConstraints());
            }
            printPlanPostfix(output);
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

    // Chooses the location constraints, i.e., whether to use storage parallelism or use a user-sepcified number
    // of cores.
    private static AlgebricksAbsolutePartitionConstraint chooseLocations(IClusterInfoCollector clusterInfoCollector,
            int parallelismHint, AlgebricksAbsolutePartitionConstraint storageLocations) throws AlgebricksException {
        try {
            Map<String, NodeControllerInfo> ncMap = clusterInfoCollector.getNodeControllerInfos();

            // Gets total number of cores in the cluster.
            int totalNumCores = getTotalNumCores(ncMap);

            // If storage parallelism is not larger than the total number of cores, we use the storage parallelism.
            // Otherwise, we will use all available cores.
            if (parallelismHint == CompilerProperties.COMPILER_PARALLELISM_AS_STORAGE
                    && storageLocations.getLocations().length <= totalNumCores) {
                return storageLocations;
            }
            return getComputationLocations(ncMap, parallelismHint);
        } catch (HyracksException e) {
            throw new AlgebricksException(e);
        }
    }

    // Computes the location constraints based on user-configured parallelism parameter.
    // Note that the parallelism parameter is only a hint -- it will not be respected if it is too small or too large.
    private static AlgebricksAbsolutePartitionConstraint getComputationLocations(Map<String, NodeControllerInfo> ncMap,
            int parallelismHint) {
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
            int availableCores = entry.getValue().getNumAvailableCores();
            int nodeParallelism =
                    selectedNodesWithOneMorePartition.contains(nodeId) ? perNodeParallelismMax : perNodeParallelismMin;
            int coresToUse =
                    nodeParallelism >= 0 && nodeParallelism < availableCores ? nodeParallelism : availableCores;
            for (int count = 0; count < coresToUse; ++count) {
                locations.add(nodeId);
            }
        }
        return new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[0]));
    }

    // Gets the total number of available cores in the cluster.
    private static int getTotalNumCores(Map<String, NodeControllerInfo> ncMap) {
        int sum = 0;
        for (Map.Entry<String, NodeControllerInfo> entry : ncMap.entrySet()) {
            sum += entry.getValue().getNumAvailableCores();
        }
        return sum;
    }

    // Gets the frame limit.
    private static int getFrameLimit(String parameterName, String parameter, long memBudgetInConfiguration,
            int frameSize, int minFrameLimit) throws AlgebricksException {
        IOptionType<Long> longBytePropertyInterpreter = OptionTypes.LONG_BYTE_UNIT;
        long memBudget = parameter == null ? memBudgetInConfiguration : longBytePropertyInterpreter.parse(parameter);
        int frameLimit = (int) (memBudget / frameSize);
        if (frameLimit < minFrameLimit) {
            throw AsterixException.create(ErrorCode.COMPILATION_BAD_QUERY_PARAMETER_VALUE, parameterName,
                    frameSize * minFrameLimit);
        }
        // Sets the frame limit to the minimum frame limit if the caculated frame limit is too small.
        return Math.max(frameLimit, minFrameLimit);
    }

    // Gets the parallelism parameter.
    private static int getParallelism(String parameter, int parallelismInConfiguration) {
        IOptionType<Integer> integerIPropertyInterpreter = OptionTypes.INTEGER;
        return parameter == null ? parallelismInConfiguration : integerIPropertyInterpreter.parse(parameter);
    }

    // Validates if the query contains unsupported query parameters.
    private static void validateConfig(Map<String, String> config) throws AlgebricksException {
        for (String parameterName : config.keySet()) {
            if (!CONFIGURABLE_PARAMETER_NAMES.contains(parameterName)) {
                throw AsterixException.create(ErrorCode.COMPILATION_UNSUPPORTED_QUERY_PARAMETER, parameterName);
            }
        }
    }
}
