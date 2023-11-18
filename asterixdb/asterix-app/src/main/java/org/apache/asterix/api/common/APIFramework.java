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
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.fields.ExplainOnlyResultsPrinter;
import org.apache.asterix.app.result.fields.SignaturePrinter;
import org.apache.asterix.common.api.INodeJobTracker;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.Job;
import org.apache.asterix.common.utils.Job.SubmissionMode;
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
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.base.IRewriterFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.asterix.translator.CompiledStatements.ICompiledStatement;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.ResultMetadata;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.ExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksStringBuilderWriter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.rewriter.base.IOptimizationContextFactory;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.algebricks.runtime.serializer.ResultSerializerFactoryProvider;
import org.apache.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.control.common.config.OptionTypes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Provides helper methods for compilation of a query into a JobSpec and submission
 * to Hyracks through the Hyracks client interface.
 */
public class APIFramework {

    private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();

    public static final String PREFIX_INTERNAL_PARAMETERS = "_internal";

    private final IRewriterFactory rewriterFactory;
    private final IAstPrintVisitorFactory astPrintVisitorFactory;
    private final ILangExpressionToPlanTranslatorFactory translatorFactory;
    private final IRuleSetFactory ruleSetFactory;
    private final Set<String> configurableParameterNames;
    private final ExecutionPlans executionPlans;
    private PlanInfo lastPlan;

    public APIFramework(ILangCompilationProvider compilationProvider) {
        this.rewriterFactory = compilationProvider.getRewriterFactory();
        this.astPrintVisitorFactory = compilationProvider.getAstPrintVisitorFactory();
        this.translatorFactory = compilationProvider.getExpressionToPlanTranslatorFactory();
        this.ruleSetFactory = compilationProvider.getRuleSetFactory();
        this.configurableParameterNames = compilationProvider.getCompilerOptions();
        executionPlans = new ExecutionPlans();
        lastPlan = null;
    }

    private class PlanInfo {
        ILogicalPlan plan;
        Map<Object, String> log2Phys;
        boolean printOptimizerEstimates;
        SessionConfig.PlanFormat format;

        public PlanInfo(ILogicalPlan plan, Map<Object, String> log2Phys, boolean printOptimizerEstimates,
                SessionConfig.PlanFormat format) {
            this.plan = plan;
            this.log2Phys = log2Phys;
            this.printOptimizerEstimates = printOptimizerEstimates;
            this.format = format;
        }
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
                AlgebricksPartitionConstraint clusterLocations, IWarningCollector warningCollector) {
            IPlanPrettyPrinter prettyPrinter = PlanPrettyPrinter.createStringPlanPrettyPrinter();
            return new AsterixOptimizationContext(this, varCounter, expressionEvalSizeComputer,
                    mergeAggregationExpressionFactory, expressionTypeComputer, missableTypeComputer,
                    conflictingTypeResolver, physicalOptimizationConfig, clusterLocations, prettyPrinter,
                    warningCollector);
        }

        @Override
        public IOptimizationContext cloneOptimizationContext(IOptimizationContext oc) {
            return new AsterixOptimizationContext((AsterixOptimizationContext) oc);
        }
    }

    public Pair<IReturningStatement, Integer> reWriteQuery(LangRewritingContext langRewritingContext,
            IReturningStatement q, SessionOutput output, boolean allowNonStoredUdfCalls, boolean inlineUdfsAndViews,
            Collection<VarIdentifier> externalVars) throws CompilationException {
        if (q == null) {
            return null;
        }
        SessionConfig conf = output.config();
        if (!conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS) && conf.is(SessionConfig.OOB_EXPR_TREE)) {
            generateExpressionTree(q);
        }
        IQueryRewriter rw = rewriterFactory.createQueryRewriter();
        rw.rewrite(langRewritingContext, q, allowNonStoredUdfCalls, inlineUdfsAndViews, externalVars);
        return new Pair<>(q, q.getVarCounter());
    }

    public JobSpecification compileQuery(IClusterInfoCollector clusterInfoCollector, MetadataProvider metadataProvider,
            Query query, int varCounter, String outputDatasetName, SessionOutput output, ICompiledStatement statement,
            Map<VarIdentifier, IAObject> externalVars, IResponsePrinter printer, IWarningCollector warningCollector,
            IRequestParameters requestParameters, EnumSet<JobFlag> runtimeFlags)
            throws AlgebricksException, ACIDException {

        // establish facts
        final boolean isQuery = query != null;
        final boolean isLoad = statement != null && statement.getKind() == Statement.Kind.LOAD;
        final boolean isCopy = statement != null && statement.getKind() == Statement.Kind.COPY_FROM;
        final SourceLocation sourceLoc =
                query != null ? query.getSourceLocation() : statement != null ? statement.getSourceLocation() : null;
        final boolean isExplainOnly = isQuery && query.isExplain();

        SessionConfig conf = output.config();
        if (isQuery && !conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS)
                && conf.is(SessionConfig.OOB_REWRITTEN_EXPR_TREE)) {
            generateRewrittenExpressionTree(query);
        }

        final TxnId txnId = metadataProvider.getTxnIdFactory().create();
        metadataProvider.setTxnId(txnId);
        ILangExpressionToPlanTranslator t =
                translatorFactory.createExpressionToPlanTranslator(metadataProvider, varCounter, externalVars);
        ResultMetadata resultMetadata = new ResultMetadata(output.config().fmt());
        ILogicalPlan plan = isLoad || isCopy ? t.translateCopyOrLoad((ICompiledDmlStatement) statement)
                : t.translate(query, outputDatasetName, statement, resultMetadata);

        ICcApplicationContext ccAppContext = metadataProvider.getApplicationContext();
        CompilerProperties compilerProperties = ccAppContext.getCompilerProperties();
        Map<String, Object> querySpecificConfig = validateConfig(metadataProvider.getConfig(), sourceLoc);
        final PhysicalOptimizationConfig physOptConf = OptimizationConfUtil.createPhysicalOptimizationConf(
                compilerProperties, querySpecificConfig, configurableParameterNames, sourceLoc);
        boolean cboMode = physOptConf.getCBOMode() || physOptConf.getCBOTestMode();
        HeuristicCompilerFactoryBuilder builder =
                new HeuristicCompilerFactoryBuilder(OptimizationContextFactory.INSTANCE);
        builder.setPhysicalOptimizationConfig(physOptConf);
        builder.setLogicalRewrites(() -> ruleSetFactory.getLogicalRewrites(ccAppContext));
        builder.setLogicalRewritesByKind(kind -> ruleSetFactory.getLogicalRewrites(kind, ccAppContext));
        builder.setPhysicalRewrites(() -> ruleSetFactory.getPhysicalRewrites(ccAppContext));
        IDataFormat format = metadataProvider.getDataFormat();
        ICompilerFactory compilerFactory = builder.create();
        builder.setExpressionEvalSizeComputer(format.getExpressionEvalSizeComputer());
        builder.setIMergeAggregationExpressionFactory(new MergeAggregationExpressionFactory());
        builder.setPartialAggregationTypeComputer(new PartialAggregationTypeComputer());
        builder.setExpressionTypeComputer(ExpressionTypeComputer.INSTANCE);
        builder.setMissableTypeComputer(MissableTypeComputer.INSTANCE);
        builder.setConflictingTypeResolver(ConflictingTypeResolver.INSTANCE);
        builder.setWarningCollector(warningCollector);
        builder.setMaxWarnings(conf.getMaxWarnings());

        if ((isQuery || isLoad || isCopy) && !conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS)
                && conf.is(SessionConfig.OOB_LOGICAL_PLAN)) {
            generateLogicalPlan(plan, output.config().getPlanFormat(), cboMode);
        }

        int parallelism = getParallelism((String) querySpecificConfig.get(CompilerProperties.COMPILER_PARALLELISM_KEY),
                compilerProperties.getParallelism());
        AlgebricksAbsolutePartitionConstraint computationLocations =
                chooseLocations(clusterInfoCollector, parallelism, metadataProvider.getClusterLocations());
        builder.setClusterLocations(computationLocations);

        builder.setBinaryBooleanInspectorFactory(format.getBinaryBooleanInspectorFactory());
        builder.setBinaryIntegerInspectorFactory(format.getBinaryIntegerInspectorFactory());
        builder.setComparatorFactoryProvider(format.getBinaryComparatorFactoryProvider());
        builder.setExpressionRuntimeProvider(
                new ExpressionRuntimeProvider(new QueryLogicalExpressionJobGen(metadataProvider.getFunctionManager())));
        builder.setHashFunctionFactoryProvider(format.getBinaryHashFunctionFactoryProvider());
        builder.setHashFunctionFamilyProvider(format.getBinaryHashFunctionFamilyProvider());
        builder.setMissingWriterFactory(format.getMissingWriterFactory());
        builder.setNullWriterFactory(format.getNullWriterFactory());
        builder.setUnnestingPositionWriterFactory(format.getUnnestingPositionWriterFactory());
        builder.setPredicateEvaluatorFactoryProvider(format.getPredicateEvaluatorFactoryProvider());
        builder.setPrinterProvider(getPrinterFactoryProvider(format, conf.fmt()));
        builder.setWriterFactory(PrinterBasedWriterFactory.INSTANCE);
        builder.setResultSerializerFactoryProvider(ResultSerializerFactoryProvider.INSTANCE);
        builder.setSerializerDeserializerProvider(format.getSerdeProvider());
        builder.setTypeTraitProvider(format.getTypeTraitProvider());
        builder.setNormalizedKeyComputerFactoryProvider(format.getNormalizedKeyComputerFactoryProvider());

        ICompiler compiler = compilerFactory.createCompiler(plan, metadataProvider, t.getVarCounter());
        if (conf.isOptimize()) {
            compiler.optimize();
            if (conf.is(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN) || isExplainOnly) {
                if (conf.is(SessionConfig.FORMAT_ONLY_PHYSICAL_OPS)) {
                    // For Optimizer tests. Print physical operators in verbose mode.
                    AlgebricksStringBuilderWriter buf = new AlgebricksStringBuilderWriter(PlanPrettyPrinter.INIT_SIZE);
                    PlanPrettyPrinter.printPhysicalOps(plan, buf, 0, true);
                    output.out().write(buf.toString());
                } else {
                    if (isQuery || isLoad || isCopy) {
                        generateOptimizedLogicalPlan(plan, output.config().getPlanFormat(), cboMode);
                    }
                }
            }
        }

        if (conf.getClientType() == SessionConfig.ClientType.JDBC) {
            executionPlans.setStatementCategory(Statement.Category.toString(getStatementCategory(query, statement)));
            if (!conf.isExecuteQuery()) {
                String stmtParams = ResultUtil.ParseOnlyResult.printStatementParameters(externalVars.keySet(), v -> v);
                executionPlans.setStatementParameters(stmtParams);
            }
            if (isExplainOnly) {
                executionPlans.setExplainOnly(true);
            } else if (isQuery) {
                executionPlans.setSignature(SignaturePrinter.generateFlatSignature(resultMetadata));
            }
        }

        boolean printSignature = isQuery && requestParameters != null && requestParameters.isPrintSignature();

        if (printSignature && !isExplainOnly) { //explainOnly adds the signature later
            printer.addResultPrinter(SignaturePrinter.newInstance(executionPlans));
        }

        if (!conf.isGenerateJobSpec()) {
            if (isQuery || isLoad || isCopy) {
                generateOptimizedLogicalPlan(plan, output.config().getPlanFormat(), cboMode);
            }
            return null;
        }

        JobEventListenerFactory jobEventListenerFactory =
                new JobEventListenerFactory(txnId, metadataProvider.isWriteTransaction());
        JobSpecification spec = compiler.createJob(ccAppContext, jobEventListenerFactory, runtimeFlags);

        if (isQuery) {
            if (!compiler.skipJobCapacityAssignment()) {
                if (requestParameters == null || !requestParameters.isSkipAdmissionPolicy()) {
                    // Sets a required capacity, only for read-only queries.
                    // DDLs and DMLs are considered not that frequent.
                    // limit the computation locations to the locations that will be used in the query
                    final INodeJobTracker nodeJobTracker = ccAppContext.getNodeJobTracker();
                    final AlgebricksAbsolutePartitionConstraint jobLocations =
                            getJobLocations(spec, nodeJobTracker, computationLocations);
                    final IClusterCapacity jobRequiredCapacity =
                            ResourceUtils.getRequiredCapacity(plan, jobLocations, physOptConf);
                    spec.setRequiredClusterCapacity(jobRequiredCapacity);
                }
            }
        }

        if (conf.is(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN) || isExplainOnly) {
            if (isQuery || isLoad || isCopy) {
                generateOptimizedLogicalPlan(plan, spec.getLogical2PhysicalMap(), output.config().getPlanFormat(),
                        cboMode);
                if (runtimeFlags.contains(JobFlag.PROFILE_RUNTIME)) {
                    lastPlan =
                            new PlanInfo(plan, spec.getLogical2PhysicalMap(), cboMode, output.config().getPlanFormat());
                }
            }
        }

        if (isExplainOnly) {
            printPlanAsResult(metadataProvider, output, printer, printSignature);
            if (!conf.is(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN)) {
                executionPlans.setOptimizedLogicalPlan(null);
            }
            return null;
        }

        if (isQuery && conf.is(SessionConfig.OOB_HYRACKS_JOB)) {
            generateJob(spec);
        }
        return spec;
    }

    private void printPlanAsResult(MetadataProvider metadataProvider, SessionOutput output, IResponsePrinter printer,
            boolean printSignature) throws AlgebricksException {
        try {
            if (printSignature) {
                printer.addResultPrinter(SignaturePrinter.INSTANCE);
            }
            printer.addResultPrinter(new ExplainOnlyResultsPrinter(metadataProvider.getApplicationContext(),
                    executionPlans.getOptimizedLogicalPlan(), output));
            printer.printResults();
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    protected IPrinterFactoryProvider getPrinterFactoryProvider(IDataFormat format,
            SessionConfig.OutputFormat outputFormat) throws AlgebricksException {
        switch (outputFormat) {
            case LOSSLESS_JSON:
                return format.getLosslessJSONPrinterFactoryProvider();
            case LOSSLESS_ADM_JSON:
                return format.getLosslessADMJSONPrinterFactoryProvider();
            case CSV:
                return format.getCSVPrinterFactoryProvider();
            case ADM:
                return format.getADMPrinterFactoryProvider();
            case CLEAN_JSON:
                return format.getCleanJSONPrinterFactoryProvider();
            default:
                throw new AlgebricksException("Unexpected OutputFormat: " + outputFormat);
        }
    }

    private IPlanPrettyPrinter getPrettyPrintVisitor(SessionConfig.PlanFormat planFormat) {
        return planFormat.equals(SessionConfig.PlanFormat.JSON) ? PlanPrettyPrinter.createJsonPlanPrettyPrinter()
                : PlanPrettyPrinter.createStringPlanPrettyPrinter();
    }

    private byte getStatementCategory(Query query, ICompiledStatement statement) {
        return statement != null && statement.getKind() != Statement.Kind.COPY_TO
                ? ((ICompiledDmlStatement) statement).getCategory()
                : query != null ? Statement.Category.QUERY : Statement.Category.DDL;
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

    public ExecutionPlans getExecutionPlans() {
        return executionPlans;
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
        ncMap.forEach((key, value) -> allNodes.add(key));
        Random random = new Random();
        for (int index = numNodesWithOneMorePartition; index >= 1; --index) {
            int pick = random.nextInt(index);
            selectedNodesWithOneMorePartition.add(allNodes.get(pick));
            Collections.swap(allNodes, pick, index - 1);
        }

        // Generates cluster locations, which has duplicates for a node if it contains more than one partitions.
        List<String> locations = new ArrayList<>();
        ncMap.forEach((nodeId, value) -> {
            int availableCores = value.getNumAvailableCores();
            int nodeParallelism =
                    selectedNodesWithOneMorePartition.contains(nodeId) ? perNodeParallelismMax : perNodeParallelismMin;
            int coresToUse =
                    nodeParallelism >= 0 && nodeParallelism < availableCores ? nodeParallelism : availableCores;
            for (int count = 0; count < coresToUse; ++count) {
                locations.add(nodeId);
            }
        });
        return new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[0]));
    }

    // Gets the total number of available cores in the cluster.
    private static int getTotalNumCores(Map<String, NodeControllerInfo> ncMap) {
        return ncMap.values().stream().mapToInt(NodeControllerInfo::getNumAvailableCores).sum();
    }

    // Gets the parallelism parameter.
    private static int getParallelism(String parameter, int parallelismInConfiguration) {
        IOptionType<Integer> integerIPropertyInterpreter = OptionTypes.INTEGER;
        return parameter == null ? parallelismInConfiguration : integerIPropertyInterpreter.parse(parameter);
    }

    // Validates if the query contains unsupported query parameters.
    private Map<String, Object> validateConfig(Map<String, Object> config, SourceLocation sourceLoc)
            throws AlgebricksException {
        for (String parameterName : config.keySet()) {
            if (!configurableParameterNames.contains(parameterName)
                    && !parameterName.startsWith(PREFIX_INTERNAL_PARAMETERS)) {
                throw AsterixException.create(ErrorCode.COMPILATION_UNSUPPORTED_QUERY_PARAMETER, sourceLoc,
                        parameterName);
            }
        }
        return config;
    }

    private void generateExpressionTree(IReturningStatement statement) throws CompilationException {
        final StringWriter stringWriter = new StringWriter();
        try (PrintWriter writer = new PrintWriter(stringWriter)) {
            statement.accept(astPrintVisitorFactory.createLangVisitor(writer), 0);
            executionPlans.setExpressionTree(stringWriter.toString());
        }
    }

    private void generateRewrittenExpressionTree(IReturningStatement statement) throws CompilationException {
        final StringWriter stringWriter = new StringWriter();
        try (PrintWriter writer = new PrintWriter(stringWriter)) {
            statement.accept(astPrintVisitorFactory.createLangVisitor(writer), 0);
            executionPlans.setRewrittenExpressionTree(stringWriter.toString());
        }
    }

    private void generateLogicalPlan(ILogicalPlan plan, SessionConfig.PlanFormat format,
            boolean printOptimizerEstimates) throws AlgebricksException {
        executionPlans
                .setLogicalPlan(getPrettyPrintVisitor(format).printPlan(plan, printOptimizerEstimates).toString());
    }

    private void generateOptimizedLogicalPlan(ILogicalPlan plan, Map<Object, String> log2phys,
            SessionConfig.PlanFormat format, boolean printOptimizerEstimates) throws AlgebricksException {
        executionPlans.setOptimizedLogicalPlan(
                getPrettyPrintVisitor(format).printPlan(plan, log2phys, printOptimizerEstimates).toString());
    }

    public void generateOptimizedLogicalPlanWithProfile(ObjectNode profile) throws HyracksDataException {
        /*TODO(ian): we call this and overwrite the non-annotated plan, but there should be some way to skip initial
                     plan printing if both profiling and plan printing are requested. */
        try {
            if (lastPlan != null) {
                executionPlans.setOptimizedLogicalPlan(getPrettyPrintVisitor(lastPlan.format)
                        .printPlan(lastPlan.plan, lastPlan.log2Phys, lastPlan.printOptimizerEstimates, profile)
                        .toString());
            }
        } catch (AlgebricksException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void generateOptimizedLogicalPlan(ILogicalPlan plan, SessionConfig.PlanFormat format,
            boolean printOptimizerEstimates) throws AlgebricksException {
        executionPlans.setOptimizedLogicalPlan(
                getPrettyPrintVisitor(format).printPlan(plan, printOptimizerEstimates).toString());
    }

    private void generateJob(JobSpecification spec) {
        final StringWriter stringWriter = new StringWriter();
        try (PrintWriter writer = new PrintWriter(stringWriter)) {
            writer.println(OBJECT_WRITER.writeValueAsString(spec.toJSON()));
            executionPlans.setJob(stringWriter.toString());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static AlgebricksAbsolutePartitionConstraint getJobLocations(JobSpecification spec,
            INodeJobTracker jobTracker, AlgebricksAbsolutePartitionConstraint clusterLocations) {
        final Set<String> jobParticipatingNodes = jobTracker.getJobParticipatingNodes(spec, null);
        return new AlgebricksAbsolutePartitionConstraint(Arrays.stream(clusterLocations.getLocations())
                .filter(jobParticipatingNodes::contains).toArray(String[]::new));
    }
}
