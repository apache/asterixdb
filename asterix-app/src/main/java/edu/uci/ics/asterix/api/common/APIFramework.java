package edu.uci.ics.asterix.api.common;

import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import edu.uci.ics.asterix.api.common.Job.SubmissionMode;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.visitor.AQLPrintVisitor;
import edu.uci.ics.asterix.aql.rewrites.AqlRewriter;
import edu.uci.ics.asterix.aql.translator.DdlTranslator;
import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.OptimizationConfUtil;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.common.AqlExpressionTypeComputer;
import edu.uci.ics.asterix.dataflow.data.common.AqlMergeAggregationExpressionFactory;
import edu.uci.ics.asterix.dataflow.data.common.AqlNullableTypeComputer;
import edu.uci.ics.asterix.dataflow.data.common.AqlPartialAggregationTypeComputer;
import edu.uci.ics.asterix.file.DatasetOperations;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.asterix.file.IndexOperations;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.jobgen.AqlLogicalExpressionJobGen;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.optimizer.base.RuleCollections;
import edu.uci.ics.asterix.runtime.job.listener.JobEventListenerFactory;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionIDFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.translator.AqlExpressionToPlanTranslator;
import edu.uci.ics.asterix.translator.DmlTranslator;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledBeginFeedStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledControlFeedStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledDeleteStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledInsertStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledLoadFromFileStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledWriteFromQueryResultStatement;
import edu.uci.ics.asterix.translator.DmlTranslator.ICompiledDmlStatement;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.compiler.api.HeuristicCompilerFactoryBuilder;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompiler;
import edu.uci.ics.hyracks.algebricks.compiler.api.ICompilerFactory;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialFixpointRuleController;
import edu.uci.ics.hyracks.algebricks.compiler.rewriter.rulecontrollers.SequentialOnceRuleController;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlanAndMetadata;
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
                RuleCollections.buildOpPushDownRuleCollection()));
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

    public static String compileDdlStatements(IHyracksClientConnection hcc, Query query, PrintWriter out,
            SessionConfig pc, DisplayFormat pdf) throws AsterixException, AlgebricksException, JSONException,
            RemoteException, ACIDException {
        // Begin a transaction against the metadata.
        // Lock the metadata in X mode to protect against other DDL and DML.
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.EXCLUSIVE);
        try {
            DdlTranslator ddlt = new DdlTranslator(mdTxnCtx, query.getPrologDeclList(), out, pc, pdf);
            ddlt.translate(hcc, false);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return ddlt.getCompiledDeclarations().getDataverseName();
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            e.printStackTrace();
            throw new AlgebricksException(e);
        }
    }

    public static Job[] compileDmlStatements(String dataverseName, Query query, PrintWriter out, SessionConfig pc,
            DisplayFormat pdf) throws AsterixException, AlgebricksException, JSONException, RemoteException,
            ACIDException {

        // Begin a transaction against the metadata.
        // Lock the metadata in S mode to protect against other DDL
        // modifications.
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.SHARED);
        try {
            DmlTranslator dmlt = new DmlTranslator(mdTxnCtx, query.getPrologDeclList());
            dmlt.translate();

            if (dmlt.getCompiledDmlStatements().size() == 0) {
                // There is no DML to run. Consider the transaction against the
                // metadata successful.
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return new Job[] {};
            }

            List<Job> dmlJobs = new ArrayList<Job>();
            AqlCompiledMetadataDeclarations metadata = dmlt.getCompiledDeclarations();

            if (!metadata.isConnectedToDataverse())
                metadata.connectToDataverse(metadata.getDataverseName());

            for (ICompiledDmlStatement stmt : dmlt.getCompiledDmlStatements()) {
                switch (stmt.getKind()) {
                    case LOAD_FROM_FILE: {
                        CompiledLoadFromFileStatement stmtLoad = (CompiledLoadFromFileStatement) stmt;
                        dmlJobs.add(DatasetOperations.createLoadDatasetJobSpec(stmtLoad, metadata));
                        break;
                    }
                    case WRITE_FROM_QUERY_RESULT: {
                        CompiledWriteFromQueryResultStatement stmtLoad = (CompiledWriteFromQueryResultStatement) stmt;
                        SessionConfig sc2 = new SessionConfig(pc.getPort(), true, pc.isPrintExprParam(),
                                pc.isPrintRewrittenExprParam(), pc.isPrintLogicalPlanParam(),
                                pc.isPrintOptimizedLogicalPlanParam(), pc.isPrintPhysicalOpsOnly(), pc.isPrintJob());
                        sc2.setGenerateJobSpec(true);
                        Pair<AqlCompiledMetadataDeclarations, JobSpecification> mj = compileQueryInternal(mdTxnCtx,
                                dataverseName, stmtLoad.getQuery(), stmtLoad.getVarCounter(),
                                stmtLoad.getDatasetName(), metadata, sc2, out, pdf,
                                Statement.Kind.WRITE_FROM_QUERY_RESULT);
                        dmlJobs.add(new Job(mj.second));
                        break;
                    }
                    case INSERT: {
                        CompiledInsertStatement stmtLoad = (CompiledInsertStatement) stmt;
                        SessionConfig sc2 = new SessionConfig(pc.getPort(), true, pc.isPrintExprParam(),
                                pc.isPrintRewrittenExprParam(), pc.isPrintLogicalPlanParam(),
                                pc.isPrintOptimizedLogicalPlanParam(), pc.isPrintPhysicalOpsOnly(), pc.isPrintJob());
                        sc2.setGenerateJobSpec(true);
                        Pair<AqlCompiledMetadataDeclarations, JobSpecification> mj = compileQueryInternal(mdTxnCtx,
                                dataverseName, stmtLoad.getQuery(), stmtLoad.getVarCounter(),
                                stmtLoad.getDatasetName(), metadata, sc2, out, pdf, Statement.Kind.INSERT);
                        dmlJobs.add(new Job(mj.second));
                        break;
                    }
                    case DELETE: {
                        CompiledDeleteStatement stmtLoad = (CompiledDeleteStatement) stmt;
                        SessionConfig sc2 = new SessionConfig(pc.getPort(), true, pc.isPrintExprParam(),
                                pc.isPrintRewrittenExprParam(), pc.isPrintLogicalPlanParam(),
                                pc.isPrintOptimizedLogicalPlanParam(), pc.isPrintPhysicalOpsOnly(), pc.isPrintJob());
                        sc2.setGenerateJobSpec(true);
                        Pair<AqlCompiledMetadataDeclarations, JobSpecification> mj = compileQueryInternal(mdTxnCtx,
                                dataverseName, stmtLoad.getQuery(), stmtLoad.getVarCounter(),
                                stmtLoad.getDatasetName(), metadata, sc2, out, pdf, Statement.Kind.DELETE);
                        dmlJobs.add(new Job(mj.second));
                        break;
                    }
                    case CREATE_INDEX: {
                        CompiledCreateIndexStatement cis = (CompiledCreateIndexStatement) stmt;
                        JobSpecification jobSpec = IndexOperations.buildSecondaryIndexLoadingJobSpec(cis, metadata);
                        dmlJobs.add(new Job(jobSpec));
                        break;
                    }

                    case BEGIN_FEED: {
                        CompiledBeginFeedStatement cbfs = (CompiledBeginFeedStatement) stmt;
                        SessionConfig sc2 = new SessionConfig(pc.getPort(), true, pc.isPrintExprParam(),
                                pc.isPrintRewrittenExprParam(), pc.isPrintLogicalPlanParam(),
                                pc.isPrintOptimizedLogicalPlanParam(), pc.isPrintPhysicalOpsOnly(), pc.isPrintJob());
                        sc2.setGenerateJobSpec(true);
                        Pair<AqlCompiledMetadataDeclarations, JobSpecification> mj = compileQueryInternal(mdTxnCtx,
                                dataverseName, cbfs.getQuery(), cbfs.getVarCounter(), cbfs.getDatasetName().getValue(),
                                metadata, sc2, out, pdf, Statement.Kind.BEGIN_FEED);
                        dmlJobs.add(new Job(mj.second));
                        break;

                    }

                    case CONTROL_FEED: {
                        CompiledControlFeedStatement cfs = (CompiledControlFeedStatement) stmt;
                        Job job = new Job(FeedOperations.buildControlFeedJobSpec(cfs, metadata),
                                SubmissionMode.ASYNCHRONOUS);
                        dmlJobs.add(job);
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException();
                    }
                }
            }
            if (pc.isPrintJob()) {
                int i = 0;
                for (Job js : dmlJobs) {
                    out.println("<H1>Hyracks job number " + i + ":</H1>");
                    out.println("<PRE>");
                    out.println(js.getJobSpec().toJSON().toString(1));
                    out.println(js.getJobSpec().getUserConstraints());
                    out.println(js.getSubmissionMode());
                    out.println("</PRE>");
                    i++;
                }
            }
            // close connection to dataverse
            if (metadata.isConnectedToDataverse())
                metadata.disconnectFromDataverse();

            Job[] jobs = dmlJobs.toArray(new Job[0]);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return jobs;
        } catch (AsterixException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (AlgebricksException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (JSONException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw new AsterixException(e);
        }
    }

    public static Pair<AqlCompiledMetadataDeclarations, JobSpecification> compileQuery(String dataverseName, Query q,
            int varCounter, String outputDatasetName, AqlCompiledMetadataDeclarations metadataDecls, SessionConfig pc,
            PrintWriter out, DisplayFormat pdf, Statement.Kind dmlKind) throws AsterixException, AlgebricksException,
            JSONException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.SHARED);
            Pair<AqlCompiledMetadataDeclarations, JobSpecification> result = compileQueryInternal(mdTxnCtx,
                    dataverseName, q, varCounter, outputDatasetName, metadataDecls, pc, out, pdf, dmlKind);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return result;
        } catch (AsterixException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (AlgebricksException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (JSONException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (RemoteException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (ACIDException e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw new AsterixException(e);
        }
    }

    public static Pair<AqlCompiledMetadataDeclarations, JobSpecification> compileQueryInternal(
            MetadataTransactionContext mdTxnCtx, String dataverseName, Query q, int varCounter,
            String outputDatasetName, AqlCompiledMetadataDeclarations metadataDecls, SessionConfig pc, PrintWriter out,
            DisplayFormat pdf, Statement.Kind dmlKind) throws AsterixException, AlgebricksException, JSONException,
            RemoteException, ACIDException {

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
        AqlRewriter rw = new AqlRewriter(q, varCounter, mdTxnCtx, dataverseName);
        rw.rewrite();
        Query rwQ = rw.getExpr();
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

            if (q != null) {
                rwQ.accept(new AQLPrintVisitor(out), 0);
            }

            switch (pdf) {
                case HTML: {
                    out.println("</PRE>");
                    break;
                }
            }

        }
        long txnId = TransactionIDFactory.generateTransactionId();
        AqlExpressionToPlanTranslator t = new AqlExpressionToPlanTranslator(txnId, mdTxnCtx, rw.getVarCounter(),
                outputDatasetName, dmlKind);

        ILogicalPlanAndMetadata planAndMetadata = t.translate(rwQ, metadataDecls);
        boolean isWriteTransaction = false;
        AqlMetadataProvider mp = (AqlMetadataProvider) planAndMetadata.getMetadataProvider();
        if (metadataDecls == null) {
            metadataDecls = mp.getMetadataDeclarations();
        }
        isWriteTransaction = mp.isWriteTransaction();

        if (outputDatasetName == null && metadataDecls.getOutputFile() == null) {
            throw new AlgebricksException("Unknown output file: `write output to nc:\"file\"' statement missing.");
        }

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

            if (q != null) {
                StringBuilder buffer = new StringBuilder();
                PlanPrettyPrinter.printPlan(planAndMetadata.getPlan(), buffer, pvisitor, 0);
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
        IDataFormat format = metadataDecls.getFormat();
        ICompilerFactory compilerFactory = builder.create();
        builder.setFrameSize(frameSize);
        builder.setExpressionEvalSizeComputer(format.getExpressionEvalSizeComputer());
        builder.setIMergeAggregationExpressionFactory(new AqlMergeAggregationExpressionFactory());
        builder.setPartialAggregationTypeComputer(new AqlPartialAggregationTypeComputer());
        builder.setExpressionTypeComputer(AqlExpressionTypeComputer.INSTANCE);
        builder.setNullableTypeComputer(AqlNullableTypeComputer.INSTANCE);

        OptimizationConfUtil.getPhysicalOptimizationConfig().setFrameSize(frameSize);
        builder.setPhysicalOptimizationConfig(OptimizationConfUtil.getPhysicalOptimizationConfig());
        ICompiler compiler = compilerFactory.createCompiler(planAndMetadata.getPlan(),
                planAndMetadata.getMetadataProvider(), t.getVarCounter());
        if (pc.isOptimize()) {
            compiler.optimize();
            if (true) {
                StringBuilder buffer = new StringBuilder();
                PlanPrettyPrinter.printPhysicalOps(planAndMetadata.getPlan(), buffer, 0);
                out.print(buffer);
            } else if (pc.isPrintOptimizedLogicalPlanParam()) {
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

                if (q != null) {
                    StringBuilder buffer = new StringBuilder();
                    PlanPrettyPrinter.printPlan(planAndMetadata.getPlan(), buffer, pvisitor, 0);
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

        if (!pc.isGenerateJobSpec()) {
            // Job spec not requested. Consider transaction against metadata
            // committed.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            return null;
        }

        AlgebricksPartitionConstraint clusterLocs = planAndMetadata.getClusterLocations();
        builder.setBinaryBooleanInspectorFactory(format.getBinaryBooleanInspectorFactory());
        builder.setBinaryIntegerInspectorFactory(format.getBinaryIntegerInspectorFactory());
        builder.setClusterLocations(clusterLocs);
        builder.setComparatorFactoryProvider(format.getBinaryComparatorFactoryProvider());
        builder.setExpressionRuntimeProvider(new LogicalExpressionJobGenToExpressionRuntimeProviderAdapter(
                AqlLogicalExpressionJobGen.INSTANCE));
        builder.setHashFunctionFactoryProvider(format.getBinaryHashFunctionFactoryProvider());
        builder.setNullWriterFactory(format.getNullWriterFactory());
        builder.setPrinterProvider(format.getPrinterFactoryProvider());
        builder.setSerializerDeserializerProvider(format.getSerdeProvider());
        builder.setTypeTraitProvider(format.getTypeTraitProvider());
        builder.setNormalizedKeyComputerFactoryProvider(format.getNormalizedKeyComputerFactoryProvider());

        JobSpecification spec = compiler.createJob(AsterixAppContextInfoImpl.INSTANCE);
        // set the job event listener
        spec.setJobletEventListenerFactory(new JobEventListenerFactory(txnId, isWriteTransaction));
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
            if (q != null) {
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
        return new Pair<AqlCompiledMetadataDeclarations, JobSpecification>(metadataDecls, spec);
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

}
