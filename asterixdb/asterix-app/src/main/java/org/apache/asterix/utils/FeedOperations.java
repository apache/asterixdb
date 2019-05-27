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
package org.apache.asterix.utils;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.DefaultStatementExecutorFactory;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.operators.FeedCollectOperatorDescriptor;
import org.apache.asterix.external.operators.FeedIntakeOperatorDescriptor;
import org.apache.asterix.external.operators.FeedMetaOperatorDescriptor;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.UpsertStatement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.feeds.FeedMetadataUtil;
import org.apache.asterix.metadata.feeds.LocationConstraint;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.job.listener.MultiTransactionJobletEventListenerFactory;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.asterix.translator.CompiledStatements;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.constraints.expressions.ConstantExpression;
import org.apache.hyracks.api.constraints.expressions.ConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.LValueConstraintExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionCountExpression;
import org.apache.hyracks.api.constraints.expressions.PartitionLocationExpression;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningWithMessageConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileRemoveOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    public static final String FEED_DATAFLOW_INTERMEIDATE_VAL_PREFIX = "val";

    private FeedOperations() {
    }

    private static Pair<JobSpecification, IAdapterFactory> buildFeedIntakeJobSpec(Feed feed,
            MetadataProvider metadataProvider, FeedPolicyAccessor policyAccessor) throws Exception {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        spec.setFrameSize(metadataProvider.getApplicationContext().getCompilerProperties().getFrameSize());
        IAdapterFactory adapterFactory;
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;
        Triple<IOperatorDescriptor, AlgebricksPartitionConstraint, IAdapterFactory> t =
                metadataProvider.buildFeedIntakeRuntime(spec, feed, policyAccessor);
        feedIngestor = t.first;
        ingesterPc = t.second;
        adapterFactory = t.third;
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return Pair.of(spec, adapterFactory);
    }

    public static JobSpecification buildRemoveFeedStorageJob(MetadataProvider metadataProvider, Feed feed)
            throws AsterixException {
        ICcApplicationContext appCtx = metadataProvider.getApplicationContext();
        JobSpecification spec = RuntimeUtils.createJobSpecification(appCtx);
        IClusterStateManager csm = appCtx.getClusterStateManager();
        AlgebricksAbsolutePartitionConstraint allCluster = csm.getClusterLocations();
        Set<String> nodes = new TreeSet<>();
        for (String node : allCluster.getLocations()) {
            nodes.add(node);
        }
        AlgebricksAbsolutePartitionConstraint locations =
                new AlgebricksAbsolutePartitionConstraint(nodes.toArray(new String[nodes.size()]));
        FileSplit[] feedLogFileSplits =
                FeedUtils.splitsForAdapter(appCtx, feed.getDataverseName(), feed.getFeedName(), locations);
        org.apache.hyracks.algebricks.common.utils.Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spC =
                StoragePathUtil.splitProviderAndPartitionConstraints(feedLogFileSplits);
        FileRemoveOperatorDescriptor frod = new FileRemoveOperatorDescriptor(spec, spC.first, true);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, frod, spC.second);
        spec.addRoot(frod);
        return spec;
    }

    private static List<Expression> addArgs(Object... args) {
        List<Expression> argExprs = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof Integer) {
                argExprs.add(new LiteralExpr(new IntegerLiteral((Integer) arg)));
            } else if (arg instanceof String) {
                argExprs.add(new LiteralExpr(new StringLiteral((String) arg)));
            } else if (arg instanceof Expression) {
                argExprs.add((Expression) arg);
            }
        }
        return argExprs;
    }

    private static Query makeConnectionQuery(FeedConnection feedConnection) throws AlgebricksException {
        // Construct from clause
        VarIdentifier fromVarId = SqlppVariableUtil.toInternalVariableIdentifier(feedConnection.getFeedName());
        VariableExpr fromTermLeftExpr = new VariableExpr(fromVarId);
        // TODO: remove target feedid from args list (xikui)
        // TODO: Get rid of this INTAKE
        List<Expression> exprList = addArgs(feedConnection.getDataverseName(),
                feedConnection.getFeedId().getEntityName(), feedConnection.getFeedId().getEntityName(),
                FeedRuntimeType.INTAKE.toString(), feedConnection.getDatasetName(), feedConnection.getOutputType());
        CallExpr datasrouceCallFunction = new CallExpr(new FunctionSignature(BuiltinFunctions.FEED_COLLECT), exprList);
        FromTerm fromterm = new FromTerm(datasrouceCallFunction, fromTermLeftExpr, null, null);
        FromClause fromClause = new FromClause(Arrays.asList(fromterm));
        WhereClause whereClause = null;
        if (feedConnection.getWhereClauseBody().length() != 0) {
            String whereClauseExpr = feedConnection.getWhereClauseBody() + ";";
            IParserFactory sqlppParserFactory = new SqlppParserFactory();
            IParser sqlppParser = sqlppParserFactory.createParser(whereClauseExpr);
            List<Statement> stmts = sqlppParser.parse();
            if (stmts.size() != 1) {
                throw new CompilationException("Exceptions happened in processing where clause.");
            }
            Query whereClauseQuery = (Query) stmts.get(0);
            whereClause = new WhereClause(whereClauseQuery.getBody());
        }

        // Attaching functions
        Expression previousVarExpr = fromTermLeftExpr;
        for (FunctionSignature functionSignature : feedConnection.getAppliedFunctions()) {
            CallExpr functionCallExpr = new CallExpr(functionSignature, addArgs(previousVarExpr));
            previousVarExpr = functionCallExpr;
        }
        // Constructing select clause
        SelectElement selectElement = new SelectElement(previousVarExpr);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause,
                whereClause != null ? Collections.singletonList(whereClause) : null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        SelectExpression body = new SelectExpression(null, selectSetOperation, null, null, true);
        Query query = new Query(false, true, body, 0);
        return query;
    }

    private static JobSpecification getConnectionJob(MetadataProvider metadataProvider, FeedConnection feedConn,
            IStatementExecutor statementExecutor, IHyracksClientConnection hcc, Boolean insertFeed)
            throws AlgebricksException, RemoteException, ACIDException {
        metadataProvider.getConfig().put(FeedActivityDetails.FEED_POLICY_NAME, feedConn.getPolicyName());
        Query feedConnQuery = makeConnectionQuery(feedConn);
        CompiledStatements.ICompiledDmlStatement clfrqs;
        if (insertFeed) {
            InsertStatement stmtUpsert = new InsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
            clfrqs = new CompiledStatements.CompiledInsertStatement(feedConn.getDataverseName(),
                    feedConn.getDatasetName(), feedConnQuery, stmtUpsert.getVarCounter(), null, null);
        } else {
            UpsertStatement stmtUpsert = new UpsertStatement(new Identifier(feedConn.getDataverseName()),
                    new Identifier(feedConn.getDatasetName()), feedConnQuery, -1, null, null);
            clfrqs = new CompiledStatements.CompiledUpsertStatement(feedConn.getDataverseName(),
                    feedConn.getDatasetName(), feedConnQuery, stmtUpsert.getVarCounter(), null, null);
        }
        return statementExecutor.rewriteCompileQuery(hcc, metadataProvider, feedConnQuery, clfrqs, null, null);
    }

    private static JobSpecification combineIntakeCollectJobs(MetadataProvider metadataProvider, Feed feed,
            JobSpecification intakeJob, List<JobSpecification> jobsList, List<FeedConnection> feedConnections,
            String[] intakeLocations) throws AlgebricksException, HyracksDataException {
        JobSpecification jobSpec = new JobSpecification(intakeJob.getFrameSize());

        // copy ingestor
        FeedIntakeOperatorDescriptor firstOp =
                (FeedIntakeOperatorDescriptor) intakeJob.getOperatorMap().get(new OperatorDescriptorId(0));
        FeedIntakeOperatorDescriptor ingestionOp;
        if (firstOp.getAdaptorFactory() == null) {
            ingestionOp = new FeedIntakeOperatorDescriptor(jobSpec, feed, firstOp.getAdaptorLibraryName(),
                    firstOp.getAdaptorFactoryClassName(), firstOp.getAdapterOutputType(), firstOp.getPolicyAccessor(),
                    firstOp.getOutputRecordDescriptors()[0]);
        } else {
            ingestionOp = new FeedIntakeOperatorDescriptor(jobSpec, feed, firstOp.getAdaptorFactory(),
                    firstOp.getAdapterOutputType(), firstOp.getPolicyAccessor(),
                    firstOp.getOutputRecordDescriptors()[0]);
        }
        // create replicator
        ReplicateOperatorDescriptor replicateOp =
                new ReplicateOperatorDescriptor(jobSpec, ingestionOp.getOutputRecordDescriptors()[0], jobsList.size());
        jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), ingestionOp, 0, replicateOp, 0);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, ingestionOp, intakeLocations);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, replicateOp, intakeLocations);
        // Loop over the jobs to copy operators and connections
        Map<OperatorDescriptorId, OperatorDescriptorId> operatorIdMapping = new HashMap<>();
        Map<ConnectorDescriptorId, ConnectorDescriptorId> connectorIdMapping = new HashMap<>();
        Map<OperatorDescriptorId, List<LocationConstraint>> operatorLocations = new HashMap<>();
        Map<OperatorDescriptorId, Integer> operatorCounts = new HashMap<>();
        Map<Integer, TxnId> txnIdMap = new HashMap<>();
        FeedMetaOperatorDescriptor metaOp;

        for (int iter1 = 0; iter1 < jobsList.size(); iter1++) {
            FeedConnection curFeedConnection = feedConnections.get(iter1);
            JobSpecification subJob = jobsList.get(iter1);
            operatorIdMapping.clear();
            Map<OperatorDescriptorId, IOperatorDescriptor> operatorsMap = subJob.getOperatorMap();
            String datasetName = feedConnections.get(iter1).getDatasetName();
            FeedConnectionId feedConnectionId = new FeedConnectionId(ingestionOp.getEntityId(), datasetName);

            FeedPolicyEntity feedPolicyEntity =
                    FeedMetadataUtil.validateIfPolicyExists(curFeedConnection.getDataverseName(),
                            curFeedConnection.getPolicyName(), metadataProvider.getMetadataTxnContext());

            for (Map.Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operatorsMap.entrySet()) {
                IOperatorDescriptor opDesc = entry.getValue();
                OperatorDescriptorId oldId = opDesc.getOperatorId();
                OperatorDescriptorId opId = null;
                if (opDesc instanceof LSMTreeInsertDeleteOperatorDescriptor
                        && ((LSMTreeInsertDeleteOperatorDescriptor) opDesc).isPrimary()) {
                    metaOp = new FeedMetaOperatorDescriptor(jobSpec, feedConnectionId, opDesc,
                            feedPolicyEntity.getProperties(), FeedRuntimeType.STORE);
                    opId = metaOp.getOperatorId();
                    opDesc.setOperatorId(opId);
                } else {
                    if (opDesc instanceof AlgebricksMetaOperatorDescriptor) {
                        AlgebricksMetaOperatorDescriptor algOp = (AlgebricksMetaOperatorDescriptor) opDesc;
                        IPushRuntimeFactory[] runtimeFactories = algOp.getPipeline().getRuntimeFactories();
                        // Tweak AssignOp to work with messages
                        if (runtimeFactories[0] instanceof AssignRuntimeFactory && runtimeFactories.length > 1) {
                            IConnectorDescriptor connectorDesc =
                                    subJob.getOperatorInputMap().get(opDesc.getOperatorId()).get(0);
                            // anything on the network interface needs to be message compatible
                            if (connectorDesc instanceof MToNPartitioningConnectorDescriptor) {
                                metaOp = new FeedMetaOperatorDescriptor(jobSpec, feedConnectionId, opDesc,
                                        feedPolicyEntity.getProperties(), FeedRuntimeType.COMPUTE);
                                opId = metaOp.getOperatorId();
                                opDesc.setOperatorId(opId);
                            }
                        }
                    }
                    if (opId == null) {
                        opId = jobSpec.createOperatorDescriptorId(opDesc);
                    }
                }
                operatorIdMapping.put(oldId, opId);
            }

            // copy connectors
            connectorIdMapping.clear();
            subJob.getConnectorMap().forEach((key, connDesc) -> {
                ConnectorDescriptorId newConnId;
                if (connDesc instanceof MToNPartitioningConnectorDescriptor) {
                    MToNPartitioningConnectorDescriptor m2nConn = (MToNPartitioningConnectorDescriptor) connDesc;
                    connDesc = new MToNPartitioningWithMessageConnectorDescriptor(jobSpec,
                            m2nConn.getTuplePartitionComputerFactory());
                    newConnId = connDesc.getConnectorId();
                } else {
                    newConnId = jobSpec.createConnectorDescriptor(connDesc);
                }
                connectorIdMapping.put(key, newConnId);
            });

            // make connections between operators
            for (Entry<ConnectorDescriptorId, Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>>> entry : subJob
                    .getConnectorOperatorMap().entrySet()) {
                ConnectorDescriptorId newId = connectorIdMapping.get(entry.getKey());
                IConnectorDescriptor connDesc = jobSpec.getConnectorMap().get(newId);
                Pair<IOperatorDescriptor, Integer> leftOp = entry.getValue().getLeft();
                Pair<IOperatorDescriptor, Integer> rightOp = entry.getValue().getRight();
                IOperatorDescriptor leftOpDesc = jobSpec.getOperatorMap().get(leftOp.getLeft().getOperatorId());
                IOperatorDescriptor rightOpDesc = jobSpec.getOperatorMap().get(rightOp.getLeft().getOperatorId());
                if (leftOp.getLeft() instanceof FeedCollectOperatorDescriptor) {
                    jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), replicateOp, iter1, leftOpDesc,
                            leftOp.getRight());
                }
                jobSpec.connect(connDesc, leftOpDesc, leftOp.getRight(), rightOpDesc, rightOp.getRight());
            }

            // prepare for setting partition constraints
            operatorLocations.clear();
            operatorCounts.clear();

            for (Constraint constraint : subJob.getUserConstraints()) {
                LValueConstraintExpression lexpr = constraint.getLValue();
                ConstraintExpression cexpr = constraint.getRValue();
                OperatorDescriptorId opId;
                switch (lexpr.getTag()) {
                    case PARTITION_COUNT:
                        opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                        operatorCounts.put(operatorIdMapping.get(opId), (int) ((ConstantExpression) cexpr).getValue());
                        break;
                    case PARTITION_LOCATION:
                        opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                        IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(operatorIdMapping.get(opId));
                        List<LocationConstraint> locations = operatorLocations.get(opDesc.getOperatorId());
                        if (locations == null) {
                            locations = new ArrayList<>();
                            operatorLocations.put(opDesc.getOperatorId(), locations);
                        }
                        String location = (String) ((ConstantExpression) cexpr).getValue();
                        LocationConstraint lc =
                                new LocationConstraint(location, ((PartitionLocationExpression) lexpr).getPartition());
                        locations.add(lc);
                        break;
                    default:
                        break;
                }
            }

            // set absolute location constraints
            for (Entry<OperatorDescriptorId, List<LocationConstraint>> entry : operatorLocations.entrySet()) {
                IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(entry.getKey());
                // why do we need to sort?
                Collections.sort(entry.getValue(), (LocationConstraint o1, LocationConstraint o2) -> {
                    return o1.partition - o2.partition;
                });
                String[] locations = new String[entry.getValue().size()];
                for (int j = 0; j < locations.length; ++j) {
                    locations[j] = entry.getValue().get(j).location;
                }
                PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, opDesc, locations);
            }

            // set count constraints
            operatorCounts.forEach((key, value) -> {
                IOperatorDescriptor opDesc = jobSpec.getOperatorMap().get(key);
                if (!operatorLocations.keySet().contains(key)) {
                    PartitionConstraintHelper.addPartitionCountConstraint(jobSpec, opDesc, value);
                }
            });
            // roots
            for (OperatorDescriptorId root : subJob.getRoots()) {
                jobSpec.addRoot(jobSpec.getOperatorMap().get(operatorIdMapping.get(root)));
            }
            int datasetId = metadataProvider
                    .findDataset(curFeedConnection.getDataverseName(), curFeedConnection.getDatasetName())
                    .getDatasetId();
            TxnId txnId = ((JobEventListenerFactory) subJob.getJobletEventListenerFactory()).getTxnId(datasetId);
            txnIdMap.put(datasetId, txnId);
        }

        // jobEventListenerFactory
        jobSpec.setJobletEventListenerFactory(new MultiTransactionJobletEventListenerFactory(txnIdMap, true));
        // useConnectorSchedulingPolicy
        jobSpec.setUseConnectorPolicyForScheduling(jobsList.get(0).isUseConnectorPolicyForScheduling());
        // connectorAssignmentPolicy
        jobSpec.setConnectorPolicyAssignmentPolicy(jobsList.get(0).getConnectorPolicyAssignmentPolicy());
        return jobSpec;
    }

    private static IStatementExecutor getSQLPPTranslator(MetadataProvider metadataProvider,
            SessionOutput sessionOutput) {
        List<Statement> stmts = new ArrayList<>();
        DefaultStatementExecutorFactory qtFactory = new DefaultStatementExecutorFactory();
        IStatementExecutor translator = qtFactory.create(metadataProvider.getApplicationContext(), stmts, sessionOutput,
                new SqlppCompilationProvider(), new StorageComponentProvider(), new ResponsePrinter(sessionOutput));
        return translator;
    }

    public static Pair<JobSpecification, AlgebricksAbsolutePartitionConstraint> buildStartFeedJob(
            MetadataProvider metadataProvider, Feed feed, List<FeedConnection> feedConnections,
            IStatementExecutor statementExecutor, IHyracksClientConnection hcc) throws Exception {
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(new HashMap<>());
        Pair<JobSpecification, IAdapterFactory> intakeInfo = buildFeedIntakeJobSpec(feed, metadataProvider, fpa);
        List<JobSpecification> jobsList = new ArrayList<>();
        // TODO: Figure out a better way to handle insert/upsert per conn instead of per feed
        Boolean insertFeed = ExternalDataUtils.isInsertFeed(feed.getConfiguration());
        // Construct the ingestion Job
        JobSpecification intakeJob = intakeInfo.getLeft();
        IAdapterFactory ingestionAdaptorFactory = intakeInfo.getRight();
        String[] ingestionLocations = ingestionAdaptorFactory.getPartitionConstraint().getLocations();
        // Add metadata configs
        metadataProvider.getConfig().put(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS, Boolean.TRUE.toString());
        metadataProvider.getConfig().put(FeedActivityDetails.COLLECT_LOCATIONS,
                StringUtils.join(ingestionLocations, ','));
        // TODO: Once we deprecated AQL, this extra queryTranslator can be removed.
        IStatementExecutor translator =
                getSQLPPTranslator(metadataProvider, ((QueryTranslator) statementExecutor).getSessionOutput());
        // Add connection job
        for (FeedConnection feedConnection : feedConnections) {
            JobSpecification connectionJob =
                    getConnectionJob(metadataProvider, feedConnection, translator, hcc, insertFeed);
            jobsList.add(connectionJob);
        }
        return Pair.of(combineIntakeCollectJobs(metadataProvider, feed, intakeJob, jobsList, feedConnections,
                ingestionLocations), intakeInfo.getRight().getPartitionConstraint());
    }
}
