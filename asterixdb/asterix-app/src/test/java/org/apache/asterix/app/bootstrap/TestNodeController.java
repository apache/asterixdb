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
package org.apache.asterix.app.bootstrap;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.app.nc.TransactionSubsystem;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.FormatUtils;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorNodePushable;
import org.apache.asterix.runtime.operators.LSMPrimaryInsertOperatorNodePushable;
import org.apache.asterix.runtime.operators.LSMPrimaryUpsertOperatorNodePushable;
import org.apache.asterix.runtime.utils.CcApplicationContext;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.transaction.management.runtime.CommitRuntime;
import org.apache.asterix.transaction.management.service.logging.LogReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;

public class TestNodeController {
    protected static final Logger LOGGER = LogManager.getLogger();

    protected static final String PATH_ACTUAL = "unittest" + File.separator;
    protected static final String PATH_BASE = FileUtil.joinPath("src", "test", "resources", "nodetests");

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    protected static TransactionProperties txnProperties;
    private static final boolean CLEANUP_ON_START = true;
    private static final boolean CLEANUP_ON_STOP = true;

    // Constants
    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;
    public static final int KB32 = 32768;
    public static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    public static final TransactionSubsystemProvider TXN_SUBSYSTEM_PROVIDER = TransactionSubsystemProvider.INSTANCE;
    // Mutables
    private long jobCounter = 100L;
    private final String testConfigFileName;
    private final boolean runHDFS;
    private final List<Pair<IOption, Object>> options = new ArrayList<>();

    public TestNodeController(String testConfigFileName, boolean runHDFS) {
        this.testConfigFileName = testConfigFileName;
        this.runHDFS = runHDFS;
    }

    public void init() throws Exception {
        init(CLEANUP_ON_START);
    }

    public void init(boolean cleanupOnStart) throws Exception {
        try {
            File outdir = new File(PATH_ACTUAL);
            outdir.mkdirs();
            ExecutionTestUtil.setUp(cleanupOnStart,
                    testConfigFileName == null ? TEST_CONFIG_FILE_NAME : testConfigFileName,
                    ExecutionTestUtil.integrationUtil, runHDFS, options);
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
    }

    public void deInit() throws Exception {
        deInit(CLEANUP_ON_STOP);
    }

    public void deInit(boolean cleanupOnStop) throws Exception {
        ExecutionTestUtil.tearDown(cleanupOnStop, runHDFS);
    }

    public void setOpts(List<Pair<IOption, Object>> opts) {
        options.addAll(opts);
    }

    public void clearOpts() {
        options.clear();
        ExecutionTestUtil.integrationUtil.clearOptions();
    }

    public TxnId getTxnJobId(IHyracksTaskContext ctx) {
        return getTxnJobId(ctx.getJobletContext().getJobId());
    }

    public TxnId getTxnJobId(JobId jobId) {
        return new TxnId(jobId.getId());
    }

    public Pair<SecondaryIndexInfo, LSMIndexBulkLoadOperatorNodePushable> getBulkLoadSecondaryOperator(
            IHyracksTaskContext ctx, Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType,
            ARecordType metaType, int[] filterFields, int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
            StorageComponentProvider storageComponentProvider, Index secondaryIndex, int numElementsHint)
            throws HyracksDataException, RemoteException, ACIDException, AlgebricksException {
        try {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            org.apache.hyracks.algebricks.common.utils.Pair<ILSMMergePolicyFactory, Map<String, String>> mergePolicy =
                    DatasetUtil.getMergePolicyFactory(dataset, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                    mergePolicy.first, mergePolicy.second, filterFields, primaryKeyIndexes, primaryKeyIndicators);
            SecondaryIndexInfo secondaryIndexInfo = new SecondaryIndexInfo(primaryIndexInfo, secondaryIndex);
            IIndexDataflowHelperFactory secondaryIndexHelperFactory = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), secondaryIndexInfo.fileSplitProvider);
            IIndexDataflowHelperFactory primaryIndexHelperFactory = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
            int[] fieldPermutation = new int[secondaryIndex.getKeyFieldNames().size()];
            for (int i = 0; i < fieldPermutation.length; i++) {
                fieldPermutation[i] = i;
            }
            LSMIndexBulkLoadOperatorNodePushable op =
                    new LSMIndexBulkLoadOperatorNodePushable(secondaryIndexHelperFactory, primaryIndexHelperFactory,
                            ctx, 0, fieldPermutation, 1.0F, false, numElementsHint, true, secondaryIndexInfo.rDesc,
                            BulkLoadUsage.CREATE_INDEX, dataset.getDatasetId(), null);
            op.setOutputFrameWriter(0, new SinkRuntimeFactory().createPushRuntime(ctx)[0], null);
            return Pair.of(secondaryIndexInfo, op);
        } catch (Throwable th) {
            throw HyracksDataException.create(th);
        }
    }

    public Pair<LSMPrimaryInsertOperatorNodePushable, IPushRuntime> getInsertPipeline(IHyracksTaskContext ctx,
            Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType, int[] filterFields,
            int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
            StorageComponentProvider storageComponentProvider, Index secondaryIndex, Index primaryKeyIndex)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        CcApplicationContext appCtx =
                (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
        MetadataProvider mdProvider = new MetadataProvider(appCtx, null);
        try {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            org.apache.hyracks.algebricks.common.utils.Pair<ILSMMergePolicyFactory, Map<String, String>> mergePolicy =
                    DatasetUtil.getMergePolicyFactory(dataset, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                    mergePolicy.first, mergePolicy.second, filterFields, primaryKeyIndexes, primaryKeyIndicators);
            IModificationOperationCallbackFactory modOpCallbackFactory =
                    dataset.getModificationCallbackFactory(storageComponentProvider, primaryIndexInfo.index,
                            IndexOperation.INSERT, primaryIndexInfo.primaryKeyIndexes);
            ISearchOperationCallbackFactory searchOpCallbackFactory =
                    dataset.getSearchCallbackFactory(storageComponentProvider, primaryIndexInfo.index,
                            IndexOperation.INSERT, primaryIndexInfo.primaryKeyIndexes);
            IRecordDescriptorProvider recordDescProvider = primaryIndexInfo.getInsertRecordDescriptorProvider();
            RecordDescriptor recordDesc =
                    recordDescProvider.getInputRecordDescriptor(new ActivityId(new OperatorDescriptorId(0), 0), 0);
            IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
            IIndexDataflowHelperFactory pkIndexHelperFactory = null;
            if (primaryKeyIndex != null) {
                SecondaryIndexInfo pkIndexInfo = new SecondaryIndexInfo(primaryIndexInfo, primaryKeyIndex);
                pkIndexHelperFactory = new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(),
                        pkIndexInfo.fileSplitProvider);
            }

            LSMPrimaryInsertOperatorNodePushable insertOp = new LSMPrimaryInsertOperatorNodePushable(ctx,
                    ctx.getTaskAttemptId().getTaskId().getPartition(), indexHelperFactory, pkIndexHelperFactory,
                    primaryIndexInfo.primaryIndexInsertFieldsPermutations, recordDesc, modOpCallbackFactory,
                    searchOpCallbackFactory, primaryKeyIndexes.length, filterFields, null);
            // For now, this assumes a single secondary index. recordDesc is always <pk-record-meta>
            // for the index, we will have to create an assign operator that extract the sk
            // then the secondary LSMInsertDeleteOperatorNodePushable
            if (secondaryIndex != null) {
                List<List<String>> skNames = secondaryIndex.getKeyFieldNames();
                List<Integer> indicators = secondaryIndex.getKeyFieldSourceIndicators();
                IScalarEvaluatorFactory[] secondaryFieldAccessEvalFactories =
                        new IScalarEvaluatorFactory[skNames.size()];
                for (int i = 0; i < skNames.size(); i++) {
                    ARecordType sourceType = dataset.hasMetaPart()
                            ? indicators.get(i).intValue() == Index.RECORD_INDICATOR ? recordType : metaType
                            : recordType;
                    int pos = skNames.get(i).size() > 1 ? -1 : sourceType.getFieldIndex(skNames.get(i).get(0));
                    secondaryFieldAccessEvalFactories[i] =
                            mdProvider.getDataFormat().getFieldAccessEvaluatorFactory(mdProvider.getFunctionManager(),
                                    sourceType, secondaryIndex.getKeyFieldNames().get(i), pos, null);
                }
                // outColumns are computed inside the assign runtime
                int[] outColumns = new int[skNames.size()];
                // projection list include old and new (primary and secondary keys)
                int[] projectionList = new int[skNames.size() + primaryIndexInfo.index.getKeyFieldNames().size()];
                for (int i = 0; i < secondaryFieldAccessEvalFactories.length; i++) {
                    outColumns[i] = primaryIndexInfo.rDesc.getFieldCount() + i;
                }
                int projCount = 0;
                for (int i = 0; i < secondaryFieldAccessEvalFactories.length; i++) {
                    projectionList[projCount++] = primaryIndexInfo.rDesc.getFieldCount() + i;
                }
                for (int i = 0; i < primaryIndexInfo.index.getKeyFieldNames().size(); i++) {
                    projectionList[projCount++] = i;
                }
                IPushRuntime assignOp =
                        new AssignRuntimeFactory(outColumns, secondaryFieldAccessEvalFactories, projectionList, true)
                                .createPushRuntime(ctx)[0];
                insertOp.setOutputFrameWriter(0, assignOp, primaryIndexInfo.rDesc);
                assignOp.setInputRecordDescriptor(0, primaryIndexInfo.rDesc);
                SecondaryIndexInfo secondaryIndexInfo = new SecondaryIndexInfo(primaryIndexInfo, secondaryIndex);
                IIndexDataflowHelperFactory secondaryIndexHelperFactory = new IndexDataflowHelperFactory(
                        storageComponentProvider.getStorageManager(), secondaryIndexInfo.fileSplitProvider);

                IModificationOperationCallbackFactory secondaryModCallbackFactory =
                        dataset.getModificationCallbackFactory(storageComponentProvider, secondaryIndex,
                                IndexOperation.INSERT, primaryKeyIndexes);

                LSMInsertDeleteOperatorNodePushable secondaryInsertOp = new LSMInsertDeleteOperatorNodePushable(ctx,
                        ctx.getTaskAttemptId().getTaskId().getPartition(), secondaryIndexInfo.insertFieldsPermutations,
                        secondaryIndexInfo.rDesc, IndexOperation.INSERT, false, secondaryIndexHelperFactory,
                        secondaryModCallbackFactory, null, null);
                assignOp.setOutputFrameWriter(0, secondaryInsertOp, secondaryIndexInfo.rDesc);

                IPushRuntime commitOp =
                        dataset.getCommitRuntimeFactory(mdProvider, secondaryIndexInfo.primaryKeyIndexes, true)
                                .createPushRuntime(ctx)[0];

                secondaryInsertOp.setOutputFrameWriter(0, commitOp, secondaryIndexInfo.rDesc);
                commitOp.setInputRecordDescriptor(0, secondaryIndexInfo.rDesc);
                return Pair.of(insertOp, commitOp);
            } else {
                IPushRuntime commitOp =
                        dataset.getCommitRuntimeFactory(mdProvider, primaryIndexInfo.primaryKeyIndexes, true)
                                .createPushRuntime(ctx)[0];
                insertOp.setOutputFrameWriter(0, commitOp, primaryIndexInfo.rDesc);
                commitOp.setInputRecordDescriptor(0, primaryIndexInfo.rDesc);
                return Pair.of(insertOp, commitOp);
            }
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    public Pair<LSMInsertDeleteOperatorNodePushable, IPushRuntime> getDeletePipeline(IHyracksTaskContext ctx,
            Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType, int[] filterFields,
            int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
            StorageComponentProvider storageComponentProvider, Index secondaryIndex)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        CcApplicationContext appCtx =
                (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
        MetadataProvider mdProvider = new MetadataProvider(appCtx, null);
        try {
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            org.apache.hyracks.algebricks.common.utils.Pair<ILSMMergePolicyFactory, Map<String, String>> mergePolicy =
                    DatasetUtil.getMergePolicyFactory(dataset, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                    mergePolicy.first, mergePolicy.second, filterFields, primaryKeyIndexes, primaryKeyIndicators);
            IModificationOperationCallbackFactory modOpCallbackFactory =
                    dataset.getModificationCallbackFactory(storageComponentProvider, primaryIndexInfo.index,
                            IndexOperation.DELETE, primaryIndexInfo.primaryKeyIndexes);
            IRecordDescriptorProvider recordDescProvider = primaryIndexInfo.getInsertRecordDescriptorProvider();
            RecordDescriptor recordDesc =
                    recordDescProvider.getInputRecordDescriptor(new ActivityId(new OperatorDescriptorId(0), 0), 0);
            IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                    storageComponentProvider.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
            LSMInsertDeleteOperatorNodePushable deleteOp =
                    new LSMInsertDeleteOperatorNodePushable(ctx, ctx.getTaskAttemptId().getTaskId().getPartition(),
                            primaryIndexInfo.primaryIndexInsertFieldsPermutations, recordDesc, IndexOperation.DELETE,
                            true, indexHelperFactory, modOpCallbackFactory, null, null);
            // For now, this assumes a single secondary index. recordDesc is always <pk-record-meta>
            // for the index, we will have to create an assign operator that extract the sk
            // then the secondary LSMInsertDeleteOperatorNodePushable
            if (secondaryIndex != null) {
                List<List<String>> skNames = secondaryIndex.getKeyFieldNames();
                List<Integer> indicators = secondaryIndex.getKeyFieldSourceIndicators();
                IScalarEvaluatorFactory[] secondaryFieldAccessEvalFactories =
                        new IScalarEvaluatorFactory[skNames.size()];
                for (int i = 0; i < skNames.size(); i++) {
                    ARecordType sourceType = dataset.hasMetaPart()
                            ? indicators.get(i).intValue() == Index.RECORD_INDICATOR ? recordType : metaType
                            : recordType;
                    int pos = skNames.get(i).size() > 1 ? -1 : sourceType.getFieldIndex(skNames.get(i).get(0));
                    secondaryFieldAccessEvalFactories[i] =
                            mdProvider.getDataFormat().getFieldAccessEvaluatorFactory(mdProvider.getFunctionManager(),
                                    sourceType, secondaryIndex.getKeyFieldNames().get(i), pos, null);
                }
                // outColumns are computed inside the assign runtime
                int[] outColumns = new int[skNames.size()];
                // projection list include old and new (primary and secondary keys)
                int[] projectionList = new int[skNames.size() + primaryIndexInfo.index.getKeyFieldNames().size()];
                for (int i = 0; i < secondaryFieldAccessEvalFactories.length; i++) {
                    outColumns[i] = primaryIndexInfo.rDesc.getFieldCount() + i;
                }
                int projCount = 0;
                for (int i = 0; i < secondaryFieldAccessEvalFactories.length; i++) {
                    projectionList[projCount++] = primaryIndexInfo.rDesc.getFieldCount() + i;
                }
                for (int i = 0; i < primaryIndexInfo.index.getKeyFieldNames().size(); i++) {
                    projectionList[projCount++] = i;
                }
                IPushRuntime assignOp =
                        new AssignRuntimeFactory(outColumns, secondaryFieldAccessEvalFactories, projectionList, true)
                                .createPushRuntime(ctx)[0];
                deleteOp.setOutputFrameWriter(0, assignOp, primaryIndexInfo.rDesc);
                assignOp.setInputRecordDescriptor(0, primaryIndexInfo.rDesc);
                SecondaryIndexInfo secondaryIndexInfo = new SecondaryIndexInfo(primaryIndexInfo, secondaryIndex);
                IIndexDataflowHelperFactory secondaryIndexHelperFactory = new IndexDataflowHelperFactory(
                        storageComponentProvider.getStorageManager(), secondaryIndexInfo.fileSplitProvider);

                IModificationOperationCallbackFactory secondaryModCallbackFactory =
                        dataset.getModificationCallbackFactory(storageComponentProvider, secondaryIndex,
                                IndexOperation.INSERT, primaryKeyIndexes);

                LSMInsertDeleteOperatorNodePushable secondaryInsertOp = new LSMInsertDeleteOperatorNodePushable(ctx,
                        ctx.getTaskAttemptId().getTaskId().getPartition(), secondaryIndexInfo.insertFieldsPermutations,
                        secondaryIndexInfo.rDesc, IndexOperation.DELETE, false, secondaryIndexHelperFactory,
                        secondaryModCallbackFactory, null, null);
                assignOp.setOutputFrameWriter(0, secondaryInsertOp, secondaryIndexInfo.rDesc);

                IPushRuntime commitOp =
                        dataset.getCommitRuntimeFactory(mdProvider, secondaryIndexInfo.primaryKeyIndexes, true)
                                .createPushRuntime(ctx)[0];

                secondaryInsertOp.setOutputFrameWriter(0, commitOp, secondaryIndexInfo.rDesc);
                commitOp.setInputRecordDescriptor(0, secondaryIndexInfo.rDesc);
                return Pair.of(deleteOp, commitOp);
            } else {
                IPushRuntime commitOp =
                        dataset.getCommitRuntimeFactory(mdProvider, primaryIndexInfo.primaryKeyIndexes, true)
                                .createPushRuntime(ctx)[0];
                deleteOp.setOutputFrameWriter(0, commitOp, primaryIndexInfo.rDesc);
                commitOp.setInputRecordDescriptor(0, primaryIndexInfo.rDesc);
                return Pair.of(deleteOp, commitOp);
            }
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    public IPushRuntime getFullScanPipeline(IFrameWriter countOp, IHyracksTaskContext ctx, Dataset dataset,
            IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
            NoMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties, int[] filterFields,
            int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
            StorageComponentProvider storageComponentProvider) throws HyracksDataException, AlgebricksException {
        IPushRuntime emptyTupleOp = new EmptyTupleSourceRuntimeFactory().createPushRuntime(ctx)[0];
        JobSpecification spec = new JobSpecification();
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields, primaryKeyIndexes, primaryKeyIndicators);
        IIndexDataflowHelperFactory indexDataflowHelperFactory = new IndexDataflowHelperFactory(
                storageComponentProvider.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        BTreeSearchOperatorDescriptor searchOpDesc = new BTreeSearchOperatorDescriptor(spec, primaryIndexInfo.rDesc,
                null, null, true, true, indexDataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, filterFields, filterFields, false);
        BTreeSearchOperatorNodePushable searchOp =
                searchOpDesc.createPushRuntime(ctx, primaryIndexInfo.getSearchRecordDescriptorProvider(),
                        ctx.getTaskAttemptId().getTaskId().getPartition(), 1);
        emptyTupleOp.setOutputFrameWriter(0, searchOp,
                primaryIndexInfo.getSearchRecordDescriptorProvider().getInputRecordDescriptor(null, 0));
        searchOp.setOutputFrameWriter(0, countOp, primaryIndexInfo.rDesc);
        return emptyTupleOp;
    }

    public LogReader getTransactionLogReader(boolean isRecoveryMode) {
        return (LogReader) getTransactionSubsystem().getLogManager().getLogReader(isRecoveryMode);
    }

    public JobId newJobId() {
        return new JobId(jobCounter++);
    }

    public IResourceFactory getPrimaryResourceFactory(IHyracksTaskContext ctx, PrimaryIndexInfo primaryIndexInfo,
            IStorageComponentProvider storageComponentProvider, Dataset dataset) throws AlgebricksException {
        Dataverse dataverse = new Dataverse(dataset.getDataverseName(), NonTaggedDataFormat.class.getName(),
                MetadataUtil.PENDING_NO_OP);
        Index index = primaryIndexInfo.getIndex();
        CcApplicationContext appCtx =
                (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
        MetadataProvider mdProvider = new MetadataProvider(appCtx, dataverse);
        try {
            return dataset.getResourceFactory(mdProvider, index, primaryIndexInfo.recordType, primaryIndexInfo.metaType,
                    primaryIndexInfo.mergePolicyFactory, primaryIndexInfo.mergePolicyProperties);
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    public PrimaryIndexInfo createPrimaryIndex(Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType,
            ARecordType metaType, int[] filterFields, IStorageComponentProvider storageComponentProvider,
            int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators, int partition)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        org.apache.hyracks.algebricks.common.utils.Pair<ILSMMergePolicyFactory, Map<String, String>> mergePolicy =
                DatasetUtil.getMergePolicyFactory(dataset, mdTxnCtx);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicy.first, mergePolicy.second, filterFields, primaryKeyIndexes, primaryKeyIndicators);
        Dataverse dataverse = new Dataverse(dataset.getDataverseName(), NonTaggedDataFormat.class.getName(),
                MetadataUtil.PENDING_NO_OP);
        MetadataProvider mdProvider = new MetadataProvider(
                (ICcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext(), dataverse);
        try {
            IResourceFactory resourceFactory = dataset.getResourceFactory(mdProvider, primaryIndexInfo.index,
                    recordType, metaType, mergePolicy.first, mergePolicy.second);
            IndexBuilderFactory indexBuilderFactory =
                    new IndexBuilderFactory(storageComponentProvider.getStorageManager(),
                            primaryIndexInfo.getFileSplitProvider(), resourceFactory, true);
            IHyracksTaskContext ctx = createTestContext(newJobId(), partition, false);
            IIndexBuilder indexBuilder = indexBuilderFactory.create(ctx, partition);
            indexBuilder.build();
        } finally {
            mdProvider.getLocks().unlock();
        }
        return primaryIndexInfo;
    }

    public SecondaryIndexInfo createSecondaryIndex(PrimaryIndexInfo primaryIndexInfo, Index secondaryIndex,
            IStorageComponentProvider storageComponentProvider, int partition)
            throws AlgebricksException, HyracksDataException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        org.apache.hyracks.algebricks.common.utils.Pair<ILSMMergePolicyFactory, Map<String, String>> mergePolicy =
                DatasetUtil.getMergePolicyFactory(primaryIndexInfo.dataset, mdTxnCtx);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        Dataverse dataverse = new Dataverse(primaryIndexInfo.dataset.getDataverseName(),
                NonTaggedDataFormat.class.getName(), MetadataUtil.PENDING_NO_OP);
        MetadataProvider mdProvider = new MetadataProvider(
                (ICcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext(), dataverse);
        SecondaryIndexInfo secondaryIndexInfo = new SecondaryIndexInfo(primaryIndexInfo, secondaryIndex);
        try {
            IResourceFactory resourceFactory = primaryIndexInfo.dataset.getResourceFactory(mdProvider, secondaryIndex,
                    primaryIndexInfo.recordType, primaryIndexInfo.metaType, mergePolicy.first, mergePolicy.second);
            IndexBuilderFactory indexBuilderFactory =
                    new IndexBuilderFactory(storageComponentProvider.getStorageManager(),
                            secondaryIndexInfo.fileSplitProvider, resourceFactory, true);
            IHyracksTaskContext ctx = createTestContext(newJobId(), partition, false);
            IIndexBuilder indexBuilder = indexBuilderFactory.create(ctx, partition);
            indexBuilder.build();
        } finally {
            mdProvider.getLocks().unlock();
        }
        return secondaryIndexInfo;
    }

    public static ISerializerDeserializer<?>[] createPrimaryIndexSerdes(int primaryIndexNumOfTupleFields,
            IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType) {
        int i = 0;
        ISerializerDeserializer<?>[] primaryIndexSerdes = new ISerializerDeserializer<?>[primaryIndexNumOfTupleFields];
        for (; i < primaryKeyTypes.length; i++) {
            primaryIndexSerdes[i] =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(primaryKeyTypes[i]);
        }
        primaryIndexSerdes[i++] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(recordType);
        if (metaType != null) {
            primaryIndexSerdes[i] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
        }
        return primaryIndexSerdes;
    }

    public static ISerializerDeserializer<?>[] createSecondaryIndexSerdes(ARecordType recordType, ARecordType metaType,
            IAType[] primaryKeyTypes, IAType[] secondaryKeyTypes) {
        ISerializerDeserializer<?>[] secondaryIndexSerdes =
                new ISerializerDeserializer<?>[secondaryKeyTypes.length + primaryKeyTypes.length];
        int i = 0;
        for (; i < secondaryKeyTypes.length; i++) {
            secondaryIndexSerdes[i] =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(secondaryKeyTypes[i]);
        }
        for (; i < secondaryKeyTypes.length + primaryKeyTypes.length; i++) {
            secondaryIndexSerdes[i] = SerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(primaryKeyTypes[i - secondaryKeyTypes.length]);
        }
        return secondaryIndexSerdes;
    }

    public static ITypeTraits[] createPrimaryIndexTypeTraits(int primaryIndexNumOfTupleFields, IAType[] primaryKeyTypes,
            ARecordType recordType, ARecordType metaType) {
        ITypeTraits[] primaryIndexTypeTraits = new ITypeTraits[primaryIndexNumOfTupleFields];
        int i = 0;
        for (; i < primaryKeyTypes.length; i++) {
            primaryIndexTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(primaryKeyTypes[i]);
        }
        primaryIndexTypeTraits[i++] = TypeTraitProvider.INSTANCE.getTypeTrait(recordType);
        if (metaType != null) {
            primaryIndexTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(metaType);
        }
        return primaryIndexTypeTraits;
    }

    public static ITypeTraits[] createSecondaryIndexTypeTraits(ARecordType recordType, ARecordType metaType,
            IAType[] primaryKeyTypes, IAType[] secondaryKeyTypes) {
        ITypeTraits[] secondaryIndexTypeTraits = new ITypeTraits[secondaryKeyTypes.length + primaryKeyTypes.length];
        int i = 0;
        for (; i < secondaryKeyTypes.length; i++) {
            secondaryIndexTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(secondaryKeyTypes[i]);
        }
        for (; i < secondaryKeyTypes.length + primaryKeyTypes.length; i++) {
            secondaryIndexTypeTraits[i] =
                    TypeTraitProvider.INSTANCE.getTypeTrait(primaryKeyTypes[i - secondaryKeyTypes.length]);
        }
        return secondaryIndexTypeTraits;
    }

    public IHyracksTaskContext createTestContext(JobId jobId, int partition, boolean withMessaging)
            throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(KB32, ExecutionTestUtil.integrationUtil.ncs[0].getIoManager());
        if (withMessaging) {
            TaskUtil.put(HyracksConstants.KEY_MESSAGE, new VSizeFrame(ctx), ctx);
        }
        IHyracksJobletContext jobletCtx = Mockito.mock(IHyracksJobletContext.class);
        JobEventListenerFactory factory = new JobEventListenerFactory(new TxnId(jobId.getId()), true);
        Mockito.when(jobletCtx.getJobletEventListenerFactory()).thenReturn(factory);
        Mockito.when(jobletCtx.getServiceContext()).thenReturn(ExecutionTestUtil.integrationUtil.ncs[0].getContext());
        Mockito.when(jobletCtx.getJobId()).thenReturn(jobId);
        ctx = Mockito.spy(ctx);
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletCtx);
        Mockito.when(ctx.getIoManager()).thenReturn(ExecutionTestUtil.integrationUtil.ncs[0].getIoManager());
        TaskAttemptId taskId =
                new TaskAttemptId(new TaskId(new ActivityId(new OperatorDescriptorId(0), 0), partition), 0);
        Mockito.when(ctx.getTaskAttemptId()).thenReturn(taskId);
        return ctx;
    }

    public TransactionSubsystem getTransactionSubsystem() {
        return (TransactionSubsystem) ((NCAppRuntimeContext) ExecutionTestUtil.integrationUtil.ncs[0]
                .getApplicationContext()).getTransactionSubsystem();
    }

    public ITransactionManager getTransactionManager() {
        return getTransactionSubsystem().getTransactionManager();
    }

    public NCAppRuntimeContext getAppRuntimeContext() {
        return (NCAppRuntimeContext) ExecutionTestUtil.integrationUtil.ncs[0].getApplicationContext();
    }

    public DatasetLifecycleManager getDatasetLifecycleManager() {
        return (DatasetLifecycleManager) getAppRuntimeContext().getDatasetLifecycleManager();
    }

    public static class SecondaryIndexInfo {
        final int[] primaryKeyIndexes;
        final PrimaryIndexInfo primaryIndexInfo;
        final Index secondaryIndex;
        final ConstantFileSplitProvider fileSplitProvider;
        final ISerializerDeserializer<?>[] secondaryIndexSerdes;
        final RecordDescriptor rDesc;
        final int[] insertFieldsPermutations;
        final ITypeTraits[] secondaryIndexTypeTraits;

        public SecondaryIndexInfo(PrimaryIndexInfo primaryIndexInfo, Index secondaryIndex) {
            this.primaryIndexInfo = primaryIndexInfo;
            this.secondaryIndex = secondaryIndex;
            List<String> nodes = Collections.singletonList(ExecutionTestUtil.integrationUtil.ncs[0].getId());
            CcApplicationContext appCtx =
                    (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
            FileSplit[] splits = SplitsAndConstraintsUtil.getIndexSplits(appCtx.getClusterStateManager(),
                    primaryIndexInfo.dataset, secondaryIndex.getIndexName(), nodes);
            fileSplitProvider = new ConstantFileSplitProvider(splits);
            secondaryIndexTypeTraits = createSecondaryIndexTypeTraits(primaryIndexInfo.recordType,
                    primaryIndexInfo.metaType, primaryIndexInfo.primaryKeyTypes,
                    secondaryIndex.getKeyFieldTypes().toArray(new IAType[secondaryIndex.getKeyFieldTypes().size()]));
            secondaryIndexSerdes = createSecondaryIndexSerdes(primaryIndexInfo.recordType, primaryIndexInfo.metaType,
                    primaryIndexInfo.primaryKeyTypes,
                    secondaryIndex.getKeyFieldTypes().toArray(new IAType[secondaryIndex.getKeyFieldTypes().size()]));
            rDesc = new RecordDescriptor(secondaryIndexSerdes, secondaryIndexTypeTraits);
            insertFieldsPermutations = new int[secondaryIndexTypeTraits.length];
            for (int i = 0; i < insertFieldsPermutations.length; i++) {
                insertFieldsPermutations[i] = i;
            }
            primaryKeyIndexes = new int[primaryIndexInfo.primaryKeyIndexes.length];
            for (int i = 0; i < primaryKeyIndexes.length; i++) {
                primaryKeyIndexes[i] = i + secondaryIndex.getKeyFieldNames().size();
            }
        }

        public IFileSplitProvider getFileSplitProvider() {
            return fileSplitProvider;
        }

        public ISerializerDeserializer<?>[] getSerdes() {
            return secondaryIndexSerdes;
        }
    }

    public static class PrimaryIndexInfo {
        private final Dataset dataset;
        private final IAType[] primaryKeyTypes;
        private final ARecordType recordType;
        private final ARecordType metaType;
        private final ILSMMergePolicyFactory mergePolicyFactory;
        private final Map<String, String> mergePolicyProperties;
        private final int primaryIndexNumOfTupleFields;
        private final ITypeTraits[] primaryIndexTypeTraits;
        private final ISerializerDeserializer<?>[] primaryIndexSerdes;
        private final ConstantFileSplitProvider fileSplitProvider;
        private final RecordDescriptor rDesc;
        private final int[] primaryIndexInsertFieldsPermutations;
        private final int[] primaryKeyIndexes;
        private final Index index;

        public PrimaryIndexInfo(Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
                ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
                int[] filterFields, int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators)
                throws AlgebricksException {
            this.dataset = dataset;
            this.primaryKeyTypes = primaryKeyTypes;
            this.recordType = recordType;
            this.metaType = metaType;
            this.mergePolicyFactory = mergePolicyFactory;
            this.mergePolicyProperties = mergePolicyProperties;
            this.primaryKeyIndexes = primaryKeyIndexes;
            primaryIndexNumOfTupleFields = primaryKeyTypes.length + (1 + ((metaType == null) ? 0 : 1));
            primaryIndexTypeTraits =
                    createPrimaryIndexTypeTraits(primaryIndexNumOfTupleFields, primaryKeyTypes, recordType, metaType);
            primaryIndexSerdes =
                    createPrimaryIndexSerdes(primaryIndexNumOfTupleFields, primaryKeyTypes, recordType, metaType);
            rDesc = new RecordDescriptor(primaryIndexSerdes, primaryIndexTypeTraits);
            primaryIndexInsertFieldsPermutations = new int[primaryIndexNumOfTupleFields];
            for (int i = 0; i < primaryIndexNumOfTupleFields; i++) {
                primaryIndexInsertFieldsPermutations[i] = i;
            }
            List<List<String>> keyFieldNames = new ArrayList<>();
            List<IAType> keyFieldTypes = Arrays.asList(primaryKeyTypes);
            for (int i = 0; i < primaryKeyIndicators.size(); i++) {
                Integer indicator = primaryKeyIndicators.get(i);
                String[] fieldNames =
                        indicator == Index.RECORD_INDICATOR ? recordType.getFieldNames() : metaType.getFieldNames();
                keyFieldNames.add(Arrays.asList(fieldNames[primaryKeyIndexes[i]]));
            }
            index = new Index(dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName(),
                    IndexType.BTREE, keyFieldNames, primaryKeyIndicators, keyFieldTypes, false, false, true,
                    MetadataUtil.PENDING_NO_OP);
            List<String> nodes = Collections.singletonList(ExecutionTestUtil.integrationUtil.ncs[0].getId());
            CcApplicationContext appCtx =
                    (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
            FileSplit[] splits = SplitsAndConstraintsUtil.getIndexSplits(appCtx.getClusterStateManager(), dataset,
                    index.getIndexName(), nodes);
            fileSplitProvider = new ConstantFileSplitProvider(splits);
        }

        public Index getIndex() {
            return index;
        }

        public Dataset getDataset() {
            return dataset;
        }

        public IRecordDescriptorProvider getInsertRecordDescriptorProvider() {
            IRecordDescriptorProvider rDescProvider = Mockito.mock(IRecordDescriptorProvider.class);
            Mockito.when(rDescProvider.getInputRecordDescriptor(Mockito.any(), Mockito.anyInt())).thenReturn(rDesc);
            return rDescProvider;
        }

        public IRecordDescriptorProvider getSearchRecordDescriptorProvider() {
            ITypeTraits[] primaryKeyTypeTraits = new ITypeTraits[primaryKeyTypes.length];
            ISerializerDeserializer<?>[] primaryKeySerdes = new ISerializerDeserializer<?>[primaryKeyTypes.length];
            for (int i = 0; i < primaryKeyTypes.length; i++) {
                primaryKeyTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(primaryKeyTypes[i]);
                primaryKeySerdes[i] =
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(primaryKeyTypes[i]);
            }
            RecordDescriptor searcgRecDesc = new RecordDescriptor(primaryKeySerdes, primaryKeyTypeTraits);
            IRecordDescriptorProvider rDescProvider = Mockito.mock(IRecordDescriptorProvider.class);
            Mockito.when(rDescProvider.getInputRecordDescriptor(Mockito.any(), Mockito.anyInt()))
                    .thenReturn(searcgRecDesc);
            return rDescProvider;
        }

        public ConstantFileSplitProvider getFileSplitProvider() {
            return fileSplitProvider;
        }
    }

    public RecordDescriptor getSearchOutputDesc(IAType[] keyTypes, ARecordType recordType, ARecordType metaType) {
        int primaryIndexNumOfTupleFields = keyTypes.length + (1 + ((metaType == null) ? 0 : 1));
        ITypeTraits[] primaryIndexTypeTraits =
                createPrimaryIndexTypeTraits(primaryIndexNumOfTupleFields, keyTypes, recordType, metaType);
        ISerializerDeserializer<?>[] primaryIndexSerdes =
                createPrimaryIndexSerdes(primaryIndexNumOfTupleFields, keyTypes, recordType, metaType);
        return new RecordDescriptor(primaryIndexSerdes, primaryIndexTypeTraits);
    }

    public IndexDataflowHelperFactory getPrimaryIndexDataflowHelperFactory(PrimaryIndexInfo primaryIndexInfo,
            IStorageComponentProvider storageComponentProvider) throws AlgebricksException {
        return new IndexDataflowHelperFactory(storageComponentProvider.getStorageManager(),
                primaryIndexInfo.getFileSplitProvider());
    }

    public IStorageManager getStorageManager() {
        CcApplicationContext appCtx =
                (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
        return appCtx.getStorageManager();
    }

    public Pair<LSMPrimaryUpsertOperatorNodePushable, CommitRuntime> getUpsertPipeline(IHyracksTaskContext ctx,
            Dataset dataset, IAType[] keyTypes, ARecordType recordType, ARecordType metaType, int[] filterFields,
            int[] keyIndexes, List<Integer> keyIndicators, StorageComponentProvider storageComponentProvider,
            IFrameOperationCallbackFactory frameOpCallbackFactory, boolean hasSecondaries) throws Exception {
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        MetadataProvider mdProvider = new MetadataProvider(
                (ICcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext(),
                MetadataBuiltinEntities.DEFAULT_DATAVERSE);
        org.apache.hyracks.algebricks.common.utils.Pair<ILSMMergePolicyFactory, Map<String, String>> mergePolicy =
                DatasetUtil.getMergePolicyFactory(dataset, mdTxnCtx);
        MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, keyTypes, recordType, metaType,
                mergePolicy.first, mergePolicy.second, filterFields, keyIndexes, keyIndicators);
        IModificationOperationCallbackFactory modificationCallbackFactory = dataset.getModificationCallbackFactory(
                storageComponentProvider, primaryIndexInfo.index, IndexOperation.UPSERT, keyIndexes);
        ISearchOperationCallbackFactory searchCallbackFactory = dataset.getSearchCallbackFactory(
                storageComponentProvider, primaryIndexInfo.index, IndexOperation.UPSERT, keyIndexes);
        IRecordDescriptorProvider recordDescProvider = primaryIndexInfo.getInsertRecordDescriptorProvider();
        IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                storageComponentProvider.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        LSMPrimaryUpsertOperatorNodePushable insertOp =
                new LSMPrimaryUpsertOperatorNodePushable(ctx, ctx.getTaskAttemptId().getTaskId().getPartition(),
                        indexHelperFactory, primaryIndexInfo.primaryIndexInsertFieldsPermutations,
                        recordDescProvider.getInputRecordDescriptor(new ActivityId(new OperatorDescriptorId(0), 0), 0),
                        modificationCallbackFactory, searchCallbackFactory,
                        keyIndexes.length, recordType, -1, frameOpCallbackFactory == null
                                ? dataset.getFrameOpCallbackFactory(mdProvider) : frameOpCallbackFactory,
                        MissingWriterFactory.INSTANCE, hasSecondaries);
        RecordDescriptor upsertOutRecDesc = getUpsertOutRecDesc(primaryIndexInfo.rDesc, dataset,
                filterFields == null ? 0 : filterFields.length, recordType, metaType);
        // fix pk fields
        int diff = upsertOutRecDesc.getFieldCount() - primaryIndexInfo.rDesc.getFieldCount();
        int[] pkFieldsInCommitOp = new int[dataset.getPrimaryKeys().size()];
        for (int i = 0; i < pkFieldsInCommitOp.length; i++) {
            pkFieldsInCommitOp[i] = diff + i;
        }
        CommitRuntime commitOp = new CommitRuntime(ctx, getTxnJobId(ctx), dataset.getDatasetId(), pkFieldsInCommitOp,
                true, ctx.getTaskAttemptId().getTaskId().getPartition(), true);
        insertOp.setOutputFrameWriter(0, commitOp, upsertOutRecDesc);
        commitOp.setInputRecordDescriptor(0, upsertOutRecDesc);
        return Pair.of(insertOp, commitOp);
    }

    private RecordDescriptor getUpsertOutRecDesc(RecordDescriptor inputRecordDesc, Dataset dataset, int numFilterFields,
            ARecordType itemType, ARecordType metaItemType) throws Exception {
        ITypeTraits[] outputTypeTraits =
                new ITypeTraits[inputRecordDesc.getFieldCount() + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];
        ISerializerDeserializer<?>[] outputSerDes = new ISerializerDeserializer[inputRecordDesc.getFieldCount()
                + (dataset.hasMetaPart() ? 2 : 1) + numFilterFields];

        // add the previous record first
        int f = 0;
        outputSerDes[f] = FormatUtils.getDefaultFormat().getSerdeProvider().getSerializerDeserializer(itemType);
        f++;
        // add the previous meta second
        if (dataset.hasMetaPart()) {
            outputSerDes[f] = FormatUtils.getDefaultFormat().getSerdeProvider().getSerializerDeserializer(metaItemType);
            outputTypeTraits[f] = FormatUtils.getDefaultFormat().getTypeTraitProvider().getTypeTrait(metaItemType);
            f++;
        }
        // add the previous filter third
        int fieldIdx = -1;
        if (numFilterFields > 0) {
            String filterField = DatasetUtil.getFilterField(dataset).get(0);
            String[] fieldNames = itemType.getFieldNames();
            int i = 0;
            for (; i < fieldNames.length; i++) {
                if (fieldNames[i].equals(filterField)) {
                    break;
                }
            }
            fieldIdx = i;
            outputTypeTraits[f] = FormatUtils.getDefaultFormat().getTypeTraitProvider()
                    .getTypeTrait(itemType.getFieldTypes()[fieldIdx]);
            outputSerDes[f] = FormatUtils.getDefaultFormat().getSerdeProvider()
                    .getSerializerDeserializer(itemType.getFieldTypes()[fieldIdx]);
            f++;
        }
        for (int j = 0; j < inputRecordDesc.getFieldCount(); j++) {
            outputTypeTraits[j + f] = inputRecordDesc.getTypeTraits()[j];
            outputSerDes[j + f] = inputRecordDesc.getFields()[j];
        }
        return new RecordDescriptor(outputSerDes, outputTypeTraits);
    }
}
