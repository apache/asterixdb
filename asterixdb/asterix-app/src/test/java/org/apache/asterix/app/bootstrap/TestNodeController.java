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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.app.external.TestLibrarian;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.app.nc.TransactionSubsystem;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.dataflow.LSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.metadata.utils.SplitsAndConstraintsUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.asterix.runtime.utils.CcApplicationContext;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.runtime.CommitRuntime;
import org.apache.asterix.transaction.management.service.logging.LogReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
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
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.util.file.FileUtil;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestNodeController {
    protected static final Logger LOGGER = Logger.getLogger(TestNodeController.class.getName());

    protected static final String PATH_ACTUAL = "unittest" + File.separator;
    protected static final String PATH_BASE = FileUtil.joinPath("src", "test", "resources", "nodetests");

    protected static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";
    protected static TransactionProperties txnProperties;
    private static final boolean cleanupOnStart = true;
    private static final boolean cleanupOnStop = true;

    // Constants
    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;
    public static final int KB32 = 32768;
    public static final int PARTITION = 0;
    public static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    public static final TransactionSubsystemProvider TXN_SUBSYSTEM_PROVIDER = TransactionSubsystemProvider.INSTANCE;
    // Mutables
    private JobId jobId;
    private long jobCounter = 0L;
    private IHyracksJobletContext jobletCtx;
    private final String testConfigFileName;
    private final boolean runHDFS;

    public TestNodeController(String testConfigFileName, boolean runHDFS) {
        this.testConfigFileName = testConfigFileName;
        this.runHDFS = runHDFS;
    }

    public void init() throws Exception {
        try {
            File outdir = new File(PATH_ACTUAL);
            outdir.mkdirs();
            // remove library directory
            TestLibrarian.removeLibraryDir();
            ExecutionTestUtil.setUp(cleanupOnStart,
                    testConfigFileName == null ? TEST_CONFIG_FILE_NAME : testConfigFileName,
                    ExecutionTestUtil.integrationUtil, runHDFS);
        } catch (Throwable th) {
            th.printStackTrace();
            throw th;
        }
        jobletCtx = Mockito.mock(IHyracksJobletContext.class);
        Mockito.when(jobletCtx.getServiceContext()).thenReturn(ExecutionTestUtil.integrationUtil.ncs[0].getContext());
        Mockito.when(jobletCtx.getJobId()).thenAnswer(new Answer<JobId>() {
            @Override
            public JobId answer(InvocationOnMock invocation) throws Throwable {
                return jobId;
            }
        });
    }

    public void deInit() throws Exception {
        TestLibrarian.removeLibraryDir();
        ExecutionTestUtil.tearDown(cleanupOnStop);
    }

    public org.apache.asterix.common.transactions.JobId getTxnJobId() {
        return new org.apache.asterix.common.transactions.JobId((int) jobId.getId());
    }

    public Pair<LSMInsertDeleteOperatorNodePushable, CommitRuntime> getInsertPipeline(IHyracksTaskContext ctx,
            Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties, int[] filterFields,
            int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
            StorageComponentProvider storageComponentProvider) throws AlgebricksException, HyracksDataException {
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields, primaryKeyIndexes, primaryKeyIndicators,
                storageComponentProvider);
        IndexOperation op = IndexOperation.INSERT;
        IModificationOperationCallbackFactory modOpCallbackFactory =
                new PrimaryIndexModificationOperationCallbackFactory(getTxnJobId(), dataset.getDatasetId(),
                        primaryIndexInfo.primaryKeyIndexes, TXN_SUBSYSTEM_PROVIDER, Operation.get(op),
                        ResourceType.LSM_BTREE);
        IRecordDescriptorProvider recordDescProvider = primaryIndexInfo.getInsertRecordDescriptorProvider();
        IIndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                storageComponentProvider.getStorageManager(), primaryIndexInfo.fileSplitProvider);
        LSMInsertDeleteOperatorNodePushable insertOp = new LSMInsertDeleteOperatorNodePushable(ctx, PARTITION,
                primaryIndexInfo.primaryIndexInsertFieldsPermutations,
                recordDescProvider.getInputRecordDescriptor(new ActivityId(new OperatorDescriptorId(0), 0), 0), op,
                true, indexHelperFactory, modOpCallbackFactory, null);
        CommitRuntime commitOp = new CommitRuntime(ctx, getTxnJobId(), dataset.getDatasetId(),
                primaryIndexInfo.primaryKeyIndexes, false, true, PARTITION, true);
        insertOp.setOutputFrameWriter(0, commitOp, primaryIndexInfo.rDesc);
        commitOp.setInputRecordDescriptor(0, primaryIndexInfo.rDesc);
        return Pair.of(insertOp, commitOp);
    }

    public IPushRuntime getFullScanPipeline(IFrameWriter countOp, IHyracksTaskContext ctx, Dataset dataset,
            IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
            NoMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties, int[] filterFields,
            int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
            StorageComponentProvider storageComponentProvider) throws HyracksDataException, AlgebricksException {
        IPushRuntime emptyTupleOp = new EmptyTupleSourceRuntimeFactory().createPushRuntime(ctx);
        JobSpecification spec = new JobSpecification();
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields, primaryKeyIndexes, primaryKeyIndicators,
                storageComponentProvider);
        IIndexDataflowHelperFactory indexDataflowHelperFactory = new IndexDataflowHelperFactory(
                storageComponentProvider.getStorageManager(), primaryIndexInfo.fileSplitProvider);
        BTreeSearchOperatorDescriptor searchOpDesc = new BTreeSearchOperatorDescriptor(spec, primaryIndexInfo.rDesc,
                null, null, true, true, indexDataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, filterFields, filterFields, false);
        BTreeSearchOperatorNodePushable searchOp =
                searchOpDesc.createPushRuntime(ctx, primaryIndexInfo.getSearchRecordDescriptorProvider(), PARTITION, 1);
        emptyTupleOp.setOutputFrameWriter(0, searchOp,
                primaryIndexInfo.getSearchRecordDescriptorProvider().getInputRecordDescriptor(null, 0));
        searchOp.setOutputFrameWriter(0, countOp, primaryIndexInfo.rDesc);
        return emptyTupleOp;
    }

    public LogReader getTransactionLogReader(boolean isRecoveryMode) {
        return (LogReader) getTransactionSubsystem().getLogManager().getLogReader(isRecoveryMode);
    }

    public JobId newJobId() {
        jobId = new JobId(jobCounter++);
        return jobId;
    }

    public IResourceFactory getPrimaryResourceFactory(IHyracksTaskContext ctx, PrimaryIndexInfo primaryIndexInfo,
            IStorageComponentProvider storageComponentProvider, Dataset dataset) throws AlgebricksException {
        Dataverse dataverse = new Dataverse(dataset.getDataverseName(), NonTaggedDataFormat.class.getName(),
                MetadataUtil.PENDING_NO_OP);
        Index index = primaryIndexInfo.getIndex();
        CcApplicationContext appCtx =
                (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
        MetadataProvider mdProvider = new MetadataProvider(appCtx, dataverse, storageComponentProvider);
        try {
            return dataset.getResourceFactory(mdProvider, index, primaryIndexInfo.recordType, primaryIndexInfo.metaType,
                    primaryIndexInfo.mergePolicyFactory, primaryIndexInfo.mergePolicyProperties);
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    public void createPrimaryIndex(Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType,
            ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            int[] filterFields, IStorageComponentProvider storageComponentProvider, int[] primaryKeyIndexes,
            List<Integer> primaryKeyIndicators) throws AlgebricksException, HyracksDataException {
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields, primaryKeyIndexes, primaryKeyIndicators,
                storageComponentProvider);
        Dataverse dataverse = new Dataverse(dataset.getDataverseName(), NonTaggedDataFormat.class.getName(),
                MetadataUtil.PENDING_NO_OP);
        MetadataProvider mdProvider = new MetadataProvider(
                (ICcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext(), dataverse,
                storageComponentProvider);
        try {
            IResourceFactory resourceFactory = dataset.getResourceFactory(mdProvider, primaryIndexInfo.index,
                    recordType, metaType, mergePolicyFactory, mergePolicyProperties);
            IndexBuilderFactory indexBuilderFactory =
                    new IndexBuilderFactory(storageComponentProvider.getStorageManager(),
                            primaryIndexInfo.fileSplitProvider, resourceFactory, !dataset.isTemp());
            IHyracksTaskContext ctx = createTestContext(false);
            IIndexBuilder indexBuilder = indexBuilderFactory.create(ctx, 0);
            indexBuilder.build();
        } finally {
            mdProvider.getLocks().unlock();
        }
    }

    private int[] createPrimaryIndexBloomFilterFields(int length) {
        int[] primaryIndexBloomFilterKeyFields = new int[length];
        for (int j = 0; j < length; ++j) {
            primaryIndexBloomFilterKeyFields[j] = j;
        }
        return primaryIndexBloomFilterKeyFields;
    }

    private IBinaryComparatorFactory[] createPrimaryIndexComparatorFactories(IAType[] primaryKeyTypes) {
        IBinaryComparatorFactory[] primaryIndexComparatorFactories =
                new IBinaryComparatorFactory[primaryKeyTypes.length];
        for (int j = 0; j < primaryKeyTypes.length; ++j) {
            primaryIndexComparatorFactories[j] =
                    BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(primaryKeyTypes[j], true);
        }
        return primaryIndexComparatorFactories;
    }

    private ISerializerDeserializer<?>[] createPrimaryIndexSerdes(int primaryIndexNumOfTupleFields,
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

    private ITypeTraits[] createPrimaryIndexTypeTraits(int primaryIndexNumOfTupleFields, IAType[] primaryKeyTypes,
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

    public IHyracksTaskContext createTestContext(boolean withMessaging) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(KB32);
        if (withMessaging) {
            TaskUtil.put(HyracksConstants.KEY_MESSAGE, new VSizeFrame(ctx), ctx);
        }
        ctx = Mockito.spy(ctx);
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletCtx);
        Mockito.when(ctx.getIoManager()).thenReturn(ExecutionTestUtil.integrationUtil.ncs[0].getIoManager());
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

    @SuppressWarnings("unused")
    private class PrimaryIndexInfo {
        private Dataset dataset;
        private IAType[] primaryKeyTypes;
        private ARecordType recordType;
        private ARecordType metaType;
        private ILSMMergePolicyFactory mergePolicyFactory;
        private Map<String, String> mergePolicyProperties;
        private int[] filterFields;
        private int primaryIndexNumOfTupleFields;
        private IBinaryComparatorFactory[] primaryIndexComparatorFactories;
        private ITypeTraits[] primaryIndexTypeTraits;
        private ISerializerDeserializer<?>[] primaryIndexSerdes;
        private int[] primaryIndexBloomFilterKeyFields;
        private ITypeTraits[] filterTypeTraits;
        private IBinaryComparatorFactory[] filterCmpFactories;
        private int[] btreeFields;
        private ConstantFileSplitProvider fileSplitProvider;
        private RecordDescriptor rDesc;
        private int[] primaryIndexInsertFieldsPermutations;
        private int[] primaryKeyIndexes;
        private List<List<String>> keyFieldNames;
        private List<Integer> keyFieldSourceIndicators;
        private List<IAType> keyFieldTypes;
        private Index index;
        private IStorageComponentProvider storageComponentProvider;

        public PrimaryIndexInfo(Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
                ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
                int[] filterFields, int[] primaryKeyIndexes, List<Integer> primaryKeyIndicators,
                IStorageComponentProvider storageComponentProvider) throws AlgebricksException {
            this.storageComponentProvider = storageComponentProvider;
            this.dataset = dataset;
            this.primaryKeyTypes = primaryKeyTypes;
            this.recordType = recordType;
            this.metaType = metaType;
            this.mergePolicyFactory = mergePolicyFactory;
            this.mergePolicyProperties = mergePolicyProperties;
            this.filterFields = filterFields;
            this.primaryKeyIndexes = primaryKeyIndexes;
            primaryIndexNumOfTupleFields = primaryKeyTypes.length + (1 + ((metaType == null) ? 0 : 1));
            primaryIndexTypeTraits =
                    createPrimaryIndexTypeTraits(primaryIndexNumOfTupleFields, primaryKeyTypes, recordType, metaType);
            primaryIndexComparatorFactories = createPrimaryIndexComparatorFactories(primaryKeyTypes);
            primaryIndexBloomFilterKeyFields = createPrimaryIndexBloomFilterFields(primaryKeyTypes.length);
            filterTypeTraits = DatasetUtil.computeFilterTypeTraits(dataset, recordType);
            filterCmpFactories = DatasetUtil.computeFilterBinaryComparatorFactories(dataset, recordType,
                    NonTaggedDataFormat.INSTANCE.getBinaryComparatorFactoryProvider());
            btreeFields = DatasetUtil.createBTreeFieldsWhenThereisAFilter(dataset);
            primaryIndexSerdes =
                    createPrimaryIndexSerdes(primaryIndexNumOfTupleFields, primaryKeyTypes, recordType, metaType);
            rDesc = new RecordDescriptor(primaryIndexSerdes, primaryIndexTypeTraits);
            primaryIndexInsertFieldsPermutations = new int[primaryIndexNumOfTupleFields];
            for (int i = 0; i < primaryIndexNumOfTupleFields; i++) {
                primaryIndexInsertFieldsPermutations[i] = i;
            }
            keyFieldSourceIndicators = primaryKeyIndicators;
            keyFieldNames = new ArrayList<>();
            keyFieldTypes = Arrays.asList(primaryKeyTypes);
            for (int i = 0; i < keyFieldSourceIndicators.size(); i++) {
                Integer indicator = keyFieldSourceIndicators.get(i);
                String[] fieldNames =
                        indicator == Index.RECORD_INDICATOR ? recordType.getFieldNames() : metaType.getFieldNames();
                keyFieldNames.add(Arrays.asList(fieldNames[primaryKeyIndexes[i]]));
            }
            index = new Index(dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName(),
                    IndexType.BTREE, keyFieldNames, keyFieldSourceIndicators, keyFieldTypes, false, false, true,
                    MetadataUtil.PENDING_NO_OP);
            List<String> nodes = Collections.singletonList(ExecutionTestUtil.integrationUtil.ncs[0].getId());
            FileSplit[] splits = SplitsAndConstraintsUtil.getIndexSplits(dataset, index.getIndexName(), nodes);
            fileSplitProvider = new ConstantFileSplitProvider(Arrays.copyOfRange(splits, 0, 1));
        }

        public Index getIndex() {
            return index;
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
                primaryIndexInfo.fileSplitProvider);
    }

    public IIndexDataflowHelper getPrimaryIndexDataflowHelper(Dataset dataset, IAType[] primaryKeyTypes,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] filterFields,
            IStorageComponentProvider storageComponentProvider, int[] primaryKeyIndexes,
            List<Integer> primaryKeyIndicators) throws AlgebricksException, HyracksDataException {
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields, primaryKeyIndexes, primaryKeyIndicators,
                storageComponentProvider);
        return getPrimaryIndexDataflowHelperFactory(primaryIndexInfo, storageComponentProvider)
                .create(createTestContext(true).getJobletContext().getServiceContext(), PARTITION);
    }
}