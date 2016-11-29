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
import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.algebra.operators.physical.CommitRuntime;
import org.apache.asterix.app.external.TestLibrarian;
import org.apache.asterix.app.nc.AsterixNCAppRuntimeContext;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.dataflow.AsterixLSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.IResourceFactory;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.asterix.runtime.util.AsterixRuntimeComponentsProvider;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadataFactory;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.asterix.transaction.management.service.logging.LogReader;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.util.TaskUtils;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelper;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;
import org.apache.hyracks.test.support.TestUtils;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestNodeController {
    protected static final Logger LOGGER = Logger.getLogger(TestNodeController.class.getName());

    protected static final String PATH_ACTUAL = "unittest" + File.separator;
    protected static final String PATH_BASE = StringUtils.join(new String[] { "src", "test", "resources", "nodetests" },
            File.separator);

    protected static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";
    protected static AsterixTransactionProperties txnProperties;
    private static final boolean cleanupOnStart = true;
    private static final boolean cleanupOnStop = true;

    // Constants
    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;
    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;
    public static final int KB32 = 32768;
    public static final int PARTITION = 0;
    public static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    public static final TransactionSubsystemProvider TXN_SUBSYSTEM_PROVIDER = new TransactionSubsystemProvider();
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
        Mockito.when(jobletCtx.getApplicationContext())
                .thenReturn(ExecutionTestUtil.integrationUtil.ncs[0].getApplicationContext());
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

    public AsterixLSMInsertDeleteOperatorNodePushable getInsertPipeline(IHyracksTaskContext ctx, Dataset dataset,
            IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
            ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties, int[] filterFields)
            throws AlgebricksException, HyracksDataException {
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields);
        IndexOperation op = IndexOperation.INSERT;
        IModificationOperationCallbackFactory modOpCallbackFactory = new PrimaryIndexModificationOperationCallbackFactory(
                getTxnJobId(), dataset.getDatasetId(), primaryIndexInfo.primaryKeyIndexes, TXN_SUBSYSTEM_PROVIDER, op,
                ResourceType.LSM_BTREE, true);
        AsterixLSMTreeInsertDeleteOperatorDescriptor indexOpDesc = getInsertOpratorDesc(primaryIndexInfo,
                modOpCallbackFactory);
        LSMBTreeDataflowHelperFactory dataflowHelperFactory = getPrimaryIndexDataflowHelperFactory(ctx,
                primaryIndexInfo);
        Mockito.when(indexOpDesc.getIndexDataflowHelperFactory()).thenReturn(dataflowHelperFactory);
        IRecordDescriptorProvider recordDescProvider = primaryIndexInfo.getInsertRecordDescriptorProvider();
        AsterixLSMInsertDeleteOperatorNodePushable insertOp = new AsterixLSMInsertDeleteOperatorNodePushable(
                indexOpDesc, ctx, PARTITION, primaryIndexInfo.primaryIndexInsertFieldsPermutations, recordDescProvider,
                op, true);
        CommitRuntime commitOp = new CommitRuntime(ctx, getTxnJobId(), dataset.getDatasetId(),
                primaryIndexInfo.primaryKeyIndexes, false, true, PARTITION, true);
        insertOp.setOutputFrameWriter(0, commitOp, primaryIndexInfo.rDesc);
        commitOp.setInputRecordDescriptor(0, primaryIndexInfo.rDesc);
        return insertOp;
    }

    public IPushRuntime getFullScanPipeline(IFrameWriter countOp, IHyracksTaskContext ctx, Dataset dataset,
            IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
            NoMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties, int[] filterFields)
            throws HyracksDataException, AlgebricksException {
        IPushRuntime emptyTupleOp = new EmptyTupleSourceRuntimeFactory().createPushRuntime(ctx);
        JobSpecification spec = new JobSpecification();
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields);
        LSMBTreeDataflowHelperFactory indexDataflowHelperFactory = getPrimaryIndexDataflowHelperFactory(ctx,
                primaryIndexInfo);
        BTreeSearchOperatorDescriptor searchOpDesc = new BTreeSearchOperatorDescriptor(spec, primaryIndexInfo.rDesc,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                primaryIndexInfo.fileSplitProvider, primaryIndexInfo.primaryIndexTypeTraits,
                primaryIndexInfo.primaryIndexComparatorFactories, primaryIndexInfo.primaryIndexBloomFilterKeyFields,
                primaryIndexInfo.primaryKeyIndexes, primaryIndexInfo.primaryKeyIndexes, true, true,
                indexDataflowHelperFactory, false, false, null, NoOpOperationCallbackFactory.INSTANCE, filterFields,
                filterFields);
        BTreeSearchOperatorNodePushable searchOp = new BTreeSearchOperatorNodePushable(searchOpDesc, ctx, 0,
                primaryIndexInfo.getSearchRecordDescriptorProvider(), /*primaryIndexInfo.primaryKeyIndexes*/null,
                /*primaryIndexInfo.primaryKeyIndexes*/null, true, true, filterFields, filterFields);
        emptyTupleOp.setFrameWriter(0, searchOp,
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

    public AsterixLSMTreeInsertDeleteOperatorDescriptor getInsertOpratorDesc(PrimaryIndexInfo primaryIndexInfo,
            IModificationOperationCallbackFactory modOpCallbackFactory) {
        AsterixLSMTreeInsertDeleteOperatorDescriptor indexOpDesc = Mockito
                .mock(AsterixLSMTreeInsertDeleteOperatorDescriptor.class);
        Mockito.when(indexOpDesc.getLifecycleManagerProvider())
                .thenReturn(AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);
        Mockito.when(indexOpDesc.getStorageManager()).thenReturn(AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);
        Mockito.when(indexOpDesc.getFileSplitProvider()).thenReturn(primaryIndexInfo.fileSplitProvider);
        Mockito.when(indexOpDesc.getLocalResourceFactoryProvider())
                .thenReturn(primaryIndexInfo.localResourceFactoryProvider);
        Mockito.when(indexOpDesc.getTreeIndexTypeTraits()).thenReturn(primaryIndexInfo.primaryIndexTypeTraits);
        Mockito.when(indexOpDesc.getTreeIndexComparatorFactories())
                .thenReturn(primaryIndexInfo.primaryIndexComparatorFactories);
        Mockito.when(indexOpDesc.getTreeIndexBloomFilterKeyFields())
                .thenReturn(primaryIndexInfo.primaryIndexBloomFilterKeyFields);
        Mockito.when(indexOpDesc.getModificationOpCallbackFactory()).thenReturn(modOpCallbackFactory);
        return indexOpDesc;
    }

    public TreeIndexCreateOperatorDescriptor getIndexCreateOpDesc(PrimaryIndexInfo primaryIndexInfo) {
        TreeIndexCreateOperatorDescriptor indexOpDesc = Mockito.mock(TreeIndexCreateOperatorDescriptor.class);
        Mockito.when(indexOpDesc.getLifecycleManagerProvider())
                .thenReturn(AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);
        Mockito.when(indexOpDesc.getStorageManager()).thenReturn(AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER);
        Mockito.when(indexOpDesc.getFileSplitProvider()).thenReturn(primaryIndexInfo.fileSplitProvider);
        Mockito.when(indexOpDesc.getLocalResourceFactoryProvider())
                .thenReturn(primaryIndexInfo.localResourceFactoryProvider);
        Mockito.when(indexOpDesc.getTreeIndexTypeTraits()).thenReturn(primaryIndexInfo.primaryIndexTypeTraits);
        Mockito.when(indexOpDesc.getTreeIndexComparatorFactories())
                .thenReturn(primaryIndexInfo.primaryIndexComparatorFactories);
        Mockito.when(indexOpDesc.getTreeIndexBloomFilterKeyFields())
                .thenReturn(primaryIndexInfo.primaryIndexBloomFilterKeyFields);
        return indexOpDesc;
    }

    public ConstantFileSplitProvider getFileSplitProvider(Dataset dataset) {
        FileSplit fileSplit = new ManagedFileSplit(ExecutionTestUtil.integrationUtil.ncs[0].getId(),
                dataset.getDataverseName() + File.separator + dataset.getDatasetName());
        return new ConstantFileSplitProvider(new FileSplit[] { fileSplit });
    }

    public ILocalResourceFactoryProvider getPrimaryIndexLocalResourceMetadataProvider(Dataset dataset,
            ITypeTraits[] primaryIndexTypeTraits, IBinaryComparatorFactory[] primaryIndexComparatorFactories,
            int[] primaryIndexBloomFilterKeyFields, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields, int[] filterFields) {
        IResourceFactory localResourceMetadata = new LSMBTreeLocalResourceMetadataFactory(primaryIndexTypeTraits,
                primaryIndexComparatorFactories, primaryIndexBloomFilterKeyFields, true, dataset.getDatasetId(),
                mergePolicyFactory, mergePolicyProperties, filterTypeTraits, filterCmpFactories, btreeFields,
                filterFields);
        ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                localResourceMetadata, LocalResource.LSMBTreeResource);
        return localResourceFactoryProvider;
    }

    public LSMBTreeDataflowHelper getPrimaryIndexDataflowHelper(IHyracksTaskContext ctx,
            PrimaryIndexInfo primaryIndexInfo, TreeIndexCreateOperatorDescriptor indexOpDesc)
            throws AlgebricksException, HyracksDataException {
        LSMBTreeDataflowHelperFactory dataflowHelperFactory = new LSMBTreeDataflowHelperFactory(
                new AsterixVirtualBufferCacheProvider(primaryIndexInfo.dataset.getDatasetId()),
                primaryIndexInfo.mergePolicyFactory, primaryIndexInfo.mergePolicyProperties,
                new PrimaryIndexOperationTrackerProvider(primaryIndexInfo.dataset.getDatasetId()),
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                BLOOM_FILTER_FALSE_POSITIVE_RATE, true, primaryIndexInfo.filterTypeTraits,
                primaryIndexInfo.filterCmpFactories, primaryIndexInfo.btreeFields, primaryIndexInfo.filterFields, true);
        IndexDataflowHelper dataflowHelper = dataflowHelperFactory.createIndexDataflowHelper(indexOpDesc, ctx,
                PARTITION);
        return (LSMBTreeDataflowHelper) dataflowHelper;
    }

    public LSMBTreeDataflowHelperFactory getPrimaryIndexDataflowHelperFactory(IHyracksTaskContext ctx,
            PrimaryIndexInfo primaryIndexInfo) throws AlgebricksException {
        return new LSMBTreeDataflowHelperFactory(
                new AsterixVirtualBufferCacheProvider(primaryIndexInfo.dataset.getDatasetId()),
                primaryIndexInfo.mergePolicyFactory, primaryIndexInfo.mergePolicyProperties,
                new PrimaryIndexOperationTrackerProvider(primaryIndexInfo.dataset.getDatasetId()),
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                BLOOM_FILTER_FALSE_POSITIVE_RATE, true, primaryIndexInfo.filterTypeTraits,
                primaryIndexInfo.filterCmpFactories, primaryIndexInfo.btreeFields, primaryIndexInfo.filterFields, true);
    }

    public LSMBTreeDataflowHelper getPrimaryIndexDataflowHelper(Dataset dataset, IAType[] primaryKeyTypes,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] filterFields)
            throws AlgebricksException, HyracksDataException {
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields);
        TreeIndexCreateOperatorDescriptor indexOpDesc = getIndexCreateOpDesc(primaryIndexInfo);
        return getPrimaryIndexDataflowHelper(createTestContext(true), primaryIndexInfo, indexOpDesc);
    }

    public void createPrimaryIndex(Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType,
            ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
            int[] filterFields) throws AlgebricksException, HyracksDataException {
        PrimaryIndexInfo primaryIndexInfo = new PrimaryIndexInfo(dataset, primaryKeyTypes, recordType, metaType,
                mergePolicyFactory, mergePolicyProperties, filterFields);
        TreeIndexCreateOperatorDescriptor indexOpDesc = getIndexCreateOpDesc(primaryIndexInfo);
        LSMBTreeDataflowHelper dataflowHelper = getPrimaryIndexDataflowHelper(createTestContext(true), primaryIndexInfo,
                indexOpDesc);
        dataflowHelper.create();
    }

    private int[] createPrimaryIndexBloomFilterFields(int length) {
        int[] primaryIndexBloomFilterKeyFields = new int[length];
        for (int j = 0; j < length; ++j) {
            primaryIndexBloomFilterKeyFields[j] = j;
        }
        return primaryIndexBloomFilterKeyFields;
    }

    private IBinaryComparatorFactory[] createPrimaryIndexComparatorFactories(IAType[] primaryKeyTypes) {
        IBinaryComparatorFactory[] primaryIndexComparatorFactories = new IBinaryComparatorFactory[primaryKeyTypes.length];
        for (int j = 0; j < primaryKeyTypes.length; ++j) {
            primaryIndexComparatorFactories[j] = BinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(primaryKeyTypes[j], true);
        }
        return primaryIndexComparatorFactories;
    }

    private ISerializerDeserializer<?>[] createPrimaryIndexSerdes(int primaryIndexNumOfTupleFields,
            IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType) {
        int i = 0;
        ISerializerDeserializer<?>[] primaryIndexSerdes = new ISerializerDeserializer<?>[primaryIndexNumOfTupleFields];
        for (; i < primaryKeyTypes.length; i++) {
            primaryIndexSerdes[i] = SerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(primaryKeyTypes[i]);
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
            TaskUtils.putInSharedMap(HyracksConstants.KEY_MESSAGE, new VSizeFrame(ctx), ctx);
        }
        ctx = Mockito.spy(ctx);
        Mockito.when(ctx.getJobletContext()).thenReturn(jobletCtx);
        Mockito.when(ctx.getIOManager())
                .thenReturn(ExecutionTestUtil.integrationUtil.ncs[0].getIoManager());
        return ctx;
    }

    public TransactionSubsystem getTransactionSubsystem() {
        return (TransactionSubsystem) ((AsterixNCAppRuntimeContext) ExecutionTestUtil.integrationUtil.ncs[0]
                .getApplicationContext().getApplicationObject()).getTransactionSubsystem();
    }

    public ITransactionManager getTransactionManager() {
        return getTransactionSubsystem().getTransactionManager();
    }

    public AsterixNCAppRuntimeContext getAppRuntimeContext() {
        return (AsterixNCAppRuntimeContext) ExecutionTestUtil.integrationUtil.ncs[0].getApplicationContext()
                .getApplicationObject();
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
        private ILocalResourceFactoryProvider localResourceFactoryProvider;
        private ConstantFileSplitProvider fileSplitProvider;
        private RecordDescriptor rDesc;
        private int[] primaryIndexInsertFieldsPermutations;
        private int[] primaryKeyIndexes;

        public PrimaryIndexInfo(Dataset dataset, IAType[] primaryKeyTypes, ARecordType recordType, ARecordType metaType,
                ILSMMergePolicyFactory mergePolicyFactory, Map<String, String> mergePolicyProperties,
                int[] filterFields) throws AlgebricksException {
            this.dataset = dataset;
            this.primaryKeyTypes = primaryKeyTypes;
            this.recordType = recordType;
            this.metaType = metaType;
            this.mergePolicyFactory = mergePolicyFactory;
            this.mergePolicyProperties = mergePolicyProperties;
            this.filterFields = filterFields;
            primaryIndexNumOfTupleFields = primaryKeyTypes.length + (1 + ((metaType == null) ? 0 : 1));
            primaryIndexTypeTraits = createPrimaryIndexTypeTraits(primaryIndexNumOfTupleFields, primaryKeyTypes,
                    recordType, metaType);
            primaryIndexComparatorFactories = createPrimaryIndexComparatorFactories(primaryKeyTypes);
            primaryIndexBloomFilterKeyFields = createPrimaryIndexBloomFilterFields(primaryKeyTypes.length);
            filterTypeTraits = DatasetUtils.computeFilterTypeTraits(dataset, recordType);
            filterCmpFactories = DatasetUtils.computeFilterBinaryComparatorFactories(dataset, recordType,
                    NonTaggedDataFormat.INSTANCE.getBinaryComparatorFactoryProvider());
            btreeFields = DatasetUtils.createBTreeFieldsWhenThereisAFilter(dataset);
            localResourceFactoryProvider = getPrimaryIndexLocalResourceMetadataProvider(dataset, primaryIndexTypeTraits,
                    primaryIndexComparatorFactories, primaryIndexBloomFilterKeyFields, mergePolicyFactory,
                    mergePolicyProperties, filterTypeTraits, filterCmpFactories, btreeFields, filterFields);
            fileSplitProvider = getFileSplitProvider(dataset);
            primaryIndexSerdes = createPrimaryIndexSerdes(primaryIndexNumOfTupleFields, primaryKeyTypes, recordType,
                    metaType);
            rDesc = new RecordDescriptor(primaryIndexSerdes, primaryIndexTypeTraits);
            primaryIndexInsertFieldsPermutations = new int[primaryIndexNumOfTupleFields];
            for (int i = 0; i < primaryIndexNumOfTupleFields; i++) {
                primaryIndexInsertFieldsPermutations[i] = i;
            }
            primaryKeyIndexes = new int[primaryKeyTypes.length];
            for (int i = 0; i < primaryKeyIndexes.length; i++) {
                primaryKeyIndexes[i] = i;
            }
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
                primaryKeySerdes[i] = SerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(primaryKeyTypes[i]);
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
        ITypeTraits[] primaryIndexTypeTraits = createPrimaryIndexTypeTraits(primaryIndexNumOfTupleFields, keyTypes,
                recordType, metaType);
        ISerializerDeserializer<?>[] primaryIndexSerdes = createPrimaryIndexSerdes(primaryIndexNumOfTupleFields,
                keyTypes, recordType, metaType);
        return new RecordDescriptor(primaryIndexSerdes, primaryIndexTypeTraits);
    }
}