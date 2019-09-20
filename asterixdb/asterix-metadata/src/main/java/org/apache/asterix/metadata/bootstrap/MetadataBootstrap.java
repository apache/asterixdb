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
package org.apache.asterix.metadata.bootstrap;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.context.DatasetInfoProvider;
import org.apache.asterix.common.context.DatasetLSMComponentIdGeneratorFactory;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.MetadataException;
import org.apache.asterix.common.ioopcallbacks.LSMIndexIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMIndexPageWriteCallbackFactory;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerFactory;
import org.apache.asterix.transaction.management.resource.DatasetLocalResourceFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.build.IndexBuilder;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ConcurrentMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Initializes the remote metadata storage facilities ("universe") using a
 * MetadataManager that is assumed to be co-located in the same JVM. The
 * metadata universe can be bootstrapped from an existing set of metadata files,
 * or it can be started from scratch, creating all the necessary persistent
 * state. The startUniverse() method is intended to be called as part of
 * application deployment (i.e., inside an NC bootstrap), and similarly
 * stopUniverse() should be called upon application undeployment.
 */
public class MetadataBootstrap {
    public static final boolean IS_DEBUG_MODE = false;
    private static final Logger LOGGER = LogManager.getLogger();
    private static INcApplicationContext appContext;
    private static ILocalResourceRepository localResourceRepository;
    private static IIOManager ioManager;
    private static String metadataNodeName;
    private static List<String> nodeNames;
    private static boolean isNewUniverse;
    private static final IMetadataIndex[] PRIMARY_INDEXES =
            new IMetadataIndex[] { MetadataPrimaryIndexes.DATAVERSE_DATASET, MetadataPrimaryIndexes.DATASET_DATASET,
                    MetadataPrimaryIndexes.DATATYPE_DATASET, MetadataPrimaryIndexes.INDEX_DATASET,
                    MetadataPrimaryIndexes.NODE_DATASET, MetadataPrimaryIndexes.NODEGROUP_DATASET,
                    MetadataPrimaryIndexes.FUNCTION_DATASET, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET,
                    MetadataPrimaryIndexes.FEED_DATASET, MetadataPrimaryIndexes.FEED_POLICY_DATASET,
                    MetadataPrimaryIndexes.LIBRARY_DATASET, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET,
                    MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET, MetadataPrimaryIndexes.FEED_CONNECTION_DATASET };

    private MetadataBootstrap() {
    }

    /**
     * bootstrap metadata
     *
     * @param ncServiceContext
     * @param isNewUniverse
     * @throws ACIDException
     * @throws RemoteException
     * @throws AlgebricksException
     * @throws Exception
     */
    public static void startUniverse(INCServiceContext ncServiceContext, boolean isNewUniverse)
            throws RemoteException, ACIDException, AlgebricksException {
        MetadataBootstrap.setNewUniverse(isNewUniverse);
        appContext = (INcApplicationContext) ncServiceContext.getApplicationContext();

        MetadataProperties metadataProperties = appContext.getMetadataProperties();
        metadataNodeName = metadataProperties.getMetadataNodeName();
        nodeNames = metadataProperties.getNodeNames();
        localResourceRepository = appContext.getLocalResourceRepository();
        ioManager = ncServiceContext.getIoManager();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            for (int i = 0; i < PRIMARY_INDEXES.length; i++) {
                enlistMetadataDataset(ncServiceContext, PRIMARY_INDEXES[i]);
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Finished enlistment of metadata B-trees in " + (isNewUniverse ? "new" : "old") + " universe");
            }
            if (isNewUniverse) {
                insertInitialDataverses(mdTxnCtx);
                insertMetadataDatasets(mdTxnCtx, PRIMARY_INDEXES);
                insertMetadataDatatypes(mdTxnCtx);
                insertNodes(mdTxnCtx);
                insertInitialGroups(mdTxnCtx);
                insertInitialAdapters(mdTxnCtx);
                BuiltinFeedPolicies.insertInitialFeedPolicies(mdTxnCtx);
                insertInitialCompactionPolicies(mdTxnCtx);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Finished creating metadata B-trees.");
                }
            } else {
                insertNewCompactionPoliciesIfNotExist(mdTxnCtx);
            }
            // #. initialize datasetIdFactory
            MetadataManager.INSTANCE.initializeDatasetIdFactory(mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            try {
                if (IS_DEBUG_MODE) {
                    LOGGER.log(Level.ERROR, "Failure during metadata bootstrap", e);
                }
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } catch (Exception e2) {
                e.addSuppressed(e2);
                // TODO change the exception type to AbortFailureException
                throw new MetadataException(e);
            }
            throw new MetadataException(e);
        }
    }

    private static void insertInitialDataverses(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        String dataFormat = NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT;
        MetadataManager.INSTANCE.addDataverse(mdTxnCtx,
                new Dataverse(MetadataConstants.METADATA_DATAVERSE_NAME, dataFormat, MetadataUtil.PENDING_NO_OP));
        MetadataManager.INSTANCE.addDataverse(mdTxnCtx, MetadataBuiltinEntities.DEFAULT_DATAVERSE);
    }

    /**
     * Inserts a metadata dataset to the physical dataset index Should be performed
     * on a bootstrap of a new universe
     *
     * @param mdTxnCtx
     * @param indexes
     * @throws AlgebricksException
     */
    public static void insertMetadataDatasets(MetadataTransactionContext mdTxnCtx, IMetadataIndex[] indexes)
            throws AlgebricksException {
        for (int i = 0; i < indexes.length; i++) {
            IDatasetDetails id = new InternalDatasetDetails(FileStructure.BTREE, PartitioningStrategy.HASH,
                    indexes[i].getPartitioningExpr(), indexes[i].getPartitioningExpr(), null,
                    indexes[i].getPartitioningExprType(), false, null);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx,
                    new Dataset(indexes[i].getDataverseName(), indexes[i].getIndexedDatasetName(),
                            indexes[i].getDataverseName(), indexes[i].getPayloadRecordType().getTypeName(),
                            indexes[i].getNodeGroupName(), StorageConstants.DEFAULT_COMPACTION_POLICY_NAME,
                            StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES, id, new HashMap<String, String>(),
                            DatasetType.INTERNAL, indexes[i].getDatasetId().getId(), MetadataUtil.PENDING_NO_OP));
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Finished inserting initial datasets.");
        }
    }

    private static void getMetadataTypes(ArrayList<IAType> types) {
        for (int i = 0; i < PRIMARY_INDEXES.length; i++) {
            types.add(PRIMARY_INDEXES[i].getPayloadRecordType());
        }
    }

    private static void insertMetadataDatatypes(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        ArrayList<IAType> types = new ArrayList<>();
        types.addAll(BuiltinTypeMap.getAllBuiltinTypes());
        getMetadataTypes(types);
        for (int i = 0; i < types.size(); i++) {
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME,
                    types.get(i).getTypeName(), types.get(i), false));
        }
        MetadataManager.INSTANCE.addDatatype(mdTxnCtx, MetadataBuiltinEntities.ANY_OBJECT_DATATYPE);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Finished inserting initial datatypes.");
        }
    }

    private static void insertNodes(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        for (String nodesName : nodeNames) {
            MetadataManager.INSTANCE.addNode(mdTxnCtx, new Node(nodesName, 0, 0));
        }
    }

    private static void insertInitialGroups(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        List<String> metadataGroupNodeNames = new ArrayList<>();
        metadataGroupNodeNames.add(metadataNodeName);
        NodeGroup groupRecord = new NodeGroup(MetadataConstants.METADATA_NODEGROUP_NAME, metadataGroupNodeNames);
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, groupRecord);
    }

    private static void insertInitialAdapters(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        String[] builtInAdapterClassNames = new String[] { GenericAdapterFactory.class.getName() };
        DatasourceAdapter adapter;
        for (String adapterClassName : builtInAdapterClassNames) {
            adapter = getAdapter(adapterClassName);
            MetadataManager.INSTANCE.addAdapter(mdTxnCtx, adapter);
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Finished inserting built-in adapters.");
        }
    }

    private static void insertInitialCompactionPolicies(MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException {
        String[] builtInCompactionPolicyClassNames = new String[] { ConstantMergePolicyFactory.class.getName(),
                PrefixMergePolicyFactory.class.getName(), ConcurrentMergePolicyFactory.class.getName(),
                NoMergePolicyFactory.class.getName(), CorrelatedPrefixMergePolicyFactory.class.getName() };
        for (String policyClassName : builtInCompactionPolicyClassNames) {
            CompactionPolicy compactionPolicy = getCompactionPolicyEntity(policyClassName);
            MetadataManager.INSTANCE.addCompactionPolicy(mdTxnCtx, compactionPolicy);
        }
    }

    /**
     * Insert newly introduced merge policies into existing datasets (for backward-compatibility)
     *
     * @param mdTxnCtx
     * @throws AlgebricksException
     */
    private static void insertNewCompactionPoliciesIfNotExist(MetadataTransactionContext mdTxnCtx)
            throws AlgebricksException {
        if (MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                ConcurrentMergePolicyFactory.NAME) == null) {
            CompactionPolicy compactionPolicy = getCompactionPolicyEntity(ConcurrentMergePolicyFactory.class.getName());
            MetadataManager.INSTANCE.addCompactionPolicy(mdTxnCtx, compactionPolicy);
        }
    }

    private static DatasourceAdapter getAdapter(String adapterFactoryClassName) throws AlgebricksException {
        try {
            String adapterName = ((IAdapterFactory) (Class.forName(adapterFactoryClassName).newInstance())).getAlias();
            return new DatasourceAdapter(new AdapterIdentifier(MetadataConstants.METADATA_DATAVERSE_NAME, adapterName),
                    adapterFactoryClassName, IDataSourceAdapter.AdapterType.INTERNAL);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new MetadataException("Unable to instantiate builtin Adapter", e);
        }
    }

    private static CompactionPolicy getCompactionPolicyEntity(String compactionPolicyClassName)
            throws AlgebricksException {
        try {
            String policyName =
                    ((ILSMMergePolicyFactory) (Class.forName(compactionPolicyClassName).newInstance())).getName();
            return new CompactionPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, policyName,
                    compactionPolicyClassName);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new MetadataException("Unable to instantiate builtin Merge Policy Factory", e);
        }
    }

    /**
     * Enlist a metadata index so it is available for metadata operations should be
     * performed upon bootstrapping
     *
     * @param index
     * @throws HyracksDataException
     */
    public static void enlistMetadataDataset(INCServiceContext ncServiceCtx, IMetadataIndex index)
            throws HyracksDataException {
        final int datasetId = index.getDatasetId().getId();
        // reserve memory for metadata dataset to ensure it can be opened when needed
        if (!appContext.getDatasetMemoryManager().reserve(index.getDatasetId().getId())) {
            throw new IllegalStateException("Failed to reserve memory for metadata dataset (" + datasetId + ")");
        }
        String metadataPartitionPath =
                StoragePathUtil.prepareStoragePartitionPath(MetadataNode.INSTANCE.getMetadataStoragePartition());
        String resourceName = metadataPartitionPath + File.separator + index.getFileNameRelativePath();
        FileReference file = ioManager.resolve(resourceName);
        index.setFile(file);
        ITypeTraits[] typeTraits = index.getTypeTraits();
        IBinaryComparatorFactory[] cmpFactories = index.getKeyBinaryComparatorFactory();
        int[] bloomFilterKeyFields = index.getBloomFilterKeyFields();

        // opTrackerProvider and ioOpCallbackFactory should both be acquired through
        // IStorageManager
        // We are unable to do this since IStorageManager needs a dataset to determine
        // the appropriate
        // objects
        ILSMOperationTrackerFactory opTrackerFactory =
                index.isPrimaryIndex() ? new PrimaryIndexOperationTrackerFactory(datasetId)
                        : new SecondaryIndexOperationTrackerFactory(datasetId);
        ILSMComponentIdGeneratorFactory idGeneratorProvider = new DatasetLSMComponentIdGeneratorFactory(datasetId);
        DatasetInfoProvider datasetInfoProvider = new DatasetInfoProvider(datasetId);
        ILSMIOOperationCallbackFactory ioOpCallbackFactory =
                new LSMIndexIOOperationCallbackFactory(idGeneratorProvider, datasetInfoProvider);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = new LSMIndexPageWriteCallbackFactory();

        IStorageComponentProvider storageComponentProvider = appContext.getStorageComponentProvider();
        if (isNewUniverse()) {
            final double bloomFilterFalsePositiveRate =
                    appContext.getStorageProperties().getBloomFilterFalsePositiveRate();
            LSMBTreeLocalResourceFactory lsmBtreeFactory =
                    new LSMBTreeLocalResourceFactory(storageComponentProvider.getStorageManager(), typeTraits,
                            cmpFactories, null, null, null, opTrackerFactory, ioOpCallbackFactory,
                            pageWriteCallbackFactory, storageComponentProvider.getMetadataPageManagerFactory(),
                            new AsterixVirtualBufferCacheProvider(datasetId),
                            storageComponentProvider.getIoOperationSchedulerProvider(),
                            appContext.getMetadataMergePolicyFactory(),
                            StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES, true, bloomFilterKeyFields,
                            bloomFilterFalsePositiveRate, true, null, NoOpCompressorDecompressorFactory.INSTANCE);
            DatasetLocalResourceFactory dsLocalResourceFactory =
                    new DatasetLocalResourceFactory(datasetId, lsmBtreeFactory);
            // TODO(amoudi) Creating the index should be done through the same code path as
            // other indexes
            // This is to be done by having a metadata dataset associated with each index
            IIndexBuilder indexBuilder = new IndexBuilder(ncServiceCtx, storageComponentProvider.getStorageManager(),
                    index::getResourceId, file, dsLocalResourceFactory, true);
            indexBuilder.build();
        } else {
            final LocalResource resource = localResourceRepository.get(file.getRelativePath());
            if (resource == null) {
                throw new HyracksDataException("Could not find required metadata indexes. Please delete "
                        + appContext.getMetadataProperties().getTransactionLogDirs()
                                .get(appContext.getTransactionSubsystem().getId())
                        + " to intialize as a new instance. (WARNING: all data will be lost.)");
            }
            // Why do we care about metadata dataset's resource ids? why not assign them ids
            // similar to other resources?
            if (index.getResourceId() != resource.getId()) {
                throw new HyracksDataException("Resource Id doesn't match expected metadata index resource id");
            }
            IndexDataflowHelper indexHelper =
                    new IndexDataflowHelper(ncServiceCtx, storageComponentProvider.getStorageManager(), file);
            indexHelper.open(); // Opening the index through the helper will ensure it gets instantiated
            indexHelper.close();
        }
    }

    /**
     * Perform recovery of DDL operations metadata records
     */
    public static void startDDLRecovery() throws AlgebricksException {
        // #. clean up any record which has pendingAdd/DelOp flag
        // as traversing all records from DATAVERSE_DATASET to DATASET_DATASET, and then
        // to INDEX_DATASET.
        MetadataTransactionContext mdTxnCtx = null;
        LOGGER.info("Starting DDL recovery ...");
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
            for (Dataverse dataverse : dataverses) {
                recoverDataverse(mdTxnCtx, dataverse);
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            LOGGER.info("Completed DDL recovery.");
        } catch (Exception e) {
            try {
                LOGGER.error("Failure during DDL recovery", e);
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            throw MetadataException.create(e);
        }
    }

    private static void recoverDataverse(MetadataTransactionContext mdTxnCtx, Dataverse dataverse)
            throws AlgebricksException {
        if (dataverse.getPendingOp() != MetadataUtil.PENDING_NO_OP) {
            // drop pending dataverse
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverse.getDataverseName());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Dropped a pending dataverse: " + dataverse.getDataverseName());
            }
        } else {
            List<Dataset> datasets =
                    MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverse.getDataverseName());
            for (Dataset dataset : datasets) {
                recoverDataset(mdTxnCtx, dataset);
            }
        }
    }

    private static void recoverDataset(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws AlgebricksException {
        if (dataset.getPendingOp() != MetadataUtil.PENDING_NO_OP) {
            // drop pending dataset
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Dropped a pending dataset: " + dataset.getDataverseName() + "." + dataset.getDatasetName());
            }
        } else {
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName());
            for (Index index : indexes) {
                if (index.getPendingOp() != MetadataUtil.PENDING_NO_OP) {
                    // drop pending index
                    MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName(),
                            index.getIndexName());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Dropped a pending index: " + dataset.getDataverseName() + "."
                                + dataset.getDatasetName() + "." + index.getIndexName());
                    }
                }
            }
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // if the dataset has no indexes, delete all its files
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName());
            if (indexes.isEmpty()) {
                List<ExternalFile> files = MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                for (ExternalFile file : files) {
                    MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Dropped an external file: " + dataset.getDataverseName() + "."
                                + dataset.getDatasetName() + "." + file.getFileNumber());
                    }
                }
            }
        }
    }

    public static boolean isNewUniverse() {
        return isNewUniverse;
    }

    public static void setNewUniverse(boolean isNewUniverse) {
        MetadataBootstrap.isNewUniverse = isNewUniverse;
    }

}
