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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.asterix.common.api.INamespacePathResolver;
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
import org.apache.asterix.common.external.IDataSourceAdapter;
import org.apache.asterix.common.ioopcallbacks.AtomicLSMIndexIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMIndexIOOperationCallbackFactory;
import org.apache.asterix.common.ioopcallbacks.LSMIndexPageWriteCallbackFactory;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.external.api.ITypedAdapterFactory;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
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
    private static IMetadataIndex[] PRIMARY_INDEXES;

    private MetadataBootstrap() {
    }

    /**
     * bootstrap metadata
     *
     * @param ncServiceContext
     * @param isNewUniverse
     * @param mdIndexesProvider
     * @throws ACIDException
     * @throws RemoteException
     * @throws AlgebricksException
     * @throws Exception
     */
    public static void startUniverse(INCServiceContext ncServiceContext, boolean isNewUniverse,
            MetadataIndexesProvider mdIndexesProvider) throws RemoteException, ACIDException, AlgebricksException {
        PRIMARY_INDEXES = mdIndexesProvider.getMetadataIndexes();
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
                enlistMetadataDataset(ncServiceContext, PRIMARY_INDEXES[i], mdIndexesProvider);
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Finished enlistment of metadata B-trees in " + (isNewUniverse ? "new" : "old") + " universe");
            }
            if (isNewUniverse) {
                insertInitialDatabases(mdTxnCtx, mdIndexesProvider);
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
                insertSynonymEntitiesIfNotExist(mdTxnCtx, mdIndexesProvider);
                insertFullTextConfigAndFilterIfNotExist(mdTxnCtx, mdIndexesProvider);
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

    private static void insertInitialDatabases(MetadataTransactionContext mdTxnCtx,
            MetadataIndexesProvider mdIndexesProvider) throws AlgebricksException {
        if (mdIndexesProvider.isUsingDatabase()) {
            MetadataManager.INSTANCE.addDatabase(mdTxnCtx, MetadataBuiltinEntities.SYSTEM_DATABASE);
            MetadataManager.INSTANCE.addDatabase(mdTxnCtx, MetadataBuiltinEntities.DEFAULT_DATABASE);
        }
    }

    private static void insertInitialDataverses(MetadataTransactionContext mdTxnCtx) throws AlgebricksException {
        MetadataManager.INSTANCE.addDataverse(mdTxnCtx, MetadataBuiltinEntities.METADATA_DATAVERSE);
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
                    indexes[i].getPartitioningExprType(), false, null, null);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx,
                    new Dataset(indexes[i].getDatabaseName(), indexes[i].getDataverseName(),
                            indexes[i].getIndexedDatasetName(), indexes[i].getDatabaseName(),
                            indexes[i].getDataverseName(), indexes[i].getPayloadRecordType().getTypeName(),
                            indexes[i].getNodeGroupName(), StorageConstants.DEFAULT_COMPACTION_POLICY_NAME,
                            StorageConstants.DEFAULT_COMPACTION_POLICY_PROPERTIES, id, new HashMap<>(),
                            DatasetType.INTERNAL, indexes[i].getDatasetId().getId(), MetadataUtil.PENDING_NO_OP));
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Finished inserting initial datasets.");
        }
    }

    public static void getMetadataIndexes(List<IMetadataIndex> outIndexes) {
        Collections.addAll(outIndexes, PRIMARY_INDEXES);
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
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(MetadataConstants.SYSTEM_DATABASE,
                    MetadataConstants.METADATA_DATAVERSE_NAME, types.get(i).getTypeName(), types.get(i), false));
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
        if (MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, ConcurrentMergePolicyFactory.NAME) == null) {
            CompactionPolicy compactionPolicy = getCompactionPolicyEntity(ConcurrentMergePolicyFactory.class.getName());
            MetadataManager.INSTANCE.addCompactionPolicy(mdTxnCtx, compactionPolicy);
        }
    }

    private static void insertSynonymEntitiesIfNotExist(MetadataTransactionContext mdTxnCtx,
            MetadataIndexesProvider mdIndexesProvider) throws AlgebricksException {
        IAType synonymDatasetRecordType = mdIndexesProvider.getSynonymEntity().getRecordType();
        if (MetadataManager.INSTANCE.getDatatype(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, synonymDatasetRecordType.getTypeName()) == null) {
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                    new Datatype(MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME,
                            synonymDatasetRecordType.getTypeName(), synonymDatasetRecordType, false));
        }
        if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, MetadataConstants.SYNONYM_DATASET_NAME) == null) {
            insertMetadataDatasets(mdTxnCtx, new IMetadataIndex[] { mdIndexesProvider.getSynonymEntity().getIndex() });
        }
    }

    // For backward-compatibility: for old datasets created by an older version of AsterixDB, they
    // 1) may not have such a full-text config dataset in the metadata catalog,
    // 2) may not have the default full-text config as an entry in the metadata catalog
    // So here, let's try to insert if not exists
    private static void insertFullTextConfigAndFilterIfNotExist(MetadataTransactionContext mdTxnCtx,
            MetadataIndexesProvider metadataIndexesProvider) throws AlgebricksException {

        // We need to insert data types first because datasets depend on data types
        // ToDo: create a new function to reduce duplicated code here: addDatatypeIfNotExist()
        IAType fullTextConfigRecordType = metadataIndexesProvider.getFullTextConfigEntity().getRecordType();
        if (MetadataManager.INSTANCE.getDatatype(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, fullTextConfigRecordType.getTypeName()) == null) {
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                    new Datatype(MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME,
                            fullTextConfigRecordType.getTypeName(), fullTextConfigRecordType, false));
        }
        IAType fullTextFilterRecordType = metadataIndexesProvider.getFullTextFilterEntity().getRecordType();
        if (MetadataManager.INSTANCE.getDatatype(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, fullTextFilterRecordType.getTypeName()) == null) {
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                    new Datatype(MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME,
                            fullTextFilterRecordType.getTypeName(), fullTextFilterRecordType, false));
        }

        if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, MetadataConstants.FULL_TEXT_CONFIG_DATASET_NAME) == null) {
            insertMetadataDatasets(mdTxnCtx,
                    new IMetadataIndex[] { metadataIndexesProvider.getFullTextConfigEntity().getIndex() });
        }
        if (MetadataManager.INSTANCE.getDataset(mdTxnCtx, MetadataConstants.SYSTEM_DATABASE,
                MetadataConstants.METADATA_DATAVERSE_NAME, MetadataConstants.FULL_TEXT_FILTER_DATASET_NAME) == null) {
            insertMetadataDatasets(mdTxnCtx,
                    new IMetadataIndex[] { metadataIndexesProvider.getFullTextFilterEntity().getIndex() });
        }
    }

    private static DatasourceAdapter getAdapter(String adapterFactoryClassName) throws AlgebricksException {
        try {
            String adapterName =
                    ((ITypedAdapterFactory) (Class.forName(adapterFactoryClassName).newInstance())).getAlias();
            return new DatasourceAdapter(
                    new AdapterIdentifier(MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME,
                            adapterName),
                    IDataSourceAdapter.AdapterType.INTERNAL, adapterFactoryClassName, null, null, null);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new MetadataException("Unable to instantiate builtin Adapter", e);
        }
    }

    private static CompactionPolicy getCompactionPolicyEntity(String compactionPolicyClassName)
            throws AlgebricksException {
        try {
            String policyName =
                    ((ILSMMergePolicyFactory) (Class.forName(compactionPolicyClassName).newInstance())).getName();
            return new CompactionPolicy(MetadataConstants.SYSTEM_DATABASE, MetadataConstants.METADATA_DATAVERSE_NAME,
                    policyName, compactionPolicyClassName);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new MetadataException("Unable to instantiate builtin Merge Policy Factory", e);
        }
    }

    /**
     * Enlist a metadata index so it is available for metadata operations should be performed upon bootstrapping
     *
     * @param index
     * @param mdIndexesProvider
     * @throws HyracksDataException
     */
    public static void enlistMetadataDataset(INCServiceContext ncServiceCtx, IMetadataIndex index,
            MetadataIndexesProvider mdIndexesProvider) throws HyracksDataException {
        final int datasetId = index.getDatasetId().getId();
        INamespacePathResolver namespacePathResolver =
                ((INcApplicationContext) ncServiceCtx.getApplicationContext()).getNamespacePathResolver();
        String metadataPartitionPath =
                StoragePathUtil.prepareStoragePartitionPath(MetadataNode.INSTANCE.getMetadataStoragePartition());
        String resourceName =
                metadataPartitionPath + File.separator + index.getFileNameRelativePath(namespacePathResolver);
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
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = appContext.isCloudDeployment()
                ? new AtomicLSMIndexIOOperationCallbackFactory(idGeneratorProvider, datasetInfoProvider)
                : new LSMIndexIOOperationCallbackFactory(idGeneratorProvider, datasetInfoProvider);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = new LSMIndexPageWriteCallbackFactory();

        IStorageComponentProvider storageComponentProvider = appContext.getStorageComponentProvider();
        boolean createMetadataDataset;
        LocalResource resource;
        if (isNewUniverse()) {
            resource = null;
            createMetadataDataset = true;
        } else {
            resource = localResourceRepository.get(file.getRelativePath());
            createMetadataDataset = resource == null;
            if (createMetadataDataset) {
                ensureCatalogUpgradability(index, mdIndexesProvider);
            }
        }
        if (createMetadataDataset) {
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
                            bloomFilterFalsePositiveRate, true, null, NoOpCompressorDecompressorFactory.INSTANCE, true,
                            TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE,
                            false, appContext.isCloudDeployment());
            DatasetLocalResourceFactory dsLocalResourceFactory =
                    new DatasetLocalResourceFactory(datasetId, lsmBtreeFactory);
            // TODO(amoudi) Creating the index should be done through the same code path as
            // other indexes
            // This is to be done by having a metadata dataset associated with each index
            IIndexBuilder indexBuilder = new IndexBuilder(ncServiceCtx, storageComponentProvider.getStorageManager(),
                    index::getResourceId, file, dsLocalResourceFactory, true);
            indexBuilder.build();
        } else {
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
        //TODO(DB): include database in recovery

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
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverse.getDatabaseName(), dataverse.getDataverseName());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Dropped a pending dataverse: " + dataverse.getDataverseName());
            }
        } else {
            List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx,
                    dataverse.getDatabaseName(), dataverse.getDataverseName());
            for (Dataset dataset : datasets) {
                recoverDataset(mdTxnCtx, dataset);
            }

            List<Library> libraries = MetadataManager.INSTANCE.getDataverseLibraries(mdTxnCtx,
                    dataverse.getDatabaseName(), dataverse.getDataverseName());
            for (Library library : libraries) {
                recoverLibrary(mdTxnCtx, library);
            }
        }
    }

    private static void recoverDataset(MetadataTransactionContext mdTxnCtx, Dataset dataset)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.VIEW) {
            // Views don't need any recovery and cannot be in a pending state
            return;
        }
        if (dataset.getPendingOp() != MetadataUtil.PENDING_NO_OP) {
            // drop pending dataset
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataset.getDatabaseName(), dataset.getDataverseName(),
                    dataset.getDatasetName(), true);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(
                        "Dropped a pending dataset: " + dataset.getDataverseName() + "." + dataset.getDatasetName());
            }
        } else {
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName());
            for (Index index : indexes) {
                if (index.getPendingOp() != MetadataUtil.PENDING_NO_OP) {
                    // drop pending index
                    MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataset.getDatabaseName(), dataset.getDataverseName(),
                            dataset.getDatasetName(), index.getIndexName());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Dropped a pending index: " + dataset.getDataverseName() + "."
                                + dataset.getDatasetName() + "." + index.getIndexName());
                    }
                }
            }
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // if the dataset has no indexes, delete all its files
            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName());
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

    private static void recoverLibrary(MetadataTransactionContext mdTxnCtx, Library library)
            throws AlgebricksException {
        if (library.getPendingOp() != MetadataUtil.PENDING_NO_OP) {
            // drop pending library
            MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, library.getDatabaseName(), library.getDataverseName(),
                    library.getName());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Dropped a pending library: " + library.getDataverseName() + "." + library.getName());
            }
        }
    }

    public static boolean isNewUniverse() {
        return isNewUniverse;
    }

    public static void setNewUniverse(boolean isNewUniverse) {
        MetadataBootstrap.isNewUniverse = isNewUniverse;
    }

    private static void ensureCatalogUpgradability(IMetadataIndex index, MetadataIndexesProvider mdIndexesProvider) {
        if (index != mdIndexesProvider.getSynonymEntity().getIndex()
                // Backward-compatibility: FULLTEXT_ENTITY_DATASET is added to AsterixDB recently
                // and may not exist in an older dataverse
                && index != mdIndexesProvider.getFullTextConfigEntity().getIndex()
                && index != mdIndexesProvider.getFullTextFilterEntity().getIndex()) {
            throw new IllegalStateException(
                    "attempt to create metadata index " + index.getIndexName() + ". Index should already exist");
        }
    }
}
