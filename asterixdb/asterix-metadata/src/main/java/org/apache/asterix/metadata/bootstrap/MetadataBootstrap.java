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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.ILocalResourceMetadata;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.external.adapter.factory.GenericAdapterFactory;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.metadata.IDatasetDetails;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.metadata.entities.AsterixBuiltinTypeMap;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import org.apache.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.metadata.entities.NodeGroup;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;

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
    private static final Logger LOGGER = Logger.getLogger(MetadataBootstrap.class.getName());
    private static IAsterixAppRuntimeContext runtimeContext;
    private static IBufferCache bufferCache;
    private static IFileMapProvider fileMapProvider;
    private static IDatasetLifecycleManager dataLifecycleManager;
    private static ILocalResourceRepository localResourceRepository;
    private static IIOManager ioManager;
    private static String metadataNodeName;
    private static List<String> nodeNames;
    private static String outputDir;
    private static boolean isNewUniverse;
    private static final IMetadataIndex[] PRIMARY_INDEXES =
            new IMetadataIndex[] { MetadataPrimaryIndexes.DATAVERSE_DATASET, MetadataPrimaryIndexes.DATASET_DATASET,
                    MetadataPrimaryIndexes.DATATYPE_DATASET, MetadataPrimaryIndexes.INDEX_DATASET,
                    MetadataPrimaryIndexes.NODE_DATASET, MetadataPrimaryIndexes.NODEGROUP_DATASET,
                    MetadataPrimaryIndexes.FUNCTION_DATASET, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET,
                    MetadataPrimaryIndexes.FEED_DATASET, MetadataPrimaryIndexes.FEED_POLICY_DATASET,
                    MetadataPrimaryIndexes.LIBRARY_DATASET, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET,
                    MetadataPrimaryIndexes.EXTERNAL_FILE_DATASET };

    private static IAsterixPropertiesProvider propertiesProvider;

    private MetadataBootstrap() {
    }

    /**
     * bootstrap metadata
     *
     * @param asterixPropertiesProvider
     * @param ncApplicationContext
     * @param isNewUniverse
     * @throws ACIDException
     * @throws RemoteException
     * @throws MetadataException
     * @throws Exception
     */
    public static void startUniverse(IAsterixPropertiesProvider asterixPropertiesProvider,
            INCApplicationContext ncApplicationContext, boolean isNewUniverse)
            throws RemoteException, ACIDException, MetadataException {
        MetadataBootstrap.setNewUniverse(isNewUniverse);
        runtimeContext = (IAsterixAppRuntimeContext) ncApplicationContext.getApplicationObject();
        propertiesProvider = asterixPropertiesProvider;

        AsterixMetadataProperties metadataProperties = propertiesProvider.getMetadataProperties();
        metadataNodeName = metadataProperties.getMetadataNodeName();
        nodeNames = metadataProperties.getNodeNames();
        dataLifecycleManager = runtimeContext.getDatasetLifecycleManager();
        localResourceRepository = runtimeContext.getLocalResourceRepository();
        bufferCache = runtimeContext.getBufferCache();
        fileMapProvider = runtimeContext.getFileMapManager();
        ioManager = ncApplicationContext.getRootContext().getIOManager();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            // Begin a transaction against the metadata.
            // Lock the metadata in X mode.
            MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.X);

            for (int i = 0; i < PRIMARY_INDEXES.length; i++) {
                enlistMetadataDataset(PRIMARY_INDEXES[i]);
            }
            if (LOGGER.isLoggable(Level.INFO)) {
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
                insertInitialFeedPolicies(mdTxnCtx);
                insertInitialCompactionPolicies(mdTxnCtx);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Finished creating metadata B-trees.");
                }
            }
            // #. initialize datasetIdFactory
            MetadataManager.INSTANCE.initializeDatasetIdFactory(mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            try {
                if (IS_DEBUG_MODE) {
                    LOGGER.log(Level.SEVERE, "Failure during metadata bootstrap", e);
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

    private static void insertInitialDataverses(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        String dataFormat = NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT;
        MetadataManager.INSTANCE.addDataverse(mdTxnCtx,
                new Dataverse(MetadataConstants.METADATA_DATAVERSE_NAME, dataFormat, IMetadataEntity.PENDING_NO_OP));
        MetadataManager.INSTANCE.addDataverse(mdTxnCtx, MetadataBuiltinEntities.DEFAULT_DATAVERSE);
    }

    /**
     * Inserts a metadata dataset to the physical dataset index
     * Should be performed on a bootstrap of a new universe
     *
     * @param mdTxnCtx
     * @param indexes
     * @throws MetadataException
     */
    public static void insertMetadataDatasets(MetadataTransactionContext mdTxnCtx, IMetadataIndex[] indexes)
            throws MetadataException {
        for (int i = 0; i < indexes.length; i++) {
            IDatasetDetails id = new InternalDatasetDetails(FileStructure.BTREE, PartitioningStrategy.HASH,
                    indexes[i].getPartitioningExpr(), indexes[i].getPartitioningExpr(), null,
                    indexes[i].getPartitioningExprType(), false, null, false);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx,
                    new Dataset(indexes[i].getDataverseName(), indexes[i].getIndexedDatasetName(),
                            indexes[i].getDataverseName(), indexes[i].getPayloadRecordType().getTypeName(),
                            indexes[i].getNodeGroupName(), GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME,
                            GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES, id, new HashMap<String, String>(),
                            DatasetType.INTERNAL, indexes[i].getDatasetId().getId(),
                            IMetadataEntity.PENDING_NO_OP));
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished inserting initial datasets.");
        }
    }

    private static void getBuiltinTypes(List<IAType> types) {
        Collection<BuiltinType> builtinTypes = AsterixBuiltinTypeMap.getBuiltinTypes().values();
        Iterator<BuiltinType> iter = builtinTypes.iterator();
        while (iter.hasNext()) {
            types.add(iter.next());
        }
    }

    private static void getMetadataTypes(ArrayList<IAType> types) {
        for (int i = 0; i < PRIMARY_INDEXES.length; i++) {
            types.add(PRIMARY_INDEXES[i].getPayloadRecordType());
        }
    }

    private static void insertMetadataDatatypes(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        ArrayList<IAType> types = new ArrayList<>();
        getBuiltinTypes(types);
        getMetadataTypes(types);
        for (int i = 0; i < types.size(); i++) {
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                    new Datatype(MetadataConstants.METADATA_DATAVERSE_NAME, types.get(i).getTypeName(), types.get(i),
                            false));
        }
        MetadataManager.INSTANCE.addDatatype(mdTxnCtx,
                MetadataBuiltinEntities.ANY_OBJECT_DATATYPE);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished inserting initial datatypes.");
        }
    }

    private static void insertNodes(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        for (String nodesName : nodeNames) {
            MetadataManager.INSTANCE.addNode(mdTxnCtx, new Node(nodesName, 0, 0));
        }
    }

    private static void insertInitialGroups(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        List<String> metadataGroupNodeNames = new ArrayList<>();
        metadataGroupNodeNames.add(metadataNodeName);
        NodeGroup groupRecord = new NodeGroup(MetadataConstants.METADATA_NODEGROUP_NAME, metadataGroupNodeNames);
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, groupRecord);
        List<String> nodes = new ArrayList<>();
        nodes.addAll(nodeNames);
        NodeGroup defaultGroup = new NodeGroup(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME, nodes);
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, defaultGroup);
    }

    private static void insertInitialAdapters(MetadataTransactionContext mdTxnCtx)
            throws MetadataException {
        String[] builtInAdapterClassNames = new String[] { GenericAdapterFactory.class.getName() };
        DatasourceAdapter adapter;
        for (String adapterClassName : builtInAdapterClassNames) {
            adapter = getAdapter(adapterClassName);
            MetadataManager.INSTANCE.addAdapter(mdTxnCtx, adapter);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished inserting built-in adapters.");
        }
    }

    private static void insertInitialFeedPolicies(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        for (FeedPolicyEntity feedPolicy : BuiltinFeedPolicies.policies) {
            MetadataManager.INSTANCE.addFeedPolicy(mdTxnCtx, feedPolicy);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Finished adding built-in feed policies.");
        }
    }

    private static void insertInitialCompactionPolicies(MetadataTransactionContext mdTxnCtx) throws MetadataException {
        String[] builtInCompactionPolicyClassNames =
                new String[] { ConstantMergePolicyFactory.class.getName(), PrefixMergePolicyFactory.class.getName(),
                        NoMergePolicyFactory.class.getName(), CorrelatedPrefixMergePolicyFactory.class.getName() };
        for (String policyClassName : builtInCompactionPolicyClassNames) {
            CompactionPolicy compactionPolicy = getCompactionPolicyEntity(policyClassName);
            MetadataManager.INSTANCE.addCompactionPolicy(mdTxnCtx, compactionPolicy);
        }
    }

    private static DatasourceAdapter getAdapter(String adapterFactoryClassName)
            throws MetadataException {
        try {
            String adapterName = ((IAdapterFactory) (Class.forName(adapterFactoryClassName).newInstance())).getAlias();
            return new DatasourceAdapter(new AdapterIdentifier(MetadataConstants.METADATA_DATAVERSE_NAME, adapterName),
                    adapterFactoryClassName, IDataSourceAdapter.AdapterType.INTERNAL);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new MetadataException("Unable to instantiate builtin Adapter", e);
        }
    }

    private static CompactionPolicy getCompactionPolicyEntity(String compactionPolicyClassName)
            throws MetadataException {
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
     * Enlist a metadata index so it is available for metadata operations
     * should be performed upon bootstrapping
     *
     * @param index
     * @throws HyracksDataException
     */
    public static void enlistMetadataDataset(IMetadataIndex index) throws HyracksDataException {
        ClusterPartition metadataPartition = propertiesProvider.getMetadataProperties().getMetadataPartition();
        int metadataDeviceId = metadataPartition.getIODeviceNum();
        String metadataPartitionPath = StoragePathUtil.prepareStoragePartitionPath(
                ClusterProperties.INSTANCE.getStorageDirectoryName(), metadataPartition.getPartitionId());
        String resourceName = metadataPartitionPath + File.separator + index.getFileNameRelativePath();
        FileReference file = ioManager.getAbsoluteFileRef(metadataDeviceId, resourceName);

        // this should not be done this way. dataset lifecycle manager shouldn't return virtual buffer caches for
        // a dataset that was not yet created
        List<IVirtualBufferCache> virtualBufferCaches = runtimeContext.getDatasetLifecycleManager()
                .getVirtualBufferCaches(index.getDatasetId().getId(), metadataPartition.getIODeviceNum());
        ITypeTraits[] typeTraits = index.getTypeTraits();
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        int[] bloomFilterKeyFields = index.getBloomFilterKeyFields();
        LSMBTree lsmBtree;
        long resourceID;
        ILSMOperationTracker opTracker =
                index.isPrimaryIndex() ? runtimeContext.getLSMBTreeOperationTracker(index.getDatasetId().getId())
                        : new BaseOperationTracker(index.getDatasetId().getId(),
                                dataLifecycleManager.getDatasetInfo(index.getDatasetId().getId()));
        final String absolutePath = file.getFile().getPath();
        if (isNewUniverse()) {
            lsmBtree = LSMBTreeUtils.createLSMTree(virtualBufferCaches, file, bufferCache, fileMapProvider, typeTraits,
                    comparatorFactories, bloomFilterKeyFields, runtimeContext.getBloomFilterFalsePositiveRate(),
                    runtimeContext.getMetadataMergePolicyFactory()
                            .createMergePolicy(GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES, dataLifecycleManager),
                    opTracker, runtimeContext.getLSMIOScheduler(),
                    LSMBTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), index.isPrimaryIndex(),
                    null, null, null, null, true);
            lsmBtree.create();
            resourceID = index.getResourceID();
            ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(typeTraits,
                    comparatorFactories, bloomFilterKeyFields, index.isPrimaryIndex(), index.getDatasetId().getId(),
                    runtimeContext.getMetadataMergePolicyFactory(), GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES,
                    null, null, null, null);
            ILocalResourceFactoryProvider localResourceFactoryProvider =
                    new PersistentLocalResourceFactoryProvider(localResourceMetadata, LocalResource.LSMBTreeResource);
            ILocalResourceFactory localResourceFactory = localResourceFactoryProvider.getLocalResourceFactory();
            localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, resourceName,
                    metadataPartition.getPartitionId(), LIFOMetaDataFrame.VERSION, absolutePath));
            dataLifecycleManager.register(absolutePath, lsmBtree);
        } else {
            final LocalResource resource = localResourceRepository.getResourceByPath(absolutePath);
            if (resource == null) {
                throw new HyracksDataException("Could not find required metadata indexes. Please delete "
                        + propertiesProvider.getMetadataProperties().getTransactionLogDirs()
                                .get(runtimeContext.getTransactionSubsystem().getId())
                        + " to intialize as a new instance. (WARNING: all data will be lost.)");
            }
            resourceID = resource.getResourceId();
            lsmBtree = (LSMBTree) dataLifecycleManager.get(absolutePath);
            if (lsmBtree == null) {
                lsmBtree = LSMBTreeUtils.createLSMTree(virtualBufferCaches, file, bufferCache, fileMapProvider,
                        typeTraits, comparatorFactories, bloomFilterKeyFields,
                        runtimeContext.getBloomFilterFalsePositiveRate(),
                        runtimeContext.getMetadataMergePolicyFactory().createMergePolicy(
                                GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES, dataLifecycleManager),
                        opTracker, runtimeContext.getLSMIOScheduler(),
                        LSMBTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(), index.isPrimaryIndex(),
                        null, null, null, null, true);
                dataLifecycleManager.register(absolutePath, lsmBtree);
            }
        }
        index.setResourceID(resourceID);
        index.setFile(file);
    }

    public static String getOutputDir() {
        return outputDir;
    }

    public static String getMetadataNodeName() {
        return metadataNodeName;
    }

    /**
     * Perform recovery of DDL operations metadata records
     */
    public static void startDDLRecovery() throws MetadataException {
        // #. clean up any record which has pendingAdd/DelOp flag
        // as traversing all records from DATAVERSE_DATASET to DATASET_DATASET, and then to INDEX_DATASET.
        MetadataTransactionContext mdTxnCtx = null;
        MetadataManager.INSTANCE.acquireWriteLatch();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting DDL recovery ...");
        }

        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
            for (Dataverse dataverse : dataverses) {
                recoverDataverse(mdTxnCtx, dataverse);
            }
            // the commit wasn't there before. yet, everything was working correctly!!!!!!!!!!!
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Completed DDL recovery.");
            }
        } catch (Exception e) {
            try {
                if (IS_DEBUG_MODE) {
                    LOGGER.log(Level.SEVERE, "Failure during DDL recovery", e);
                }
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            throw new MetadataException(e);
        } finally {
            MetadataManager.INSTANCE.releaseWriteLatch();
        }
    }

    private static void recoverDataverse(MetadataTransactionContext mdTxnCtx, Dataverse dataverse)
            throws MetadataException {
        if (dataverse.getPendingOp() != IMetadataEntity.PENDING_NO_OP) {
            // drop pending dataverse
            MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverse.getDataverseName());
            if (LOGGER.isLoggable(Level.INFO)) {
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

    private static void recoverDataset(MetadataTransactionContext mdTxnCtx, Dataset dataset) throws MetadataException {
        if (dataset.getPendingOp() != IMetadataEntity.PENDING_NO_OP) {
            // drop pending dataset
            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName());
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(
                        "Dropped a pending dataset: " + dataset.getDataverseName() + "." + dataset.getDatasetName());
            }
        } else {
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDataverseName(),
                            dataset.getDatasetName());
            for (Index index : indexes) {
                if (index.getPendingOp() != IMetadataEntity.PENDING_NO_OP) {
                    // drop pending index
                    MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataset.getDataverseName(), dataset.getDatasetName(),
                            index.getIndexName());
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Dropped a pending index: " + dataset.getDataverseName() + "."
                                + dataset.getDatasetName()
                                + "." + index.getIndexName());
                    }
                }
            }
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // if the dataset has no indexes, delete all its files
            List<Index> indexes =
                    MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDataverseName(),
                            dataset.getDatasetName());
            if (indexes.isEmpty()) {
                List<ExternalFile> files =
                        MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                for (ExternalFile file : files) {
                    MetadataManager.INSTANCE.dropExternalFile(mdTxnCtx, file);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Dropped an external file: " + dataset.getDataverseName() + "."
                                + dataset.getDatasetName()
                                + "." + file.getFileNumber());
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
