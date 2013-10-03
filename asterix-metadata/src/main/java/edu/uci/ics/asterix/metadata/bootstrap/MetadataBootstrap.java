/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.bootstrap;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.api.ILocalResourceMetadata;
import edu.uci.ics.asterix.common.config.AsterixMetadataProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.config.IAsterixPropertiesProvider;
import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.external.adapter.factory.IAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.AdapterIdentifier;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.CompactionPolicy;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails.FileStructure;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails.PartitioningStrategy;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.util.IndexFileNameUtil;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;

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
    private static final Logger LOGGER = Logger.getLogger(MetadataBootstrap.class.getName());
    public static final boolean IS_DEBUG_MODE = false;//true

    private static IAsterixAppRuntimeContext runtimeContext;

    private static IBufferCache bufferCache;
    private static IFileMapProvider fileMapProvider;
    private static IIndexLifecycleManager indexLifecycleManager;
    private static ILocalResourceRepository localResourceRepository;
    private static IIOManager ioManager;

    private static String metadataNodeName;
    private static String metadataStore;
    private static Set<String> nodeNames;
    private static String outputDir;

    private static IMetadataIndex[] primaryIndexes;
    private static IMetadataIndex[] secondaryIndexes;

    private static IAsterixPropertiesProvider propertiesProvider;

    private static void initLocalIndexArrays() {
        primaryIndexes = new IMetadataIndex[] { MetadataPrimaryIndexes.DATAVERSE_DATASET,
                MetadataPrimaryIndexes.DATASET_DATASET, MetadataPrimaryIndexes.DATATYPE_DATASET,
                MetadataPrimaryIndexes.INDEX_DATASET, MetadataPrimaryIndexes.NODE_DATASET,
                MetadataPrimaryIndexes.NODEGROUP_DATASET, MetadataPrimaryIndexes.FUNCTION_DATASET,
                MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET, MetadataPrimaryIndexes.COMPACTION_POLICY_DATASET };
        secondaryIndexes = new IMetadataIndex[] { MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX,
                MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX,
                MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX };
    }

    public static void startUniverse(IAsterixPropertiesProvider asterixPropertiesProvider,
            INCApplicationContext ncApplicationContext, boolean isNewUniverse) throws Exception {
        runtimeContext = (IAsterixAppRuntimeContext) ncApplicationContext.getApplicationObject();
        propertiesProvider = asterixPropertiesProvider;

        // Initialize static metadata objects, such as record types and metadata
        // index descriptors.
        // The order of these calls is important because the index descriptors
        // rely on the type type descriptors.
        MetadataRecordTypes.init();
        MetadataPrimaryIndexes.init();
        MetadataSecondaryIndexes.init();
        initLocalIndexArrays();

        AsterixMetadataProperties metadataProperties = propertiesProvider.getMetadataProperties();
        metadataNodeName = metadataProperties.getMetadataNodeName();
        metadataStore = metadataProperties.getMetadataStore();
        nodeNames = metadataProperties.getNodeNames();
        // nodeStores = asterixProperity.getStores();

        indexLifecycleManager = runtimeContext.getIndexLifecycleManager();
        localResourceRepository = runtimeContext.getLocalResourceRepository();
        bufferCache = runtimeContext.getBufferCache();
        fileMapProvider = runtimeContext.getFileMapManager();
        ioManager = ncApplicationContext.getRootContext().getIOManager();

        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            // Begin a transaction against the metadata.
            // Lock the metadata in X mode.
            MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.X);

            if (isNewUniverse) {
                for (int i = 0; i < primaryIndexes.length; i++) {
                    enlistMetadataDataset(primaryIndexes[i], true, mdTxnCtx);
                }
                for (int i = 0; i < secondaryIndexes.length; i++) {
                    enlistMetadataDataset(secondaryIndexes[i], true, mdTxnCtx);
                }
                insertInitialDataverses(mdTxnCtx);
                insertInitialDatasets(mdTxnCtx);
                insertInitialDatatypes(mdTxnCtx);
                insertInitialIndexes(mdTxnCtx);
                insertNodes(mdTxnCtx);
                insertInitialGroups(mdTxnCtx);
                insertInitialAdapters(mdTxnCtx);
                insertInitialCompactionPolicies(mdTxnCtx);

                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Finished creating metadata B-trees.");
                }
            } else {
                for (int i = 0; i < primaryIndexes.length; i++) {
                    enlistMetadataDataset(primaryIndexes[i], false, mdTxnCtx);
                }
                for (int i = 0; i < secondaryIndexes.length; i++) {
                    enlistMetadataDataset(secondaryIndexes[i], false, mdTxnCtx);
                }

                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Finished enlistment of metadata B-trees.");
                }
            }

            //#. initialize datasetIdFactory
            MetadataManager.INSTANCE.initializeDatasetIdFactory(mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            try {
                if (IS_DEBUG_MODE) {
                    e.printStackTrace();
                }
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } catch (Exception e2) {
                e.addSuppressed(e2);
                //TODO
                //change the exception type to AbortFailureException
                throw new MetadataException(e);
            }
            throw e;
        }
    }

    public static void stopUniverse() throws HyracksDataException {
        // Close all BTree files in BufferCache.
        for (int i = 0; i < primaryIndexes.length; i++) {
            long resourceID = localResourceRepository
                    .getResourceByName(primaryIndexes[i].getFile().getFile().getPath()).getResourceId();
            indexLifecycleManager.close(resourceID);
            indexLifecycleManager.unregister(resourceID);
        }
        for (int i = 0; i < secondaryIndexes.length; i++) {
            long resourceID = localResourceRepository.getResourceByName(
                    secondaryIndexes[i].getFile().getFile().getPath()).getResourceId();
            indexLifecycleManager.close(resourceID);
            indexLifecycleManager.unregister(resourceID);
        }
    }

    public static void insertInitialDataverses(MetadataTransactionContext mdTxnCtx) throws Exception {
        String dataverseName = MetadataPrimaryIndexes.DATAVERSE_DATASET.getDataverseName();
        String dataFormat = NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT;
        MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverseName, dataFormat,
                IMetadataEntity.PENDING_NO_OP));
    }

    public static void insertInitialDatasets(MetadataTransactionContext mdTxnCtx) throws Exception {
        for (int i = 0; i < primaryIndexes.length; i++) {
            IDatasetDetails id = new InternalDatasetDetails(FileStructure.BTREE, PartitioningStrategy.HASH,
                    primaryIndexes[i].getPartitioningExpr(), primaryIndexes[i].getPartitioningExpr(),
                    primaryIndexes[i].getNodeGroupName(), GlobalConfig.DEFAULT_COMPACTION_POLICY_NAME,
                    GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES);
            MetadataManager.INSTANCE.addDataset(mdTxnCtx, new Dataset(primaryIndexes[i].getDataverseName(),
                    primaryIndexes[i].getIndexedDatasetName(), primaryIndexes[i].getPayloadRecordType().getTypeName(),
                    id, new HashMap<String, String>(), DatasetType.INTERNAL, primaryIndexes[i].getDatasetId().getId(),
                    IMetadataEntity.PENDING_NO_OP));
        }
    }

    public static void getBuiltinTypes(ArrayList<IAType> types) throws Exception {
        Collection<BuiltinType> builtinTypes = AsterixBuiltinTypeMap.getBuiltinTypes().values();
        Iterator<BuiltinType> iter = builtinTypes.iterator();
        while (iter.hasNext())
            types.add(iter.next());
    }

    public static void getMetadataTypes(ArrayList<IAType> types) throws Exception {
        for (int i = 0; i < primaryIndexes.length; i++)
            types.add(primaryIndexes[i].getPayloadRecordType());
    }

    public static void insertInitialDatatypes(MetadataTransactionContext mdTxnCtx) throws Exception {
        String dataverseName = MetadataPrimaryIndexes.DATAVERSE_DATASET.getDataverseName();
        ArrayList<IAType> types = new ArrayList<IAType>();
        getBuiltinTypes(types);
        getMetadataTypes(types);
        for (int i = 0; i < types.size(); i++) {
            MetadataManager.INSTANCE.addDatatype(mdTxnCtx, new Datatype(dataverseName, types.get(i).getTypeName(),
                    types.get(i), false));
        }
    }

    public static void insertInitialIndexes(MetadataTransactionContext mdTxnCtx) throws Exception {
        for (int i = 0; i < secondaryIndexes.length; i++) {
            MetadataManager.INSTANCE.addIndex(mdTxnCtx, new Index(secondaryIndexes[i].getDataverseName(),
                    secondaryIndexes[i].getIndexedDatasetName(), secondaryIndexes[i].getIndexName(), IndexType.BTREE,
                    secondaryIndexes[i].getPartitioningExpr(), false, IMetadataEntity.PENDING_NO_OP));
        }
    }

    public static void insertNodes(MetadataTransactionContext mdTxnCtx) throws Exception {
        Iterator<String> iter = nodeNames.iterator();
        // Set<Entry<String, String[]>> set = nodeStores.entrySet();
        // Iterator<Entry<String, String[]>> im = set.iterator();

        while (iter.hasNext()) {
            // Map.Entry<String, String[]> me = (Map.Entry<String,
            // String[]>)im.next();
            MetadataManager.INSTANCE.addNode(mdTxnCtx, new Node(iter.next(), 0, 0/*
                                                                                 * , me . getValue ( )
                                                                                 */));
        }
    }

    public static void insertInitialGroups(MetadataTransactionContext mdTxnCtx) throws Exception {
        String groupName = MetadataPrimaryIndexes.DATAVERSE_DATASET.getNodeGroupName();
        List<String> metadataGroupNodeNames = new ArrayList<String>();
        metadataGroupNodeNames.add(metadataNodeName);
        NodeGroup groupRecord = new NodeGroup(groupName, metadataGroupNodeNames);
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, groupRecord);

        List<String> nodes = new ArrayList<String>();
        nodes.addAll(nodeNames);
        NodeGroup defaultGroup = new NodeGroup(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME, nodes);
        MetadataManager.INSTANCE.addNodegroup(mdTxnCtx, defaultGroup);

    }

    private static void insertInitialAdapters(MetadataTransactionContext mdTxnCtx) throws Exception {
        String[] builtInAdapterClassNames = new String[] {
                "edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory",
                "edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory",
                "edu.uci.ics.asterix.external.adapter.factory.HiveAdapterFactory",
                "edu.uci.ics.asterix.external.adapter.factory.PullBasedTwitterAdapterFactory",
                "edu.uci.ics.asterix.external.adapter.factory.RSSFeedAdapterFactory",
                "edu.uci.ics.asterix.external.adapter.factory.CNNFeedAdapterFactory", };
        DatasourceAdapter adapter;
        for (String adapterClassName : builtInAdapterClassNames) {
            adapter = getAdapter(adapterClassName);
            MetadataManager.INSTANCE.addAdapter(mdTxnCtx, adapter);
        }
    }

    private static void insertInitialCompactionPolicies(MetadataTransactionContext mdTxnCtx) throws Exception {
        String[] builtInCompactionPolicyClassNames = new String[] {
                "edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory",
                "edu.uci.ics.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory", };
        CompactionPolicy compactionPolicy;
        for (String policyClassName : builtInCompactionPolicyClassNames) {
            compactionPolicy = getCompactionPolicyEntity(policyClassName);
            MetadataManager.INSTANCE.addCompactionPolicy(mdTxnCtx, compactionPolicy);
        }
    }

    private static DatasourceAdapter getAdapter(String adapterFactoryClassName) throws Exception {
        String adapterName = ((IAdapterFactory) (Class.forName(adapterFactoryClassName).newInstance())).getName();
        return new DatasourceAdapter(new AdapterIdentifier(MetadataConstants.METADATA_DATAVERSE_NAME, adapterName),
                adapterFactoryClassName, DatasourceAdapter.AdapterType.INTERNAL);
    }

    private static CompactionPolicy getCompactionPolicyEntity(String compactionPolicyClassName) throws Exception {
        String policyName = ((ILSMMergePolicyFactory) (Class.forName(compactionPolicyClassName).newInstance()))
                .getName();
        return new CompactionPolicy(MetadataConstants.METADATA_DATAVERSE_NAME, policyName, compactionPolicyClassName);
    }

    private static void enlistMetadataDataset(IMetadataIndex index, boolean create, MetadataTransactionContext mdTxnCtx)
            throws Exception {
        String filePath = ioManager.getIODevices().get(runtimeContext.getMetaDataIODeviceId()).getPath()
                + File.separator
                + IndexFileNameUtil.prepareFileName(metadataStore + File.separator + index.getFileNameRelativePath(),
                        runtimeContext.getMetaDataIODeviceId());
        FileReference file = new FileReference(new File(filePath));
        List<IVirtualBufferCache> virtualBufferCaches = runtimeContext.getVirtualBufferCaches(index.getDatasetId()
                .getId());
        ITypeTraits[] typeTraits = index.getTypeTraits();
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        int[] bloomFilterKeyFields = index.getBloomFilterKeyFields();
        LSMBTree lsmBtree = null;
        long resourceID = -1;
        ILSMOperationTracker opTracker = index.isPrimaryIndex() ? runtimeContext.getLSMBTreeOperationTracker(index
                .getDatasetId().getId()) : new BaseOperationTracker((DatasetLifecycleManager) indexLifecycleManager,
                index.getDatasetId().getId());
        if (create) {
            lsmBtree = LSMBTreeUtils.createLSMTree(
                    virtualBufferCaches,
                    file,
                    bufferCache,
                    fileMapProvider,
                    typeTraits,
                    comparatorFactories,
                    bloomFilterKeyFields,
                    runtimeContext.getBloomFilterFalsePositiveRate(),
                    runtimeContext.getMetadataMergePolicyFactory().createMergePolicy(
                            GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES), opTracker,
                    runtimeContext.getLSMIOScheduler(),
                    LSMBTreeIOOperationCallbackFactory.INSTANCE.createIOOperationCallback());
            lsmBtree.create();
            resourceID = runtimeContext.getResourceIdFactory().createId();
            ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(typeTraits,
                    comparatorFactories, bloomFilterKeyFields, index.isPrimaryIndex(), index.getDatasetId().getId(),
                    runtimeContext.getMetadataMergePolicyFactory(), GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES);
            ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                    localResourceMetadata, LocalResource.LSMBTreeResource);
            ILocalResourceFactory localResourceFactory = localResourceFactoryProvider.getLocalResourceFactory();
            localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, file.getFile()
                    .getPath(), 0));
            indexLifecycleManager.register(resourceID, lsmBtree);
        } else {
            resourceID = localResourceRepository.getResourceByName(file.getFile().getPath()).getResourceId();
            lsmBtree = (LSMBTree) indexLifecycleManager.getIndex(resourceID);
            if (lsmBtree == null) {
                lsmBtree = LSMBTreeUtils.createLSMTree(virtualBufferCaches, file, bufferCache, fileMapProvider,
                        typeTraits, comparatorFactories, bloomFilterKeyFields, runtimeContext
                                .getBloomFilterFalsePositiveRate(), runtimeContext.getMetadataMergePolicyFactory()
                                .createMergePolicy(GlobalConfig.DEFAULT_COMPACTION_POLICY_PROPERTIES), opTracker,
                        runtimeContext.getLSMIOScheduler(), LSMBTreeIOOperationCallbackFactory.INSTANCE
                                .createIOOperationCallback());
                indexLifecycleManager.register(resourceID, lsmBtree);
            }
        }

        index.setResourceID(resourceID);
        index.setFile(file);
        indexLifecycleManager.open(resourceID);
    }

    public static String getOutputDir() {
        return outputDir;
    }

    public static String getMetadataNodeName() {
        return metadataNodeName;
    }

    public static void startDDLRecovery() throws RemoteException, ACIDException, MetadataException {
        //#. clean up any record which has pendingAdd/DelOp flag 
        //   as traversing all records from DATAVERSE_DATASET to DATASET_DATASET, and then to INDEX_DATASET.
        String dataverseName = null;
        String datasetName = null;
        String indexName = null;
        MetadataTransactionContext mdTxnCtx = null;

        MetadataManager.INSTANCE.acquireWriteLatch();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting DDL recovery ...");
        }

        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
            for (Dataverse dataverse : dataverses) {
                dataverseName = dataverse.getDataverseName();
                if (dataverse.getPendingOp() != IMetadataEntity.PENDING_NO_OP) {
                    //drop pending dataverse
                    MetadataManager.INSTANCE.dropDataverse(mdTxnCtx, dataverseName);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Dropped a pending dataverse: " + dataverseName);
                    }
                } else {
                    List<Dataset> datasets = MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dataverseName);
                    for (Dataset dataset : datasets) {
                        datasetName = dataset.getDatasetName();
                        if (dataset.getPendingOp() != IMetadataEntity.PENDING_NO_OP) {
                            //drop pending dataset
                            MetadataManager.INSTANCE.dropDataset(mdTxnCtx, dataverseName, datasetName);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Dropped a pending dataset: " + dataverseName + "." + datasetName);
                            }
                        } else {
                            List<Index> indexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                                    datasetName);
                            for (Index index : indexes) {
                                indexName = index.getIndexName();
                                if (index.getPendingOp() != IMetadataEntity.PENDING_NO_OP) {
                                    //drop pending index
                                    MetadataManager.INSTANCE.dropIndex(mdTxnCtx, dataverseName, datasetName, indexName);
                                    if (LOGGER.isLoggable(Level.INFO)) {
                                        LOGGER.info("Dropped a pending index: " + dataverseName + "." + datasetName
                                                + "." + indexName);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Completed DDL recovery.");
            }
        } catch (Exception e) {
            try {
                if (IS_DEBUG_MODE) {
                    e.printStackTrace();
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
}
