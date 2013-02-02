/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.context.AsterixRuntimeComponentsProvider;
import edu.uci.ics.asterix.external.adapter.factory.IAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.AdapterIdentifier;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
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
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.ILocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.LSMBTreeLocalResourceMetadata;
import edu.uci.ics.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceRepository;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager.ResourceType;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.HeapBufferAllocator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactory;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.LocalResource;
import edu.uci.ics.hyracks.storage.common.file.TransientFileMapManager;

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
    private static final int DEFAULT_MEM_PAGE_SIZE = 32768;
    private static final int DEFAULT_MEM_NUM_PAGES = 100;

    private static AsterixAppRuntimeContext runtimeContext;

    private static IBufferCache bufferCache;
    private static IFileMapProvider fileMapProvider;
    private static IIndexLifecycleManager indexLifecycleManager;
    private static ILocalResourceRepository localResourceRepository;
    private static IIOManager ioManager;

    private static String metadataNodeName;
    private static String metadataStore;
    private static HashSet<String> nodeNames;
    private static String outputDir;

    private static IMetadataIndex[] primaryIndexes;
    private static IMetadataIndex[] secondaryIndexes;

    private static void initLocalIndexArrays() {
        primaryIndexes = new IMetadataIndex[] { MetadataPrimaryIndexes.DATAVERSE_DATASET,
                MetadataPrimaryIndexes.DATASET_DATASET, MetadataPrimaryIndexes.DATATYPE_DATASET,
                MetadataPrimaryIndexes.INDEX_DATASET, MetadataPrimaryIndexes.NODE_DATASET,
                MetadataPrimaryIndexes.NODEGROUP_DATASET, MetadataPrimaryIndexes.FUNCTION_DATASET,
                MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET };
        secondaryIndexes = new IMetadataIndex[] { MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX,
                MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX,
                MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX };
    }

    public static void startUniverse(AsterixProperties asterixProperties, INCApplicationContext ncApplicationContext)
            throws Exception {
        runtimeContext = (AsterixAppRuntimeContext) ncApplicationContext.getApplicationObject();

        // Initialize static metadata objects, such as record types and metadata
        // index descriptors.
        // The order of these calls is important because the index descriptors
        // rely on the type type descriptors.
        MetadataRecordTypes.init();
        MetadataPrimaryIndexes.init();
        MetadataSecondaryIndexes.init();
        initLocalIndexArrays();

        boolean isNewUniverse = true;
        TransactionalResourceRepository resourceRepository = runtimeContext.getTransactionSubsystem()
                .getTransactionalResourceRepository();
        resourceRepository.registerTransactionalResourceManager(ResourceType.LSM_BTREE, new IndexResourceManager(
                ResourceType.LSM_BTREE, runtimeContext.getTransactionSubsystem()));
        resourceRepository.registerTransactionalResourceManager(ResourceType.LSM_RTREE, new IndexResourceManager(
                ResourceType.LSM_RTREE, runtimeContext.getTransactionSubsystem()));
        resourceRepository.registerTransactionalResourceManager(ResourceType.LSM_INVERTED_INDEX,
                new IndexResourceManager(ResourceType.LSM_INVERTED_INDEX, runtimeContext.getTransactionSubsystem()));

        metadataNodeName = asterixProperties.getMetadataNodeName();
        isNewUniverse = asterixProperties.isNewUniverse();
        metadataStore = asterixProperties.getMetadataStore();
        nodeNames = asterixProperties.getNodeNames();
        // nodeStores = asterixProperity.getStores();

        outputDir = asterixProperties.getOutputDir();
        if (outputDir != null) {
            (new File(outputDir)).mkdirs();
        }

        indexLifecycleManager = runtimeContext.getIndexLifecycleManager();
        localResourceRepository = runtimeContext.getLocalResourceRepository();
        bufferCache = runtimeContext.getBufferCache();
        fileMapProvider = runtimeContext.getFileMapManager();
        ioManager = ncApplicationContext.getRootContext().getIOManager();

        if (isNewUniverse) {
            //Do checkpoint only if it is new universe
            runtimeContext.getTransactionSubsystem().getRecoveryManager().checkpoint();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            try {
                // Begin a transaction against the metadata.
                // Lock the metadata in X mode.
                MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.X);
                
                for (int i = 0; i < primaryIndexes.length; i++) {
                    enlistMetadataDataset(primaryIndexes[i], true);
                    registerTransactionalResource(primaryIndexes[i], resourceRepository);
                }
                for (int i = 0; i < secondaryIndexes.length; i++) {
                    enlistMetadataDataset(secondaryIndexes[i], true);
                    registerTransactionalResource(secondaryIndexes[i], resourceRepository);
                }
                insertInitialDataverses(mdTxnCtx);
                insertInitialDatasets(mdTxnCtx);
                insertInitialDatatypes(mdTxnCtx);
                insertInitialIndexes(mdTxnCtx);
                insertNodes(mdTxnCtx);
                insertInitialGroups(mdTxnCtx);
                insertInitialAdapters(mdTxnCtx);
                
                MetadataManager.INSTANCE.initializeDatasetIdFactory(mdTxnCtx);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            } catch (Exception e) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                throw e;
            }
            LOGGER.info("FINISHED CREATING METADATA B-TREES.");
        } else {
            for (int i = 0; i < primaryIndexes.length; i++) {
                enlistMetadataDataset(primaryIndexes[i], false);
                registerTransactionalResource(primaryIndexes[i], resourceRepository);
            }
            for (int i = 0; i < secondaryIndexes.length; i++) {
                enlistMetadataDataset(secondaryIndexes[i], false);
                registerTransactionalResource(secondaryIndexes[i], resourceRepository);
            }
            LOGGER.info("FINISHED ENLISTMENT OF METADATA B-TREES.");
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

    private static void registerTransactionalResource(IMetadataIndex metadataIndex,
            TransactionalResourceRepository resourceRepository) throws ACIDException {
        long resourceId = metadataIndex.getResourceID();
        IIndex index = indexLifecycleManager.getIndex(resourceId);
        resourceRepository.registerTransactionalResource(resourceId, index);
        metadataIndex.initIndexLogger(index);
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
                    primaryIndexes[i].getNodeGroupName());
            MetadataManager.INSTANCE.addDataset(mdTxnCtx, new Dataset(primaryIndexes[i].getDataverseName(),
                    primaryIndexes[i].getIndexedDatasetName(), primaryIndexes[i].getPayloadRecordType().getTypeName(),
                    id, DatasetType.INTERNAL, primaryIndexes[i].getDatasetId().getId(), IMetadataEntity.PENDING_NO_OP));
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

    private static DatasourceAdapter getAdapter(String adapterFactoryClassName) throws Exception {
        String adapterName = ((IAdapterFactory) (Class.forName(adapterFactoryClassName).newInstance())).getName();
        return new DatasourceAdapter(new AdapterIdentifier(MetadataConstants.METADATA_DATAVERSE_NAME, adapterName),
                adapterFactoryClassName, DatasourceAdapter.AdapterType.INTERNAL);
    }

    public static void enlistMetadataDataset(IMetadataIndex index, boolean create) throws Exception {
        String filePath = metadataStore + index.getFileNameRelativePath();
        FileReference file = new FileReference(new File(filePath));
        IInMemoryBufferCache memBufferCache = new InMemoryBufferCache(new HeapBufferAllocator(), DEFAULT_MEM_PAGE_SIZE,
                DEFAULT_MEM_NUM_PAGES, new TransientFileMapManager());
        ITypeTraits[] typeTraits = index.getTypeTraits();
        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        IInMemoryFreePageManager memFreePageManager = new InMemoryFreePageManager(DEFAULT_MEM_NUM_PAGES,
                metaDataFrameFactory);
        LSMBTree lsmBtree = LSMBTreeUtils.createLSMTree(memBufferCache, memFreePageManager, ioManager, file,
                bufferCache, fileMapProvider, typeTraits, comparatorFactories, runtimeContext.getLSMMergePolicy(),
                runtimeContext.getLSMBTreeOperationTrackerFactory(), runtimeContext.getLSMIOScheduler(),
                AsterixRuntimeComponentsProvider.LSMBTREE_PROVIDER);
        long resourceID = -1;
        if (create) {
            lsmBtree.create();
            resourceID = runtimeContext.getResourceIdFactory().createId();

            ILocalResourceMetadata localResourceMetadata = new LSMBTreeLocalResourceMetadata(typeTraits,
                    comparatorFactories, index.isPrimaryIndex(), GlobalConfig.DEFAULT_INDEX_MEM_PAGE_SIZE,
                    GlobalConfig.DEFAULT_INDEX_MEM_NUM_PAGES);
            ILocalResourceFactoryProvider localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(
                    localResourceMetadata, LocalResource.LSMBTreeResource);
            ILocalResourceFactory localResourceFactory = localResourceFactoryProvider.getLocalResourceFactory();
            localResourceRepository.insert(localResourceFactory.createLocalResource(resourceID, file.getFile()
                    .getPath(), 0));
        } else {
            resourceID = localResourceRepository.getResourceByName(file.getFile().getPath()).getResourceId();
        }
        index.setResourceID(resourceID);
        index.setFile(file);
        indexLifecycleManager.register(resourceID, lsmBtree);
        indexLifecycleManager.open(resourceID);
    }

    public static String getOutputDir() {
        return outputDir;
    }

    public static String getMetadataNodeName() {
        return metadataNodeName;
    }

}
