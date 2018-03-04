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
package org.apache.asterix.app.nc;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.common.api.ICoordinationService;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.IDatasetMemoryManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.api.IPropertiesFactory;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.BuildProperties;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.NodeProperties;
import org.apache.asterix.common.config.PropertiesAccessor;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetMemoryManager;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.IReplicaManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataBootstrap;
import org.apache.asterix.replication.management.ReplicationChannel;
import org.apache.asterix.replication.management.ReplicationManager;
import org.apache.asterix.runtime.transaction.GlobalResourceIdFactoryProvider;
import org.apache.asterix.runtime.utils.NoOpCoordinationService;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepositoryFactory;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.IPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.storage.common.file.ILocalResourceRepositoryFactory;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.hyracks.util.MaintainedThreadNameExecutorService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NCAppRuntimeContext implements INcApplicationContext {
    private static final Logger LOGGER = LogManager.getLogger();

    private ILSMMergePolicyFactory metadataMergePolicyFactory;
    private final INCServiceContext ncServiceContext;
    private final IResourceIdFactory resourceIdFactory;
    private final CompilerProperties compilerProperties;
    private final ExternalProperties externalProperties;
    private final MetadataProperties metadataProperties;
    private final StorageProperties storageProperties;
    private final TransactionProperties txnProperties;
    private final ActiveProperties activeProperties;
    private final BuildProperties buildProperties;
    private final ReplicationProperties replicationProperties;
    private final MessagingProperties messagingProperties;
    private final NodeProperties nodeProperties;
    private ExecutorService threadExecutor;
    private IDatasetMemoryManager datasetMemoryManager;
    private IDatasetLifecycleManager datasetLifecycleManager;
    private IBufferCache bufferCache;
    private ITransactionSubsystem txnSubsystem;
    private IMetadataNode metadataNodeStub;
    private ILSMIOOperationScheduler lsmIOScheduler;
    private PersistentLocalResourceRepository localResourceRepository;
    private IIOManager ioManager;
    private boolean isShuttingdown;
    private ActiveManager activeManager;
    private IReplicationChannel replicationChannel;
    private IReplicationManager replicationManager;
    private final ILibraryManager libraryManager;
    private final NCExtensionManager ncExtensionManager;
    private final IStorageComponentProvider componentProvider;
    private IHyracksClientConnection hcc;
    private IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    private IReplicaManager replicaManager;

    public NCAppRuntimeContext(INCServiceContext ncServiceContext, List<AsterixExtension> extensions,
            IPropertiesFactory propertiesFactory) throws AsterixException, InstantiationException,
            IllegalAccessException, ClassNotFoundException, IOException {
        List<AsterixExtension> allExtensions = new ArrayList<>();
        this.ncServiceContext = ncServiceContext;
        compilerProperties = propertiesFactory.newCompilerProperties();
        externalProperties = propertiesFactory.newExternalProperties();
        metadataProperties = propertiesFactory.newMetadataProperties();
        storageProperties = propertiesFactory.newStorageProperties();
        txnProperties = propertiesFactory.newTransactionProperties();
        activeProperties = propertiesFactory.newActiveProperties();
        buildProperties = propertiesFactory.newBuildProperties();
        replicationProperties = propertiesFactory.newReplicationProperties();
        messagingProperties = propertiesFactory.newMessagingProperties();
        nodeProperties = propertiesFactory.newNodeProperties();
        libraryManager = new ExternalLibraryManager();
        if (extensions != null) {
            allExtensions.addAll(extensions);
        }
        PropertiesAccessor propertiesAccessor = PropertiesAccessor.getInstance(ncServiceContext.getAppConfig());
        allExtensions.addAll(propertiesAccessor.getExtensions());
        ncExtensionManager = new NCExtensionManager(allExtensions);
        componentProvider = new StorageComponentProvider();
        resourceIdFactory = new GlobalResourceIdFactoryProvider(ncServiceContext).createResourceIdFactory();
    }

    @Override
    public void initialize(boolean initialRun) throws IOException, ACIDException {
        ioManager = getServiceContext().getIoManager();
        threadExecutor =
                MaintainedThreadNameExecutorService.newCachedThreadPool(getServiceContext().getThreadFactory());
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageCleanerPolicy pcp = new DelayPageCleanerPolicy(600000);
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy(allocator,
                storageProperties.getBufferCachePageSize(), storageProperties.getBufferCacheNumPages());

        AsynchronousScheduler.INSTANCE.init(getServiceContext().getThreadFactory());
        lsmIOScheduler = AsynchronousScheduler.INSTANCE;

        metadataMergePolicyFactory = new PrefixMergePolicyFactory();
        indexCheckpointManagerProvider = new IndexCheckpointManagerProvider(ioManager);

        ILocalResourceRepositoryFactory persistentLocalResourceRepositoryFactory =
                new PersistentLocalResourceRepositoryFactory(ioManager, indexCheckpointManagerProvider);

        localResourceRepository =
                (PersistentLocalResourceRepository) persistentLocalResourceRepositoryFactory.createRepository();

        txnSubsystem = new TransactionSubsystem(this);
        IRecoveryManager recoveryMgr = txnSubsystem.getRecoveryManager();
        SystemState systemState = recoveryMgr.getSystemState();
        if (initialRun || systemState == SystemState.PERMANENT_DATA_LOSS) {
            //delete any storage data before the resource factory is initialized
            if (LOGGER.isWarnEnabled()) {
                LOGGER.log(Level.WARN,
                        "Deleting the storage dir. initialRun = " + initialRun + ", systemState = " + systemState);
            }
            localResourceRepository.deleteStorageData();
        }
        datasetMemoryManager = new DatasetMemoryManager(storageProperties);
        datasetLifecycleManager =
                new DatasetLifecycleManager(storageProperties, localResourceRepository, txnSubsystem.getLogManager(),
                        datasetMemoryManager, indexCheckpointManagerProvider, ioManager.getIODevices().size());
        final String nodeId = getServiceContext().getNodeId();
        final ClusterPartition[] nodePartitions = metadataProperties.getNodePartitions().get(nodeId);
        final Set<Integer> nodePartitionsIds =
                Arrays.stream(nodePartitions).map(ClusterPartition::getPartitionId).collect(Collectors.toSet());
        replicaManager = new ReplicaManager(this, nodePartitionsIds);
        isShuttingdown = false;
        activeManager = new ActiveManager(threadExecutor, getServiceContext().getNodeId(),
                activeProperties.getMemoryComponentGlobalBudget(), compilerProperties.getFrameSize(),
                this.ncServiceContext);

        if (replicationProperties.isReplicationEnabled()) {
            replicationManager = new ReplicationManager(this, replicationProperties);

            //pass replication manager to replication required object
            //LogManager to replicate logs
            txnSubsystem.getLogManager().setReplicationManager(replicationManager);

            //PersistentLocalResourceRepository to replicate metadata files and delete backups on drop index
            localResourceRepository.setReplicationManager(replicationManager);

            //initialize replication channel
            replicationChannel = new ReplicationChannel(this);

            bufferCache = new BufferCache(ioManager, prs, pcp, new FileMapManager(),
                    storageProperties.getBufferCacheMaxOpenFiles(), getServiceContext().getThreadFactory(),
                    replicationManager);
        } else {
            bufferCache = new BufferCache(ioManager, prs, pcp, new FileMapManager(),
                    storageProperties.getBufferCacheMaxOpenFiles(), getServiceContext().getThreadFactory());
        }

        /*
         * The order of registration is important. The buffer cache must registered before recovery and transaction
         * managers. Notes: registered components are stopped in reversed order
         */
        ILifeCycleComponentManager lccm = getServiceContext().getLifeCycleComponentManager();
        lccm.register((ILifeCycleComponent) bufferCache);
        /*
         * LogManager must be stopped after RecoveryManager, DatasetLifeCycleManager, and ReplicationManager
         * to process any logs that might be generated during stopping these components
         */
        lccm.register((ILifeCycleComponent) txnSubsystem.getLogManager());
        /*
         * ReplicationManager must be stopped after indexLifecycleManager and recovery manager
         * so that any logs/files generated during closing datasets or checkpoints are sent to remote replicas
         */
        if (replicationManager != null) {
            lccm.register(replicationManager);
        }
        lccm.register((ILifeCycleComponent) txnSubsystem.getRecoveryManager());
        /*
         * Stopping indexLifecycleManager will flush and close all datasets.
         */
        lccm.register((ILifeCycleComponent) datasetLifecycleManager);
        lccm.register((ILifeCycleComponent) txnSubsystem.getTransactionManager());
        lccm.register((ILifeCycleComponent) txnSubsystem.getLockManager());
        lccm.register(txnSubsystem.getCheckpointManager());
    }

    @Override
    public boolean isShuttingdown() {
        return isShuttingdown;
    }

    @Override
    public void setShuttingdown(boolean isShuttingdown) {
        this.isShuttingdown = isShuttingdown;
    }

    @Override
    public synchronized void preStop() throws Exception {
        activeManager.shutdown();
        if (metadataNodeStub != null) {
            unexportMetadataNodeStub();
        }
    }

    @Override
    public void deinitialize() throws HyracksDataException {
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    @Override
    public ITransactionSubsystem getTransactionSubsystem() {
        return txnSubsystem;
    }

    @Override
    public IDatasetLifecycleManager getDatasetLifecycleManager() {
        return datasetLifecycleManager;
    }

    @Override
    public IDatasetMemoryManager getDatasetMemoryManager() {
        return datasetMemoryManager;
    }

    @Override
    public ILSMIOOperationScheduler getLSMIOScheduler() {
        return lsmIOScheduler;
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository() {
        return localResourceRepository;
    }

    @Override
    public IResourceIdFactory getResourceIdFactory() {
        return resourceIdFactory;
    }

    @Override
    public IIOManager getIoManager() {
        return ioManager;
    }

    @Override
    public StorageProperties getStorageProperties() {
        return storageProperties;
    }

    @Override
    public TransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public CompilerProperties getCompilerProperties() {
        return compilerProperties;
    }

    @Override
    public MetadataProperties getMetadataProperties() {
        return metadataProperties;
    }

    @Override
    public ExternalProperties getExternalProperties() {
        return externalProperties;
    }

    @Override
    public ActiveProperties getActiveProperties() {
        return activeProperties;
    }

    @Override
    public BuildProperties getBuildProperties() {
        return buildProperties;
    }

    @Override
    public MessagingProperties getMessagingProperties() {
        return messagingProperties;
    }

    @Override
    public NodeProperties getNodeProperties() {
        return nodeProperties;
    }

    @Override
    public ExecutorService getThreadExecutor() {
        return threadExecutor;
    }

    @Override
    public ILSMMergePolicyFactory getMetadataMergePolicyFactory() {
        return metadataMergePolicyFactory;
    }

    @Override
    public ActiveManager getActiveManager() {
        return activeManager;
    }

    @Override
    public ReplicationProperties getReplicationProperties() {
        return replicationProperties;
    }

    @Override
    public IReplicationChannel getReplicationChannel() {
        return replicationChannel;
    }

    @Override
    public IReplicationManager getReplicationManager() {
        return replicationManager;
    }

    @Override
    public ILibraryManager getLibraryManager() {
        return libraryManager;
    }

    @Override
    public void initializeMetadata(boolean newUniverse, int partitionId) throws Exception {
        LOGGER.info("Bootstrapping metadata");
        MetadataNode.INSTANCE.initialize(this, ncExtensionManager.getMetadataTupleTranslatorProvider(),
                ncExtensionManager.getMetadataExtensions(), partitionId);

        //noinspection unchecked
        ConcurrentHashMap<CcId, IAsterixStateProxy> proxyMap =
                (ConcurrentHashMap<CcId, IAsterixStateProxy>) getServiceContext().getDistributedState();
        if (proxyMap == null) {
            throw new IllegalStateException("Metadata node cannot access distributed state");
        }

        // This is a special case, we just give the metadataNode directly.
        // This way we can delay the registration of the metadataNode until
        // it is completely initialized.
        MetadataManager.initialize(proxyMap.values(), MetadataNode.INSTANCE);
        MetadataBootstrap.startUniverse(getServiceContext(), newUniverse);
        MetadataBootstrap.startDDLRecovery();
        ncExtensionManager.initializeMetadata(getServiceContext());
        LOGGER.info("Metadata node bound");
    }

    @Override
    public synchronized void exportMetadataNodeStub() throws RemoteException {
        if (metadataNodeStub == null) {
            metadataNodeStub = (IMetadataNode) UnicastRemoteObject.exportObject(MetadataNode.INSTANCE,
                    getMetadataProperties().getMetadataPort());
        }
    }

    @Override
    public synchronized void unexportMetadataNodeStub() throws RemoteException {
        if (metadataNodeStub != null) {
            UnicastRemoteObject.unexportObject(MetadataNode.INSTANCE, false);
        }
        metadataNodeStub = null;
    }

    @Override
    public synchronized void bindMetadataNodeStub(CcId ccId) throws RemoteException {
        if (metadataNodeStub == null) {
            throw new IllegalStateException("Metadata node not exported");

        }
        //noinspection unchecked
        ((ConcurrentMap<CcId, IAsterixStateProxy>) getServiceContext().getDistributedState()).get(ccId)
                .setMetadataNode(metadataNodeStub);
    }

    public NCExtensionManager getNcExtensionManager() {
        return ncExtensionManager;
    }

    @Override
    public IStorageComponentProvider getStorageComponentProvider() {
        return componentProvider;
    }

    @Override
    public INCServiceContext getServiceContext() {
        return ncServiceContext;
    }

    @Override
    public IHyracksClientConnection getHcc() throws HyracksDataException {
        if (hcc == null || !hcc.isConnected()) {
            synchronized (this) {
                if (hcc == null || !hcc.isConnected()) {
                    try {
                        NodeControllerService ncSrv = (NodeControllerService) ncServiceContext.getControllerService();
                        // TODO(mblow): multicc
                        CcId primaryCcId = ncSrv.getPrimaryCcId();
                        ClusterControllerInfo ccInfo = ncSrv.getNodeParameters(primaryCcId).getClusterControllerInfo();
                        hcc = new HyracksConnection(ccInfo.getClientNetAddress(), ccInfo.getClientNetPort());
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }
        return hcc;
    }

    @Override
    public IReplicaManager getReplicaManager() {
        return replicaManager;
    }

    @Override
    public IIndexCheckpointManagerProvider getIndexCheckpointManagerProvider() {
        return indexCheckpointManagerProvider;
    }

    @Override
    public ICoordinationService getCoordinationService() {
        return NoOpCoordinationService.INSTANCE;
    }

    @Override
    public long getMaxTxnId() {
        if (txnSubsystem == null) {
            throw new IllegalStateException("cannot determine max txn id before txnSubsystem is initialized!");
        }

        return Math.max(MetadataManager.INSTANCE == null ? 0 : MetadataManager.INSTANCE.getMaxTxnId(),
                txnSubsystem.getTransactionManager().getMaxTxnId());
    }
}
