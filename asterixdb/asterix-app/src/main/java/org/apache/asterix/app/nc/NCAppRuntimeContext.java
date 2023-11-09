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

import static org.apache.hyracks.control.common.controllers.ControllerConfig.Option.CLOUD_DEPLOYMENT;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.cloud.CloudManagerProvider;
import org.apache.asterix.cloud.LocalPartitionBootstrapper;
import org.apache.asterix.common.api.IConfigValidator;
import org.apache.asterix.common.api.IConfigValidatorFactory;
import org.apache.asterix.common.api.ICoordinationService;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.IDiskWriteRateLimiterProvider;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.api.IPropertiesFactory;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IReceptionistFactory;
import org.apache.asterix.common.cloud.IPartitionBootstrapper;
import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.config.BuildProperties;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.NodeProperties;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.config.TransactionProperties;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.DiskWriteRateLimiterProvider;
import org.apache.asterix.common.context.GlobalVirtualBufferCache;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.replication.IReplicationStrategyFactory;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.IReplicaManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.common.transactions.IRecoveryManagerFactory;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.RMIClientFactory;
import org.apache.asterix.metadata.RMIServerFactory;
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
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.client.result.ResultSet;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.ConcurrentMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.GreedyScheduler;
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
import org.apache.hyracks.util.NetworkUtil;
import org.apache.hyracks.util.cache.CacheManager;
import org.apache.hyracks.util.cache.ICacheManager;
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
    private IDatasetLifecycleManager datasetLifecycleManager;
    private IBufferCache bufferCache;
    private IVirtualBufferCache virtualBufferCache;
    private ITransactionSubsystem txnSubsystem;
    private IMetadataNode metadataNodeStub;
    private ILSMIOOperationScheduler lsmIOScheduler;
    private PersistentLocalResourceRepository localResourceRepository;
    private IIOManager ioManager;
    private IIOManager persistenceIOManager;
    private boolean isShuttingdown;
    private ActiveManager activeManager;
    private IReplicationChannel replicationChannel;
    private IReplicationManager replicationManager;
    private ExternalLibraryManager libraryManager;
    private final NCExtensionManager ncExtensionManager;
    private final IStorageComponentProvider componentProvider;
    private final IPersistedResourceRegistry persistedResourceRegistry;
    private volatile HyracksConnection hcc;
    private volatile ResultSet resultSet;
    private IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    private IReplicaManager replicaManager;
    private IReceptionist receptionist;
    private final ICacheManager cacheManager;
    private IConfigValidator configValidator;
    private IDiskWriteRateLimiterProvider diskWriteRateLimiterProvider;
    private final CloudProperties cloudProperties;
    private IPartitionBootstrapper partitionBootstrapper;
    private final INamespacePathResolver namespacePathResolver;
    private final INamespaceResolver namespaceResolver;

    public NCAppRuntimeContext(INCServiceContext ncServiceContext, NCExtensionManager extensionManager,
            IPropertiesFactory propertiesFactory, INamespaceResolver namespaceResolver,
            INamespacePathResolver namespacePathResolver) {
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
        cloudProperties = propertiesFactory.newCloudProperties();
        ncExtensionManager = extensionManager;
        componentProvider = new StorageComponentProvider();
        resourceIdFactory = new GlobalResourceIdFactoryProvider(ncServiceContext, getResourceIdBlockSize())
                .createResourceIdFactory();
        persistedResourceRegistry = ncServiceContext.getPersistedResourceRegistry();
        cacheManager = new CacheManager();
        this.namespacePathResolver = namespacePathResolver;
        this.namespaceResolver = namespaceResolver;
    }

    @Override
    public void initialize(IRecoveryManagerFactory recoveryManagerFactory, IReceptionistFactory receptionistFactory,
            IConfigValidatorFactory configValidatorFactory, IReplicationStrategyFactory replicationStrategyFactory,
            boolean initialRun) throws IOException {
        ioManager = getServiceContext().getIoManager();
        if (isCloudDeployment()) {
            persistenceIOManager =
                    CloudManagerProvider.createIOManager(cloudProperties, ioManager, namespacePathResolver);
            partitionBootstrapper = CloudManagerProvider.getCloudPartitionBootstrapper(persistenceIOManager);
        } else {
            persistenceIOManager = ioManager;
            partitionBootstrapper = new LocalPartitionBootstrapper(ioManager);
        }
        int ioQueueLen = getServiceContext().getAppConfig().getInt(NCConfig.Option.IO_QUEUE_SIZE);
        threadExecutor =
                MaintainedThreadNameExecutorService.newCachedThreadPool(getServiceContext().getThreadFactory());
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageCleanerPolicy pcp = new DelayPageCleanerPolicy(600000);
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy(allocator,
                storageProperties.getBufferCachePageSize(), storageProperties.getBufferCacheNumPages());
        lsmIOScheduler = createIoScheduler(storageProperties);
        metadataMergePolicyFactory = new ConcurrentMergePolicyFactory();
        indexCheckpointManagerProvider = new IndexCheckpointManagerProvider(persistenceIOManager);
        ILocalResourceRepositoryFactory persistentLocalResourceRepositoryFactory =
                new PersistentLocalResourceRepositoryFactory(persistenceIOManager, indexCheckpointManagerProvider,
                        persistedResourceRegistry);
        localResourceRepository =
                (PersistentLocalResourceRepository) persistentLocalResourceRepositoryFactory.createRepository();
        configValidator = configValidatorFactory.create();
        txnSubsystem = new TransactionSubsystem(this, recoveryManagerFactory);
        IRecoveryManager recoveryMgr = txnSubsystem.getRecoveryManager();
        SystemState systemState = recoveryMgr.getSystemState();
        boolean resetStorageData = initialRun || systemState == SystemState.PERMANENT_DATA_LOSS;
        if (resetStorageData) {
            //delete any storage data before the resource factory is initialized
            if (LOGGER.isWarnEnabled()) {
                LOGGER.log(Level.WARN,
                        "Deleting the storage dir. initialRun = " + initialRun + ", systemState = " + systemState);
            }
            localResourceRepository.deleteStorageData();
        }
        int maxScheduledFlushes = storageProperties.getMaxScheduledFlushes();
        if (maxScheduledFlushes <= 0) {
            maxScheduledFlushes = ioManager.getIODevices().size();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("The value of maxScheduledFlushes is not provided. Setting maxConcurrentFlushes = {}.",
                        maxScheduledFlushes);
            }
        }
        virtualBufferCache = new GlobalVirtualBufferCache(allocator, storageProperties, maxScheduledFlushes);
        // Must start vbc now instead of by life cycle component manager (lccm) because lccm happens after
        // the metadata bootstrap task
        ((ILifeCycleComponent) virtualBufferCache).start();
        datasetLifecycleManager =
                new DatasetLifecycleManager(storageProperties, localResourceRepository, txnSubsystem.getLogManager(),
                        virtualBufferCache, indexCheckpointManagerProvider, ioManager.getIODevices().size());
        localResourceRepository.setDatasetLifecycleManager(datasetLifecycleManager);
        final String nodeId = getServiceContext().getNodeId();
        final Set<Integer> nodePartitions = metadataProperties.getNodePartitions(nodeId);
        replicaManager = new ReplicaManager(this, nodePartitions);
        isShuttingdown = false;
        activeManager = new ActiveManager(threadExecutor, getServiceContext().getNodeId(),
                activeProperties.getMemoryComponentGlobalBudget(), compilerProperties.getFrameSize(),
                this.ncServiceContext);
        receptionist = receptionistFactory.create();

        if (replicationProperties.isReplicationEnabled()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Replication is enabled");
            }
            replicationManager = new ReplicationManager(this, replicationStrategyFactory, replicationProperties);

            //pass replication manager to replication required object
            //LogManager to replicate logs
            txnSubsystem.getLogManager().setReplicationManager(replicationManager);

            //PersistentLocalResourceRepository to replicate metadata files and delete backups on drop index
            localResourceRepository.setReplicationManager(replicationManager);

            //initialize replication channel
            replicationChannel = new ReplicationChannel(this);

            bufferCache = new BufferCache(persistenceIOManager, prs, pcp, new FileMapManager(),
                    storageProperties.getBufferCacheMaxOpenFiles(), ioQueueLen, getServiceContext().getThreadFactory(),
                    replicationManager);
        } else {
            bufferCache = new BufferCache(persistenceIOManager, prs, pcp, new FileMapManager(),
                    storageProperties.getBufferCacheMaxOpenFiles(), ioQueueLen, getServiceContext().getThreadFactory());
        }

        NodeControllerService ncs = (NodeControllerService) getServiceContext().getControllerService();
        FileReference appDir =
                ioManager.resolveAbsolutePath(getServiceContext().getServerCtx().getAppDir().getAbsolutePath());
        libraryManager = new ExternalLibraryManager(ncs, persistedResourceRegistry, appDir, ioManager);
        libraryManager.initialize(resetStorageData);

        /*
         * The order of registration is important. The buffer cache must registered before recovery and transaction
         * managers. Notes: registered components are stopped in reversed order
         */
        ILifeCycleComponentManager lccm = getServiceContext().getLifeCycleComponentManager();
        lccm.register((ILifeCycleComponent) virtualBufferCache);
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
        lccm.register(libraryManager);

        diskWriteRateLimiterProvider = new DiskWriteRateLimiterProvider();
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
    public IVirtualBufferCache getVirtualBufferCache() {
        return virtualBufferCache;
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
    public IIOManager getPersistenceIoManager() {
        return persistenceIOManager;
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
    public INamespaceResolver getNamespaceResolver() {
        return namespaceResolver;
    }

    @Override
    public INamespacePathResolver getNamespacePathResolver() {
        return namespacePathResolver;
    }

    @Override
    public void initializeMetadata(boolean newUniverse, int partitionId) throws Exception {
        LOGGER.info("Bootstrapping ({}) metadata in partition {}", newUniverse ? "new" : "existing", partitionId);
        MetadataNode.INSTANCE.initialize(this, ncExtensionManager.getMetadataIndexesProvider(),
                ncExtensionManager.getMetadataTupleTranslatorProvider(), ncExtensionManager.getMetadataExtensions(),
                partitionId);

        // This is a special case, we just give the metadataNode directly.
        // This way we can delay the registration of the metadataNode until
        // it is completely initialized.
        MetadataManager.initialize(getAsterixStateProxies(), MetadataNode.INSTANCE);
        MetadataBootstrap.startUniverse(getServiceContext(), newUniverse,
                ncExtensionManager.getMetadataIndexesProvider());
        MetadataBootstrap.startDDLRecovery();
        ncExtensionManager.initializeMetadata(getServiceContext(), ncExtensionManager.getMetadataIndexesProvider());
        LOGGER.info("Metadata node bound");
    }

    @Override
    public synchronized void exportMetadataNodeStub() throws RemoteException {
        if (metadataNodeStub == null) {
            final INetworkSecurityManager networkSecurityManager =
                    ncServiceContext.getControllerService().getNetworkSecurityManager();

            metadataNodeStub = (IMetadataNode) UnicastRemoteObject.exportObject(MetadataNode.INSTANCE,
                    getMetadataProperties().getMetadataPort(),
                    RMIClientFactory.getSocketFactory(networkSecurityManager),
                    RMIServerFactory.getSocketFactory(networkSecurityManager));
        }
    }

    @Override
    public synchronized void unexportMetadataNodeStub() throws RemoteException {
        if (metadataNodeStub != null) {
            UnicastRemoteObject.unexportObject(MetadataNode.INSTANCE, false);
        }
        metadataNodeStub = null;
    }

    protected Collection<IAsterixStateProxy> getAsterixStateProxies() {
        //noinspection unchecked
        ConcurrentHashMap<CcId, IAsterixStateProxy> proxyMap =
                (ConcurrentHashMap<CcId, IAsterixStateProxy>) getServiceContext().getDistributedState();
        if (proxyMap == null) {
            throw new IllegalStateException("Metadata node cannot access distributed state");
        }

        return proxyMap.values();
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

    @Override
    public NCExtensionManager getExtensionManager() {
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
        HyracksConnection hc = hcc;
        if (hc == null || !hc.isConnected()) {
            synchronized (this) {
                hc = hcc;
                if (hc == null || !hc.isConnected()) {
                    try {
                        ResultSet rs = resultSet;
                        resultSet = null;
                        NetworkUtil.closeQuietly(rs);

                        NodeControllerService ncSrv = (NodeControllerService) ncServiceContext.getControllerService();
                        // TODO(mblow): multicc
                        CcId primaryCcId = ncSrv.getPrimaryCcId();
                        ClusterControllerInfo ccInfo = ncSrv.getNodeParameters(primaryCcId).getClusterControllerInfo();
                        NetworkUtil.closeQuietly(hc);
                        hcc = hc = new HyracksConnection(ccInfo.getClientNetAddress(), ccInfo.getClientNetPort(),
                                ncSrv.getNetworkSecurityManager().getSocketChannelFactory());
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }
        return hc;
    }

    @Override
    public IResultSet getResultSet() throws HyracksDataException {
        ResultSet rs = resultSet;
        if (rs == null) {
            synchronized (this) {
                rs = resultSet;
                if (rs == null) {
                    try {
                        resultSet = rs = ResultReader.createResultSet(getHcc(), ncServiceContext.getControllerService(),
                                compilerProperties);
                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }
        }
        return rs;
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

    @Override
    public IPersistedResourceRegistry getPersistedResourceRegistry() {
        return persistedResourceRegistry;
    }

    @Override
    public IReceptionist getReceptionist() {
        return receptionist;
    }

    @Override
    public ICacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public IConfigValidator getConfigValidator() {
        return configValidator;
    }

    private ILSMIOOperationScheduler createIoScheduler(StorageProperties properties) {
        String schedulerName = storageProperties.getIoScheduler();
        int numPartitions = ioManager.getIODevices().size();

        int maxConcurrentFlushes = storageProperties.geMaxConcurrentFlushes(numPartitions);
        int maxScheduledMerges = storageProperties.getMaxScheduledMerges(numPartitions);
        int maxConcurrentMerges = storageProperties.getMaxConcurrentMerges(numPartitions);

        ILSMIOOperationScheduler ioScheduler = null;
        if (AsynchronousScheduler.FACTORY.getName().equalsIgnoreCase(schedulerName)) {
            ioScheduler = AsynchronousScheduler.FACTORY.createIoScheduler(getServiceContext().getThreadFactory(),
                    HaltCallback.INSTANCE, maxConcurrentFlushes, maxScheduledMerges, maxConcurrentMerges);
        } else if (GreedyScheduler.FACTORY.getName().equalsIgnoreCase(schedulerName)) {
            ioScheduler = GreedyScheduler.FACTORY.createIoScheduler(getServiceContext().getThreadFactory(),
                    HaltCallback.INSTANCE, maxConcurrentFlushes, maxScheduledMerges, maxConcurrentMerges);
        } else {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.log(Level.WARN,
                        "Unknown storage I/O scheduler: " + schedulerName + "; defaulting to greedy I/O scheduler.");
            }
            ioScheduler = GreedyScheduler.FACTORY.createIoScheduler(getServiceContext().getThreadFactory(),
                    HaltCallback.INSTANCE, maxConcurrentFlushes, maxScheduledMerges, maxConcurrentMerges);
        }
        return ioScheduler;
    }

    @Override
    public IDiskWriteRateLimiterProvider getDiskWriteRateLimiterProvider() {
        return diskWriteRateLimiterProvider;
    }

    @Override
    public boolean isCloudDeployment() {
        return ncServiceContext.getAppConfig().getBoolean(CLOUD_DEPLOYMENT);
    }

    @Override
    public CloudProperties getCloudProperties() {
        return cloudProperties;
    }

    @Override
    public IPartitionBootstrapper getPartitionBootstrapper() {
        return partitionBootstrapper;
    }

    private int getResourceIdBlockSize() {
        return isCloudDeployment() ? storageProperties.getStoragePartitionsCount()
                : ncServiceContext.getIoManager().getIODevices().size();
    }
}
