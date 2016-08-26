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
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.api.common.AsterixAppRuntimeContextProviderForRecovery;
import org.apache.asterix.common.api.AsterixThreadExecutor;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixBuildProperties;
import org.apache.asterix.common.config.AsterixCompilerProperties;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.AsterixExtensionProperties;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.AsterixPropertiesAccessor;
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.context.AsterixFileMapManager;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.api.IMetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataBootstrap;
import org.apache.asterix.metadata.bootstrap.MetadataIndexImmutableProperties;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.asterix.replication.management.ReplicationChannel;
import org.apache.asterix.replication.management.ReplicationManager;
import org.apache.asterix.replication.recovery.RemoteRecoveryManager;
import org.apache.asterix.replication.storage.ReplicaResourcesManager;
import org.apache.asterix.transaction.management.resource.GlobalResourceIdFactoryProvider;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepositoryFactory;
import org.apache.asterix.transaction.management.service.transaction.TransactionSubsystem;
import org.apache.hyracks.api.application.IApplicationConfig;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.PrefixMergePolicyFactory;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICacheMemoryAllocator;
import org.apache.hyracks.storage.common.buffercache.IPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ILocalResourceRepositoryFactory;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

public class AsterixNCAppRuntimeContext implements IAsterixAppRuntimeContext, IAsterixPropertiesProvider {
    private static final Logger LOGGER = Logger.getLogger(AsterixNCAppRuntimeContext.class.getName());

    private ILSMMergePolicyFactory metadataMergePolicyFactory;
    private final INCApplicationContext ncApplicationContext;

    private AsterixCompilerProperties compilerProperties;
    private AsterixExternalProperties externalProperties;
    private AsterixMetadataProperties metadataProperties;
    private AsterixStorageProperties storageProperties;
    private AsterixTransactionProperties txnProperties;
    private AsterixFeedProperties feedProperties;
    private AsterixBuildProperties buildProperties;
    private AsterixReplicationProperties replicationProperties;
    private MessagingProperties messagingProperties;

    private AsterixThreadExecutor threadExecutor;
    private IDatasetLifecycleManager datasetLifecycleManager;
    private IFileMapManager fileMapManager;
    private IBufferCache bufferCache;
    private ITransactionSubsystem txnSubsystem;

    private ILSMIOOperationScheduler lsmIOScheduler;
    private PersistentLocalResourceRepository localResourceRepository;
    private IResourceIdFactory resourceIdFactory;
    private IIOManager ioManager;
    private boolean isShuttingdown;

    private ActiveManager activeManager;

    private IReplicationChannel replicationChannel;
    private IReplicationManager replicationManager;
    private IRemoteRecoveryManager remoteRecoveryManager;
    private IReplicaResourcesManager replicaResourcesManager;
    private final int metadataRmiPort;

    private final ILibraryManager libraryManager;
    private final NCExtensionManager ncExtensionManager;

    public AsterixNCAppRuntimeContext(INCApplicationContext ncApplicationContext, int metadataRmiPort,
            List<AsterixExtension> extensions) throws AsterixException, InstantiationException, IllegalAccessException,
            ClassNotFoundException, IOException {
        List<AsterixExtension> allExtensions = new ArrayList<>();
        this.ncApplicationContext = ncApplicationContext;
        // Determine whether to use old-style asterix-configuration.xml or new-style configuration.
        // QQQ strip this out eventually
        AsterixPropertiesAccessor propertiesAccessor;
        IApplicationConfig cfg = ncApplicationContext.getAppConfig();
        // QQQ this is NOT a good way to determine whether the config is valid
        if (cfg.getString("cc", "cluster.address") != null) {
            propertiesAccessor = new AsterixPropertiesAccessor(cfg);
        } else {
            propertiesAccessor = new AsterixPropertiesAccessor();
        }
        compilerProperties = new AsterixCompilerProperties(propertiesAccessor);
        externalProperties = new AsterixExternalProperties(propertiesAccessor);
        metadataProperties = new AsterixMetadataProperties(propertiesAccessor);
        storageProperties = new AsterixStorageProperties(propertiesAccessor);
        txnProperties = new AsterixTransactionProperties(propertiesAccessor);
        feedProperties = new AsterixFeedProperties(propertiesAccessor);
        buildProperties = new AsterixBuildProperties(propertiesAccessor);
        replicationProperties = new AsterixReplicationProperties(propertiesAccessor,
                AsterixClusterProperties.INSTANCE.getCluster());
        messagingProperties = new MessagingProperties(propertiesAccessor);
        this.metadataRmiPort = metadataRmiPort;
        libraryManager = new ExternalLibraryManager();
        if (extensions != null) {
            allExtensions.addAll(extensions);
        }
        allExtensions.addAll(new AsterixExtensionProperties(propertiesAccessor).getExtensions());
        ncExtensionManager = new NCExtensionManager(allExtensions);
    }

    @Override
    public void initialize(boolean initialRun) throws IOException, ACIDException {
        Logger.getLogger("org.apache").setLevel(externalProperties.getLogLevel());

        threadExecutor = new AsterixThreadExecutor(ncApplicationContext.getThreadFactory());
        fileMapManager = new AsterixFileMapManager();
        ICacheMemoryAllocator allocator = new HeapBufferAllocator();
        IPageCleanerPolicy pcp = new DelayPageCleanerPolicy(600000);
        ioManager = ncApplicationContext.getRootContext().getIOManager();
        IPageReplacementStrategy prs = new ClockPageReplacementStrategy(allocator,
                storageProperties.getBufferCachePageSize(), storageProperties.getBufferCacheNumPages());

        AsynchronousScheduler.INSTANCE.init(ncApplicationContext.getThreadFactory());
        lsmIOScheduler = AsynchronousScheduler.INSTANCE;

        metadataMergePolicyFactory = new PrefixMergePolicyFactory();

        ILocalResourceRepositoryFactory persistentLocalResourceRepositoryFactory =
                new PersistentLocalResourceRepositoryFactory(
                ioManager, ncApplicationContext.getNodeId(), metadataProperties);

        localResourceRepository = (PersistentLocalResourceRepository) persistentLocalResourceRepositoryFactory
                .createRepository();

        IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider =
                new AsterixAppRuntimeContextProviderForRecovery(this);
        txnSubsystem = new TransactionSubsystem(ncApplicationContext.getNodeId(), asterixAppRuntimeContextProvider,
                txnProperties);

        IRecoveryManager recoveryMgr = txnSubsystem.getRecoveryManager();
        SystemState systemState = recoveryMgr.getSystemState();
        if (initialRun || systemState == SystemState.NEW_UNIVERSE) {
            //delete any storage data before the resource factory is initialized
            localResourceRepository.deleteStorageData(true);
        }
        initializeResourceIdFactory();

        datasetLifecycleManager = new DatasetLifecycleManager(storageProperties, localResourceRepository,
                MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID, txnSubsystem.getLogManager(),
                ioManager.getIODevices().size());

        isShuttingdown = false;

        activeManager = new ActiveManager(ncApplicationContext.getNodeId(),
                feedProperties.getMemoryComponentGlobalBudget(), compilerProperties.getFrameSize());

        if (replicationProperties.isReplicationEnabled()) {
            String nodeId = ncApplicationContext.getNodeId();

            replicaResourcesManager = new ReplicaResourcesManager(localResourceRepository, metadataProperties);

            replicationManager = new ReplicationManager(nodeId, replicationProperties, replicaResourcesManager,
                    txnSubsystem.getLogManager(), asterixAppRuntimeContextProvider);

            //pass replication manager to replication required object
            //LogManager to replicate logs
            txnSubsystem.getLogManager().setReplicationManager(replicationManager);

            //PersistentLocalResourceRepository to replicate metadata files and delete backups on drop index
            localResourceRepository.setReplicationManager(replicationManager);

            /**
             * add the partitions that will be replicated in this node as inactive partitions
             */
            //get nodes which replicate to this node
            Set<String> replicationClients = replicationProperties.getNodeReplicationClients(nodeId);
            //remove the node itself
            replicationClients.remove(nodeId);
            for (String clientId : replicationClients) {
                //get the partitions of each client
                ClusterPartition[] clientPartitions = metadataProperties.getNodePartitions().get(clientId);
                for (ClusterPartition partition : clientPartitions) {
                    localResourceRepository.addInactivePartition(partition.getPartitionId());
                }
            }

            //initialize replication channel
            replicationChannel = new ReplicationChannel(nodeId, replicationProperties, txnSubsystem.getLogManager(),
                    replicaResourcesManager, replicationManager, ncApplicationContext,
                    asterixAppRuntimeContextProvider);

            remoteRecoveryManager = new RemoteRecoveryManager(replicationManager, this, replicationProperties);

            bufferCache = new BufferCache(ioManager, prs, pcp, fileMapManager,
                    storageProperties.getBufferCacheMaxOpenFiles(), ncApplicationContext.getThreadFactory(),
                    replicationManager);
        } else {
            bufferCache = new BufferCache(ioManager, prs, pcp, fileMapManager,
                    storageProperties.getBufferCacheMaxOpenFiles(), ncApplicationContext.getThreadFactory());
        }

        /*
         * The order of registration is important. The buffer cache must registered before recovery and transaction
         * managers. Notes: registered components are stopped in reversed order
         */
        ILifeCycleComponentManager lccm = ncApplicationContext.getLifeCycleComponentManager();
        lccm.register((ILifeCycleComponent) bufferCache);
        /**
         * LogManager must be stopped after RecoveryManager, DatasetLifeCycleManager, and ReplicationManager
         * to process any logs that might be generated during stopping these components
         */
        lccm.register((ILifeCycleComponent) txnSubsystem.getLogManager());
        /**
         * ReplicationManager must be stopped after indexLifecycleManager and recovery manager
         * so that any logs/files generated during closing datasets or checkpoints are sent to remote replicas
         */
        if (replicationManager != null) {
            lccm.register(replicationManager);
        }
        lccm.register((ILifeCycleComponent) txnSubsystem.getRecoveryManager());
        /**
         * Stopping indexLifecycleManager will flush and close all datasets.
         */
        lccm.register((ILifeCycleComponent) datasetLifecycleManager);
        lccm.register((ILifeCycleComponent) txnSubsystem.getTransactionManager());
        lccm.register((ILifeCycleComponent) txnSubsystem.getLockManager());
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
    public void deinitialize() throws HyracksDataException {
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    @Override
    public IFileMapProvider getFileMapManager() {
        return fileMapManager;
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
    public double getBloomFilterFalsePositiveRate() {
        return storageProperties.getBloomFilterFalsePositiveRate();
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
    public IIOManager getIOManager() {
        return ioManager;
    }

    @Override
    public AsterixStorageProperties getStorageProperties() {
        return storageProperties;
    }

    @Override
    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    @Override
    public AsterixCompilerProperties getCompilerProperties() {
        return compilerProperties;
    }

    @Override
    public AsterixMetadataProperties getMetadataProperties() {
        return metadataProperties;
    }

    @Override
    public AsterixExternalProperties getExternalProperties() {
        return externalProperties;
    }

    @Override
    public AsterixFeedProperties getFeedProperties() {
        return feedProperties;
    }

    @Override
    public AsterixBuildProperties getBuildProperties() {
        return buildProperties;
    }

    @Override
    public MessagingProperties getMessagingProperties() {
        return messagingProperties;
    }

    @Override
    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID) {
        return datasetLifecycleManager.getOperationTracker(datasetID);
    }

    @Override
    public AsterixThreadExecutor getThreadExecutor() {
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
    public AsterixReplicationProperties getReplicationProperties() {
        return replicationProperties;
    }

    @Override
    public IReplicationChannel getReplicationChannel() {
        return replicationChannel;
    }

    @Override
    public IReplicaResourcesManager getReplicaResourcesManager() {
        return replicaResourcesManager;
    }

    @Override
    public IRemoteRecoveryManager getRemoteRecoveryManager() {
        return remoteRecoveryManager;
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
    public void initializeResourceIdFactory() throws HyracksDataException {
        resourceIdFactory = new GlobalResourceIdFactoryProvider(ncApplicationContext).createResourceIdFactory();
    }

    @Override
    public void initializeMetadata(boolean newUniverse) throws Exception {
        IAsterixStateProxy proxy;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Bootstrapping metadata");
        }
        MetadataNode.INSTANCE.initialize(this, ncExtensionManager.getMetadataTupleTranslatorProvider(),
                ncExtensionManager.getMetadataExtensions());

        proxy = (IAsterixStateProxy) ncApplicationContext.getDistributedState();
        if (proxy == null) {
            throw new IllegalStateException("Metadata node cannot access distributed state");
        }

        // This is a special case, we just give the metadataNode directly.
        // This way we can delay the registration of the metadataNode until
        // it is completely initialized.
        MetadataManager.instantiate(new MetadataManager(proxy, MetadataNode.INSTANCE));
        MetadataBootstrap.startUniverse(this, ncApplicationContext, newUniverse);
        MetadataBootstrap.startDDLRecovery();
        ncExtensionManager.initializeMetadata();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Metadata node bound");
        }
    }

    @Override
    public void exportMetadataNodeStub() throws RemoteException {
        IMetadataNode stub = (IMetadataNode) UnicastRemoteObject.exportObject(MetadataNode.INSTANCE, metadataRmiPort);
        ((IAsterixStateProxy) ncApplicationContext.getDistributedState()).setMetadataNode(stub);
    }

    @Override
    public void unexportMetadataNodeStub() throws RemoteException {
        UnicastRemoteObject.unexportObject(MetadataNode.INSTANCE, false);
    }

    public NCExtensionManager getNcExtensionManager() {
        return ncExtensionManager;
    }

}
